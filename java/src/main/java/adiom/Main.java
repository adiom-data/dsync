package adiom;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.implementation.PartitionKeyHelper;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.google.protobuf.ByteString;

import adiom.v1.ConnectorServiceGrpc;
import adiom.v1.Messages.Capabilities;
import adiom.v1.Messages.DataType;
import adiom.v1.Messages.GetInfoRequest;
import adiom.v1.Messages.GetInfoResponse;
import adiom.v1.Messages.Update;
import adiom.v1.Messages.WriteDataRequest;
import adiom.v1.Messages.WriteDataResponse;
import adiom.v1.Messages.WriteUpdatesRequest;
import adiom.v1.Messages.WriteUpdatesResponse;
import adiom.v1.Messages.Capabilities.Sink;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.StreamObserver;

public class Main {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("3 Required arguments: port url key");
            return;
        }
        String cert = System.getenv("CERT_FILE");
        String key = System.getenv("KEY_FILE");
        ServerCredentials creds = InsecureServerCredentials.create();
        try {
            if (cert != null && key != null) {
                creds = TlsServerCredentials.create(new File(cert), new File(key));
            } else {
                System.out.println("env variables CERT_FILE and KEY_FILE not found, using no credentials.");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("starting server on port " + args[0]);
        try {
            Server s = Grpc.newServerBuilderForPort(Integer.parseInt(args[0]), creds)
                .addService(new MyConn(args[1], args[2]))
                .addService(ProtoReflectionServiceV1.newInstance())
                .maxInboundMessageSize(100000000)
                .build();
            s.start();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        System.out.println("bye");
                        s.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("Shutdown was not clean.");
                    }
                }
            });
            s.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Unable to start. Are your credentials and parameters correct?");
        }
    }

    private static class NsHelper {
        public CosmosContainer container;
        public PartitionKeyDefinition pkd;
    }

    private static class MyConn extends ConnectorServiceGrpc.ConnectorServiceImplBase {

        private CosmosClient client;
        private ConcurrentHashMap<String, NsHelper> nsHelpers;

        public MyConn(String endpoint, String key) {
            super();
            this.nsHelpers = new ConcurrentHashMap<>();
            this.client = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .buildClient();
        }

        @Override
        public void getInfo(GetInfoRequest request, StreamObserver<GetInfoResponse> responseObserver) {
            responseObserver.onNext(GetInfoResponse.newBuilder()
                    .setDbType("CosmosDB-NoSQL")
                    .setCapabilities(Capabilities.newBuilder()
                            .setSink(Sink.newBuilder()
                                    .addSupportedDataTypes(DataType.DATA_TYPE_JSON_ID)))
                    .build());
            responseObserver.onCompleted();
        }

        private <T> NsHelper getNsHelper(StreamObserver<T> responseObserver, String namespace) {
            NsHelper helper = this.nsHelpers.get(namespace);
            if (helper == null) {
                String[] splittedNs = namespace.split("\\.");
                if (splittedNs.length != 2) {
                    responseObserver.onError(
                            Status.INVALID_ARGUMENT.withDescription("Invalid namespace " + namespace).asException());
                    return null;
                }
                helper = new NsHelper();
                helper.container = this.client.getDatabase(splittedNs[0]).getContainer(splittedNs[1]);
                helper.pkd = helper.container.read().getProperties().getPartitionKeyDefinition();
                this.nsHelpers.put(namespace, helper);
            }
            return helper;
        }

        private boolean doOps(CosmosContainer container, List<CosmosItemOperation> inputOps, int retries) {
            List<CosmosItemOperation> ops = inputOps;
            boolean ok = true;
            for (int i = 0; i <= retries; i++) {
                List<CosmosItemOperation> retryOps = new ArrayList<CosmosItemOperation>();
                for (CosmosBulkOperationResponse<Object> r : container.executeBulkOperations(ops)) {
                    if (r.getException() != null) {
                        ok = false;
                        r.getException().printStackTrace();
                    }
                    if (!r.getResponse().isSuccessStatusCode()) {
                        retryOps.add(r.getOperation());
                    }
                }
                if (ok && retryOps.size() > 0) {
                    if (i == retries) {
                        System.out.println("" + retryOps.size() + " items failed after " + retries + " retries.");
                    } else {
                        ops = retryOps;
                        try {
                            int delay = (int)(100 * Math.pow(1.5, i) + Math.random() * 50);
                            Thread.sleep(delay);
                        } catch (Exception e) {
                        }
                    }
                } else {
                    return ok;
                }
            }
            return ok;
        }

        @Override
        public void writeData(WriteDataRequest request, StreamObserver<WriteDataResponse> responseObserver) {
            NsHelper helper = getNsHelper(responseObserver, request.getNamespace());
            if (helper == null) {
                return;
            }

            List<CosmosItemOperation> ops = new ArrayList<CosmosItemOperation>(request.getDataCount());
            for (ByteString data : request.getDataList()) {
                Document d = new Document(data.toByteArray());
                PartitionKey k = PartitionKeyHelper.extractPartitionKeyFromDocument(d, helper.pkd);
                ops.add(CosmosBulkOperations.getUpsertItemOperation(d, k));
            }

            boolean ok = doOps(helper.container, ops, 3);
            if (!ok) {
                responseObserver.onError(Status.ABORTED.asException());
                return;
            }
            responseObserver.onNext(WriteDataResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void writeUpdates(WriteUpdatesRequest request, StreamObserver<WriteUpdatesResponse> responseObserver) {
            NsHelper helper = getNsHelper(responseObserver, request.getNamespace());
            if (helper == null) {
                return;
            }

            HashSet<String> seen = new HashSet<>();

            List<CosmosItemOperation> ops = new ArrayList<CosmosItemOperation>(request.getUpdatesCount());
            for (Update u : request.getUpdatesList().reversed()) {
                String id = BsonHelper.getId(u.getIdList());
                if (seen.contains(id)) {
                    continue;
                }
                seen.add(id);
                switch (u.getType()) {
                    case UPDATE_TYPE_INSERT:
                    case UPDATE_TYPE_UPDATE:
                        Document d = new Document(u.getData().toByteArray());
                        PartitionKey k = PartitionKeyHelper.extractPartitionKeyFromDocument(d, helper.pkd);
                        ops.add(CosmosBulkOperations.getUpsertItemOperation(d, k));
                        break;
                    case UPDATE_TYPE_DELETE:
                        PartitionKey deletePK = BsonHelper.getPartitionKey(u.getIdList());
                        ops.add(CosmosBulkOperations.getDeleteItemOperation(id, deletePK));
                        break;
                    default:
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Unsupported type " + u.getType()).asException());
                        return;
                }
            }

            boolean ok = doOps(helper.container, ops, 3);
            if (!ok) {
                responseObserver.onError(Status.ABORTED.asException());
                return;
            }

            responseObserver.onNext(WriteUpdatesResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    }
}
