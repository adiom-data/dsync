package adiom;

import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.implementation.Document;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers;
import com.azure.cosmos.implementation.PartitionKeyHelper;
import com.azure.cosmos.implementation.ImplementationBridgeHelpers.CosmosAsyncContainerHelper.CosmosAsyncContainerAccessor;
import com.azure.cosmos.models.CosmosBulkOperationResponse;
import com.azure.cosmos.models.CosmosBulkOperations;
import com.azure.cosmos.models.CosmosChangeFeedRequestOptions;
import com.azure.cosmos.models.CosmosItemOperation;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.FeedResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.PartitionKeyDefinition;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteString.Output;

import adiom.v1.ConnectorServiceGrpc;
import adiom.v1.Messages.Capabilities;
import adiom.v1.Messages.DataType;
import adiom.v1.Messages.GeneratePlanRequest;
import adiom.v1.Messages.GeneratePlanResponse;
import adiom.v1.Messages.GetInfoRequest;
import adiom.v1.Messages.GetInfoResponse;
import adiom.v1.Messages.GetNamespaceMetadataRequest;
import adiom.v1.Messages.GetNamespaceMetadataResponse;
import adiom.v1.Messages.ListDataRequest;
import adiom.v1.Messages.ListDataResponse;
import adiom.v1.Messages.Partition;
import adiom.v1.Messages.StreamLSNRequest;
import adiom.v1.Messages.StreamLSNResponse;
import adiom.v1.Messages.StreamUpdatesRequest;
import adiom.v1.Messages.StreamUpdatesResponse;
import adiom.v1.Messages.Update;
import adiom.v1.Messages.UpdatesPartition;
import adiom.v1.Messages.WriteDataRequest;
import adiom.v1.Messages.WriteDataResponse;
import adiom.v1.Messages.WriteUpdatesRequest;
import adiom.v1.Messages.WriteUpdatesResponse;
import adiom.v1.Messages.Capabilities.Sink;
import adiom.v1.Messages.Capabilities.Source;
import io.grpc.Context;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerCredentials;
import io.grpc.Status;
import io.grpc.TlsServerCredentials;
import io.grpc.opentelemetry.GrpcOpenTelemetry;
import io.grpc.protobuf.services.ProtoReflectionServiceV1;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import reactor.core.publisher.Flux;

public class Main {

    private static final int COSMOS_MAX_ITEM_BYTES = 2_000_000; // Azure Cosmos DB per-item max (2MB)

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static final List<String> DEFAULT_COSMOS_INTERNAL_KEYS = Arrays.asList("_rid", "_self", "_etag", "_attachments", "_ts");
    private static List<String> CosmosInternalKeys;
    private static Config CONFIG;
    private static Cache<Long, CacheItem> CACHE = Caffeine.newBuilder().expireAfterAccess(150, TimeUnit.SECONDS)
            .build();
    private static long currentIndex = 1;

    public static void main(String[] args) {
        OpenTelemetry otel = AutoConfiguredOpenTelemetrySdk.initialize().getOpenTelemetrySdk();
        OpenTelemetryAppender.install(otel);
        logger.atInfo().addKeyValue("type", "cosmos").addKeyValue("lang", "java").log("cosmos connector");
        GrpcOpenTelemetry grpcOpenTelemetry = GrpcOpenTelemetry.newBuilder()
                .sdk(otel)
                .build();
        grpcOpenTelemetry.registerGlobal();
        if (args.length < 3) {
            System.out.println("3 Required arguments: port url key");
            return;
        }
        try {
            if (args.length > 3) {
                Yaml yaml = new Yaml();
                InputStream is = new FileInputStream(args[3]);
                Config cfg = yaml.loadAs(is, Config.class);
                CONFIG = cfg;
                is.close();
            } else {
                CONFIG = new Config();
                CONFIG.namespaces = new java.util.HashMap<String, NamespaceConfig>();
            }
        } catch (Exception e) {
            System.out.println("Could not read config");
            e.printStackTrace();
            return;
        }

        if (CONFIG.cosmosInternalKeys != null && !CONFIG.cosmosInternalKeys.isEmpty()) {
            CosmosInternalKeys = CONFIG.cosmosInternalKeys;
        } else {
            CosmosInternalKeys = DEFAULT_COSMOS_INTERNAL_KEYS;
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
        public CosmosAsyncContainer asyncContainer;
        public PartitionKeyDefinition pkd;
    }

    private static class MyConn extends ConnectorServiceGrpc.ConnectorServiceImplBase {

        private CosmosClient client;
        private CosmosAsyncClient asyncClient;
        private ConcurrentHashMap<String, NsHelper> nsHelpers;

        public MyConn(String endpoint, String key) {
            super();
            this.nsHelpers = new ConcurrentHashMap<>();
            this.client = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .buildClient();
            this.asyncClient = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .buildAsyncClient();
        }

        @Override
        public void getInfo(GetInfoRequest request, StreamObserver<GetInfoResponse> responseObserver) {
            responseObserver.onNext(GetInfoResponse.newBuilder()
                    .setDbType("CosmosDB-NoSQL")
                    .setCapabilities(Capabilities.newBuilder()
                            .setSource(Source.newBuilder()
                                    .addSupportedDataTypes(DataType.DATA_TYPE_JSON_ID)
                                    .setMultiNamespacePlan(true))
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
                helper.asyncContainer = this.asyncClient.getDatabase(splittedNs[0]).getContainer(splittedNs[1]);
                try {
                    helper.pkd = helper.container.read().getProperties().getPartitionKeyDefinition();
                } catch (com.azure.cosmos.CosmosException e) {
                    responseObserver.onError(
                            Status.INTERNAL.withDescription(
                                    "Failed to read container properties for '" + namespace + "'. Does it exist?")
                                    .asException());
                    return null;
                }
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
                        logger.atWarn().addKeyValue("size", retryOps.size()).addKeyValue("retries", retries)
                                .log("Failed after retries.");
                        return false;
                    } else {
                        ops = retryOps;
                        try {
                            int delay = (int) (100 * Math.pow(1.5, i) + Math.random() * 50);
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
                // Check if document size exceeds 2MB limit
                int size = data.size(); // avoid full copy just to measure
                if (size > COSMOS_MAX_ITEM_BYTES) {
                    int previewLen = Math.min(100, size);
                    String previewUtf8 = data.substring(0, previewLen).toStringUtf8();
                    logger.atWarn()
                          .addKeyValue("namespace", request.getNamespace())
                          .addKeyValue("docSizeBytes", size)
                          .addKeyValue("limitBytes", COSMOS_MAX_ITEM_BYTES)
                          .addKeyValue("first100Chars", previewUtf8)
                          .log("Skipping oversized document in writeData.");
                    continue;
                }
                byte[] docBytes = data.toByteArray();
                Document d = new Document(docBytes);
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
                        // Check if document size exceeds 2MB limit
                        int size = u.getData().size(); // avoid full copy just to measure
                        if (size > COSMOS_MAX_ITEM_BYTES) {
                            int previewLen = Math.min(100, size);
                            String previewUtf8 = u.getData().substring(0, previewLen).toStringUtf8();
                            logger.atWarn()
                                .addKeyValue("namespace", request.getNamespace())
                                .addKeyValue("docSizeBytes", size)
                                .addKeyValue("limitBytes", COSMOS_MAX_ITEM_BYTES)
                                .addKeyValue("first100Chars", previewUtf8)
                                .log("Skipping oversized document in writeUpdates.");
                            continue;
                        }

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

        @Override
        public void generatePlan(GeneratePlanRequest request, StreamObserver<GeneratePlanResponse> responseObserver) {
            GeneratePlanResponse.Builder responseBuilder = GeneratePlanResponse.newBuilder();
            CosmosAsyncContainerAccessor caca = ImplementationBridgeHelpers.CosmosAsyncContainerHelper
                    .getCosmosAsyncContainerAccessor();
            for (String namespace : request.getNamespacesList()) {
                NamespaceConfig cfg = CONFIG.namespaces.get(namespace);
                int partitionFanout = 0;
                if (cfg != null) {
                    partitionFanout = cfg.partitionfanout;
                }
                NsHelper helper = getNsHelper(responseObserver, namespace);
                if (helper == null) {
                    return;
                }

                for (FeedRange fr : helper.container.getFeedRanges()) {
                    Integer count = helper.container
                            .queryItems("SELECT VALUE COUNT(1) FROM c",
                                    new CosmosQueryRequestOptions().setFeedRange(fr), Integer.class)
                            .stream().findFirst().orElse(0);

                    if (partitionFanout > 1) {
                        List<FeedRange> frs = caca.trySplitFeedRange(helper.asyncContainer, fr, partitionFanout)
                                .block();
                        if (frs == null) {
                            frs = Arrays.asList(fr);
                        }
                        for (FeedRange splitFR : frs) {
                            responseBuilder.addPartitions(Partition.newBuilder().setNamespace(namespace)
                                    .setCursor(ByteString.copyFromUtf8(splitFR.toString()))
                                    .setEstimatedCount(count / frs.size()));
                        }
                    } else {
                        responseBuilder.addPartitions(Partition.newBuilder().setNamespace(namespace)
                                .setCursor(ByteString.copyFromUtf8(fr.toString())).setEstimatedCount(count));
                    }
                }

                CosmosChangeFeedRequestOptions ccfro = CosmosChangeFeedRequestOptions
                        .createForProcessingFromNow(FeedRange.forFullRange()).setMaxItemCount(1);
                UpdatesPartition.Builder updatesPartitionBuilder = UpdatesPartition.newBuilder()
                        .addNamespaces(namespace);
                for (FeedResponse<Object> fr : helper.container.queryChangeFeed(ccfro, Object.class).iterableByPage()) {
                    updatesPartitionBuilder.setCursor(ByteString.copyFromUtf8(fr.getContinuationToken()));
                }

                responseBuilder.addUpdatesPartitions(updatesPartitionBuilder);
            }

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void getNamespaceMetadata(GetNamespaceMetadataRequest request,
                StreamObserver<GetNamespaceMetadataResponse> responseObserver) {
            NsHelper helper = getNsHelper(responseObserver, request.getNamespace());
            if (helper == null) {
                return;
            }

            CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
            String query = "SELECT VALUE COUNT(1) FROM c";
            CosmosPagedIterable<Integer> results = helper.container.queryItems(query, queryOptions, Integer.class);
            Integer count = results.stream().findFirst().orElse(0);
            responseObserver.onNext(GetNamespaceMetadataResponse.newBuilder().setCount(count).build());
            responseObserver.onCompleted();
        }

        private synchronized long nextIndex() {
            return currentIndex++;
        }

        @Override
        public void listData(ListDataRequest request, StreamObserver<ListDataResponse> responseObserver) {
            String namespace = request.getPartition().getNamespace();

            if (!request.getCursor().isEmpty()) {
                try {
                    InputStream is = request.getCursor().newInput();
                    ObjectInputStream ois = new ObjectInputStream(is);
                    Cursor c = (Cursor) ois.readObject();
                    ois.close();
                    is.close();
                    // System.out.println(c.id + ": " + c.counter);
                    CacheItem ci = CACHE.getIfPresent(c.id);
                    if (ci == null) {
                        responseObserver.onError(Status.INVALID_ARGUMENT
                                .withDescription("Cursor no longer available: " + c.id).asException());
                        return;
                    }
                    ci.next(c.counter, responseObserver);
                    return;
                } catch (Exception e) {
                    responseObserver.onError(Status.INTERNAL.withCause(e).asException());
                    return;
                }
            }

            long idx = nextIndex();
            String feedRange = request.getPartition().getCursor().toStringUtf8();
            NsHelper helper = getNsHelper(responseObserver, namespace);
            if (helper == null) {
                return;
            }

            int pageSize = 1000;
            int prefetch = 5;
            NamespaceConfig cfg = CONFIG.namespaces.get(namespace);
            if (cfg != null) {
                if (cfg.pagesize > 0) {
                    pageSize = cfg.pagesize;
                }
                if (cfg.prefetch > 0) {
                    prefetch = cfg.prefetch;
                }
            }

            CosmosQueryRequestOptions opts = new CosmosQueryRequestOptions()
                    .setFeedRange(FeedRange.fromString(feedRange)).setMaxDegreeOfParallelism(0)
                    .setMaxBufferedItemCount(pageSize);
            CosmosPagedFlux<JsonNode> cpi = helper.asyncContainer.queryItems("select * from c", opts, JsonNode.class);
            Flux<FeedResponse<JsonNode>> flux = cpi.byPage(null, pageSize);
            Flux<ListDataResponse> ldrFlux = flux.zipWith(Flux.range(1, Integer.MAX_VALUE)).map(tup -> {
                FeedResponse<JsonNode> fr = tup.getT1();
                int counter = tup.getT2();
                ListDataResponse.Builder builder = ListDataResponse.newBuilder();
                for (JsonNode node : fr.getResults()) {
                    ObjectNode objectNode = (ObjectNode) (node);
                    objectNode.remove(CosmosInternalKeys);
                    builder.addData(ByteString.copyFromUtf8(node.toString()));
                }
                if (fr.getContinuationToken() != null) {
                    Cursor ci = new Cursor();
                    ci.id = idx;
                    ci.counter = counter;
                    try {
                        Output o = ByteString.newOutput();
                        ObjectOutputStream oos = new ObjectOutputStream(o);
                        oos.writeObject(ci);
                        builder.setNextCursor(o.toByteString());
                        // System.out.println("X " + idx + ": " + counter + ": " + ci.id + "-" +
                        // ci.counter);
                        oos.close();
                        o.close();
                    } catch (Exception e) {
                        throw new InternalError(e);
                    }
                }
                return builder.build();
            });
            // System.out.println(idx + ": " + feedRange);
            CacheItem ci = new CacheItem(ldrFlux.toIterable(prefetch).iterator());
            CACHE.put(idx, ci);
            ci.next(0, responseObserver);
            return;
        }

        @Override
        public void streamLSN(StreamLSNRequest request,
                StreamObserver<StreamLSNResponse> responseObserver) {
            responseObserver.onCompleted();
        }

        @Override
        public void streamUpdates(StreamUpdatesRequest request,
                StreamObserver<StreamUpdatesResponse> responseObserver) {
            if (request.getNamespacesCount() != 1) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("Must have exactly 1 namespace, but has " + request.getNamespacesCount())
                        .asException());
                return;
            }
            String namespace = request.getNamespaces(0);
            NsHelper helper = getNsHelper(responseObserver, namespace);
            if (helper == null) {
                return;
            }

            String continuation = request.getCursor().toStringUtf8();

            while (!Context.current().isCancelled()) {
                Iterable<FeedResponse<JsonNode>> it = helper.container.queryChangeFeed(CosmosChangeFeedRequestOptions
                        .createForProcessingFromContinuation(continuation).allVersionsAndDeletes(), JsonNode.class)
                        .iterableByPage();
                for (FeedResponse<JsonNode> fr : it) {
                    List<Update> updates = new ArrayList<>();
                    for (JsonNode node : fr.getResults()) {
                        JsonNode opType = node.get("metadata").get("operationType");
                        if (opType != null && opType.asText() == "delete") {
                            String id = node.get("metadata").get("id").asText();
                            updates.add(Update.newBuilder().setType(adiom.v1.Messages.UpdateType.UPDATE_TYPE_DELETE)
                                    .addId(BsonHelper.toId(id)).build());
                        } else {
                            adiom.v1.Messages.UpdateType typ = adiom.v1.Messages.UpdateType.UPDATE_TYPE_UPDATE;
                            if (opType != null && opType.asText() == "create") {
                                typ = adiom.v1.Messages.UpdateType.UPDATE_TYPE_INSERT;
                            }
                            JsonNode currentNode = node.get("current");
                            ObjectNode objectNode = (ObjectNode) (currentNode);
                            objectNode.remove(CosmosInternalKeys);
                            String id = currentNode.get("id").asText();
                            updates.add(Update.newBuilder().setType(typ)
                                    .setData(ByteString.copyFromUtf8(currentNode.toString())).addId(BsonHelper.toId(id))
                                    .build());
                        }
                    }

                    continuation = fr.getContinuationToken();

                    if (updates.size() > 0) {
                        StreamUpdatesResponse item = StreamUpdatesResponse.newBuilder().setNamespace(namespace)
                                .setNextCursor(ByteString.copyFromUtf8(continuation)).addAllUpdates(updates).build();
                        responseObserver.onNext(item);
                    }
                }
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                    break;
                }
            }
            responseObserver.onCompleted();
        }
    }
}
