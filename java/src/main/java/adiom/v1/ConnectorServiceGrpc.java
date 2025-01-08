package adiom.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.0)",
    comments = "Source: adiom/v1/adiom.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ConnectorServiceGrpc {

  private ConnectorServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "adiom.v1.ConnectorService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.GetInfoRequest,
      adiom.v1.Messages.GetInfoResponse> getGetInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetInfo",
      requestType = adiom.v1.Messages.GetInfoRequest.class,
      responseType = adiom.v1.Messages.GetInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.GetInfoRequest,
      adiom.v1.Messages.GetInfoResponse> getGetInfoMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.GetInfoRequest, adiom.v1.Messages.GetInfoResponse> getGetInfoMethod;
    if ((getGetInfoMethod = ConnectorServiceGrpc.getGetInfoMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getGetInfoMethod = ConnectorServiceGrpc.getGetInfoMethod) == null) {
          ConnectorServiceGrpc.getGetInfoMethod = getGetInfoMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.GetInfoRequest, adiom.v1.Messages.GetInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("GetInfo"))
              .build();
        }
      }
    }
    return getGetInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.GetNamespaceMetadataRequest,
      adiom.v1.Messages.GetNamespaceMetadataResponse> getGetNamespaceMetadataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetNamespaceMetadata",
      requestType = adiom.v1.Messages.GetNamespaceMetadataRequest.class,
      responseType = adiom.v1.Messages.GetNamespaceMetadataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.GetNamespaceMetadataRequest,
      adiom.v1.Messages.GetNamespaceMetadataResponse> getGetNamespaceMetadataMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.GetNamespaceMetadataRequest, adiom.v1.Messages.GetNamespaceMetadataResponse> getGetNamespaceMetadataMethod;
    if ((getGetNamespaceMetadataMethod = ConnectorServiceGrpc.getGetNamespaceMetadataMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getGetNamespaceMetadataMethod = ConnectorServiceGrpc.getGetNamespaceMetadataMethod) == null) {
          ConnectorServiceGrpc.getGetNamespaceMetadataMethod = getGetNamespaceMetadataMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.GetNamespaceMetadataRequest, adiom.v1.Messages.GetNamespaceMetadataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetNamespaceMetadata"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetNamespaceMetadataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetNamespaceMetadataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("GetNamespaceMetadata"))
              .build();
        }
      }
    }
    return getGetNamespaceMetadataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.WriteDataRequest,
      adiom.v1.Messages.WriteDataResponse> getWriteDataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteData",
      requestType = adiom.v1.Messages.WriteDataRequest.class,
      responseType = adiom.v1.Messages.WriteDataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.WriteDataRequest,
      adiom.v1.Messages.WriteDataResponse> getWriteDataMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.WriteDataRequest, adiom.v1.Messages.WriteDataResponse> getWriteDataMethod;
    if ((getWriteDataMethod = ConnectorServiceGrpc.getWriteDataMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getWriteDataMethod = ConnectorServiceGrpc.getWriteDataMethod) == null) {
          ConnectorServiceGrpc.getWriteDataMethod = getWriteDataMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.WriteDataRequest, adiom.v1.Messages.WriteDataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteData"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.WriteDataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.WriteDataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("WriteData"))
              .build();
        }
      }
    }
    return getWriteDataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.WriteUpdatesRequest,
      adiom.v1.Messages.WriteUpdatesResponse> getWriteUpdatesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "WriteUpdates",
      requestType = adiom.v1.Messages.WriteUpdatesRequest.class,
      responseType = adiom.v1.Messages.WriteUpdatesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.WriteUpdatesRequest,
      adiom.v1.Messages.WriteUpdatesResponse> getWriteUpdatesMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.WriteUpdatesRequest, adiom.v1.Messages.WriteUpdatesResponse> getWriteUpdatesMethod;
    if ((getWriteUpdatesMethod = ConnectorServiceGrpc.getWriteUpdatesMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getWriteUpdatesMethod = ConnectorServiceGrpc.getWriteUpdatesMethod) == null) {
          ConnectorServiceGrpc.getWriteUpdatesMethod = getWriteUpdatesMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.WriteUpdatesRequest, adiom.v1.Messages.WriteUpdatesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "WriteUpdates"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.WriteUpdatesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.WriteUpdatesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("WriteUpdates"))
              .build();
        }
      }
    }
    return getWriteUpdatesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.GeneratePlanRequest,
      adiom.v1.Messages.GeneratePlanResponse> getGeneratePlanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GeneratePlan",
      requestType = adiom.v1.Messages.GeneratePlanRequest.class,
      responseType = adiom.v1.Messages.GeneratePlanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.GeneratePlanRequest,
      adiom.v1.Messages.GeneratePlanResponse> getGeneratePlanMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.GeneratePlanRequest, adiom.v1.Messages.GeneratePlanResponse> getGeneratePlanMethod;
    if ((getGeneratePlanMethod = ConnectorServiceGrpc.getGeneratePlanMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getGeneratePlanMethod = ConnectorServiceGrpc.getGeneratePlanMethod) == null) {
          ConnectorServiceGrpc.getGeneratePlanMethod = getGeneratePlanMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.GeneratePlanRequest, adiom.v1.Messages.GeneratePlanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GeneratePlan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GeneratePlanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GeneratePlanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("GeneratePlan"))
              .build();
        }
      }
    }
    return getGeneratePlanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.ListDataRequest,
      adiom.v1.Messages.ListDataResponse> getListDataMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListData",
      requestType = adiom.v1.Messages.ListDataRequest.class,
      responseType = adiom.v1.Messages.ListDataResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.ListDataRequest,
      adiom.v1.Messages.ListDataResponse> getListDataMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.ListDataRequest, adiom.v1.Messages.ListDataResponse> getListDataMethod;
    if ((getListDataMethod = ConnectorServiceGrpc.getListDataMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getListDataMethod = ConnectorServiceGrpc.getListDataMethod) == null) {
          ConnectorServiceGrpc.getListDataMethod = getListDataMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.ListDataRequest, adiom.v1.Messages.ListDataResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListData"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.ListDataRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.ListDataResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("ListData"))
              .build();
        }
      }
    }
    return getListDataMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.StreamUpdatesRequest,
      adiom.v1.Messages.StreamUpdatesResponse> getStreamUpdatesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamUpdates",
      requestType = adiom.v1.Messages.StreamUpdatesRequest.class,
      responseType = adiom.v1.Messages.StreamUpdatesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.StreamUpdatesRequest,
      adiom.v1.Messages.StreamUpdatesResponse> getStreamUpdatesMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.StreamUpdatesRequest, adiom.v1.Messages.StreamUpdatesResponse> getStreamUpdatesMethod;
    if ((getStreamUpdatesMethod = ConnectorServiceGrpc.getStreamUpdatesMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getStreamUpdatesMethod = ConnectorServiceGrpc.getStreamUpdatesMethod) == null) {
          ConnectorServiceGrpc.getStreamUpdatesMethod = getStreamUpdatesMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.StreamUpdatesRequest, adiom.v1.Messages.StreamUpdatesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamUpdates"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.StreamUpdatesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.StreamUpdatesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("StreamUpdates"))
              .build();
        }
      }
    }
    return getStreamUpdatesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.StreamLSNRequest,
      adiom.v1.Messages.StreamLSNResponse> getStreamLSNMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StreamLSN",
      requestType = adiom.v1.Messages.StreamLSNRequest.class,
      responseType = adiom.v1.Messages.StreamLSNResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.StreamLSNRequest,
      adiom.v1.Messages.StreamLSNResponse> getStreamLSNMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.StreamLSNRequest, adiom.v1.Messages.StreamLSNResponse> getStreamLSNMethod;
    if ((getStreamLSNMethod = ConnectorServiceGrpc.getStreamLSNMethod) == null) {
      synchronized (ConnectorServiceGrpc.class) {
        if ((getStreamLSNMethod = ConnectorServiceGrpc.getStreamLSNMethod) == null) {
          ConnectorServiceGrpc.getStreamLSNMethod = getStreamLSNMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.StreamLSNRequest, adiom.v1.Messages.StreamLSNResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StreamLSN"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.StreamLSNRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.StreamLSNResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ConnectorServiceMethodDescriptorSupplier("StreamLSN"))
              .build();
        }
      }
    }
    return getStreamLSNMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ConnectorServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConnectorServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConnectorServiceStub>() {
        @java.lang.Override
        public ConnectorServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConnectorServiceStub(channel, callOptions);
        }
      };
    return ConnectorServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ConnectorServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConnectorServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConnectorServiceBlockingStub>() {
        @java.lang.Override
        public ConnectorServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConnectorServiceBlockingStub(channel, callOptions);
        }
      };
    return ConnectorServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ConnectorServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ConnectorServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ConnectorServiceFutureStub>() {
        @java.lang.Override
        public ConnectorServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ConnectorServiceFutureStub(channel, callOptions);
        }
      };
    return ConnectorServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void getInfo(adiom.v1.Messages.GetInfoRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetInfoMethod(), responseObserver);
    }

    /**
     */
    default void getNamespaceMetadata(adiom.v1.Messages.GetNamespaceMetadataRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetNamespaceMetadataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetNamespaceMetadataMethod(), responseObserver);
    }

    /**
     * <pre>
     * Sink
     * </pre>
     */
    default void writeData(adiom.v1.Messages.WriteDataRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.WriteDataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteDataMethod(), responseObserver);
    }

    /**
     */
    default void writeUpdates(adiom.v1.Messages.WriteUpdatesRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.WriteUpdatesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getWriteUpdatesMethod(), responseObserver);
    }

    /**
     * <pre>
     * Source
     * </pre>
     */
    default void generatePlan(adiom.v1.Messages.GeneratePlanRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GeneratePlanResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGeneratePlanMethod(), responseObserver);
    }

    /**
     */
    default void listData(adiom.v1.Messages.ListDataRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.ListDataResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListDataMethod(), responseObserver);
    }

    /**
     */
    default void streamUpdates(adiom.v1.Messages.StreamUpdatesRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.StreamUpdatesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStreamUpdatesMethod(), responseObserver);
    }

    /**
     */
    default void streamLSN(adiom.v1.Messages.StreamLSNRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.StreamLSNResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getStreamLSNMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ConnectorService.
   */
  public static abstract class ConnectorServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ConnectorServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ConnectorService.
   */
  public static final class ConnectorServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ConnectorServiceStub> {
    private ConnectorServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConnectorServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConnectorServiceStub(channel, callOptions);
    }

    /**
     */
    public void getInfo(adiom.v1.Messages.GetInfoRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getNamespaceMetadata(adiom.v1.Messages.GetNamespaceMetadataRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetNamespaceMetadataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetNamespaceMetadataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Sink
     * </pre>
     */
    public void writeData(adiom.v1.Messages.WriteDataRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.WriteDataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteDataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void writeUpdates(adiom.v1.Messages.WriteUpdatesRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.WriteUpdatesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getWriteUpdatesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Source
     * </pre>
     */
    public void generatePlan(adiom.v1.Messages.GeneratePlanRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GeneratePlanResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGeneratePlanMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void listData(adiom.v1.Messages.ListDataRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.ListDataResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListDataMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void streamUpdates(adiom.v1.Messages.StreamUpdatesRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.StreamUpdatesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getStreamUpdatesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void streamLSN(adiom.v1.Messages.StreamLSNRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.StreamLSNResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
          getChannel().newCall(getStreamLSNMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ConnectorService.
   */
  public static final class ConnectorServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ConnectorServiceBlockingStub> {
    private ConnectorServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConnectorServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConnectorServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public adiom.v1.Messages.GetInfoResponse getInfo(adiom.v1.Messages.GetInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public adiom.v1.Messages.GetNamespaceMetadataResponse getNamespaceMetadata(adiom.v1.Messages.GetNamespaceMetadataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetNamespaceMetadataMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Sink
     * </pre>
     */
    public adiom.v1.Messages.WriteDataResponse writeData(adiom.v1.Messages.WriteDataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteDataMethod(), getCallOptions(), request);
    }

    /**
     */
    public adiom.v1.Messages.WriteUpdatesResponse writeUpdates(adiom.v1.Messages.WriteUpdatesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getWriteUpdatesMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Source
     * </pre>
     */
    public adiom.v1.Messages.GeneratePlanResponse generatePlan(adiom.v1.Messages.GeneratePlanRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGeneratePlanMethod(), getCallOptions(), request);
    }

    /**
     */
    public adiom.v1.Messages.ListDataResponse listData(adiom.v1.Messages.ListDataRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListDataMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<adiom.v1.Messages.StreamUpdatesResponse> streamUpdates(
        adiom.v1.Messages.StreamUpdatesRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getStreamUpdatesMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<adiom.v1.Messages.StreamLSNResponse> streamLSN(
        adiom.v1.Messages.StreamLSNRequest request) {
      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
          getChannel(), getStreamLSNMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ConnectorService.
   */
  public static final class ConnectorServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ConnectorServiceFutureStub> {
    private ConnectorServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ConnectorServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ConnectorServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.GetInfoResponse> getInfo(
        adiom.v1.Messages.GetInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.GetNamespaceMetadataResponse> getNamespaceMetadata(
        adiom.v1.Messages.GetNamespaceMetadataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetNamespaceMetadataMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Sink
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.WriteDataResponse> writeData(
        adiom.v1.Messages.WriteDataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteDataMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.WriteUpdatesResponse> writeUpdates(
        adiom.v1.Messages.WriteUpdatesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getWriteUpdatesMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Source
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.GeneratePlanResponse> generatePlan(
        adiom.v1.Messages.GeneratePlanRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGeneratePlanMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.ListDataResponse> listData(
        adiom.v1.Messages.ListDataRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListDataMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_INFO = 0;
  private static final int METHODID_GET_NAMESPACE_METADATA = 1;
  private static final int METHODID_WRITE_DATA = 2;
  private static final int METHODID_WRITE_UPDATES = 3;
  private static final int METHODID_GENERATE_PLAN = 4;
  private static final int METHODID_LIST_DATA = 5;
  private static final int METHODID_STREAM_UPDATES = 6;
  private static final int METHODID_STREAM_LSN = 7;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_INFO:
          serviceImpl.getInfo((adiom.v1.Messages.GetInfoRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.GetInfoResponse>) responseObserver);
          break;
        case METHODID_GET_NAMESPACE_METADATA:
          serviceImpl.getNamespaceMetadata((adiom.v1.Messages.GetNamespaceMetadataRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.GetNamespaceMetadataResponse>) responseObserver);
          break;
        case METHODID_WRITE_DATA:
          serviceImpl.writeData((adiom.v1.Messages.WriteDataRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.WriteDataResponse>) responseObserver);
          break;
        case METHODID_WRITE_UPDATES:
          serviceImpl.writeUpdates((adiom.v1.Messages.WriteUpdatesRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.WriteUpdatesResponse>) responseObserver);
          break;
        case METHODID_GENERATE_PLAN:
          serviceImpl.generatePlan((adiom.v1.Messages.GeneratePlanRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.GeneratePlanResponse>) responseObserver);
          break;
        case METHODID_LIST_DATA:
          serviceImpl.listData((adiom.v1.Messages.ListDataRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.ListDataResponse>) responseObserver);
          break;
        case METHODID_STREAM_UPDATES:
          serviceImpl.streamUpdates((adiom.v1.Messages.StreamUpdatesRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.StreamUpdatesResponse>) responseObserver);
          break;
        case METHODID_STREAM_LSN:
          serviceImpl.streamLSN((adiom.v1.Messages.StreamLSNRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.StreamLSNResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getGetInfoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.GetInfoRequest,
              adiom.v1.Messages.GetInfoResponse>(
                service, METHODID_GET_INFO)))
        .addMethod(
          getGetNamespaceMetadataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.GetNamespaceMetadataRequest,
              adiom.v1.Messages.GetNamespaceMetadataResponse>(
                service, METHODID_GET_NAMESPACE_METADATA)))
        .addMethod(
          getWriteDataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.WriteDataRequest,
              adiom.v1.Messages.WriteDataResponse>(
                service, METHODID_WRITE_DATA)))
        .addMethod(
          getWriteUpdatesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.WriteUpdatesRequest,
              adiom.v1.Messages.WriteUpdatesResponse>(
                service, METHODID_WRITE_UPDATES)))
        .addMethod(
          getGeneratePlanMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.GeneratePlanRequest,
              adiom.v1.Messages.GeneratePlanResponse>(
                service, METHODID_GENERATE_PLAN)))
        .addMethod(
          getListDataMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.ListDataRequest,
              adiom.v1.Messages.ListDataResponse>(
                service, METHODID_LIST_DATA)))
        .addMethod(
          getStreamUpdatesMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              adiom.v1.Messages.StreamUpdatesRequest,
              adiom.v1.Messages.StreamUpdatesResponse>(
                service, METHODID_STREAM_UPDATES)))
        .addMethod(
          getStreamLSNMethod(),
          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
            new MethodHandlers<
              adiom.v1.Messages.StreamLSNRequest,
              adiom.v1.Messages.StreamLSNResponse>(
                service, METHODID_STREAM_LSN)))
        .build();
  }

  private static abstract class ConnectorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ConnectorServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return adiom.v1.Adiom.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ConnectorService");
    }
  }

  private static final class ConnectorServiceFileDescriptorSupplier
      extends ConnectorServiceBaseDescriptorSupplier {
    ConnectorServiceFileDescriptorSupplier() {}
  }

  private static final class ConnectorServiceMethodDescriptorSupplier
      extends ConnectorServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ConnectorServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (ConnectorServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ConnectorServiceFileDescriptorSupplier())
              .addMethod(getGetInfoMethod())
              .addMethod(getGetNamespaceMetadataMethod())
              .addMethod(getWriteDataMethod())
              .addMethod(getWriteUpdatesMethod())
              .addMethod(getGeneratePlanMethod())
              .addMethod(getListDataMethod())
              .addMethod(getStreamUpdatesMethod())
              .addMethod(getStreamLSNMethod())
              .build();
        }
      }
    }
    return result;
  }
}
