package adiom.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.0)",
    comments = "Source: adiom/v1/vector.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class ChunkingServiceGrpc {

  private ChunkingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "adiom.v1.ChunkingService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<adiom.v1.Vector.GetSupportedDataTypesRequest,
      adiom.v1.Vector.GetSupportedDataTypesResponse> getGetSupportedDataTypesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSupportedDataTypes",
      requestType = adiom.v1.Vector.GetSupportedDataTypesRequest.class,
      responseType = adiom.v1.Vector.GetSupportedDataTypesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Vector.GetSupportedDataTypesRequest,
      adiom.v1.Vector.GetSupportedDataTypesResponse> getGetSupportedDataTypesMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Vector.GetSupportedDataTypesRequest, adiom.v1.Vector.GetSupportedDataTypesResponse> getGetSupportedDataTypesMethod;
    if ((getGetSupportedDataTypesMethod = ChunkingServiceGrpc.getGetSupportedDataTypesMethod) == null) {
      synchronized (ChunkingServiceGrpc.class) {
        if ((getGetSupportedDataTypesMethod = ChunkingServiceGrpc.getGetSupportedDataTypesMethod) == null) {
          ChunkingServiceGrpc.getGetSupportedDataTypesMethod = getGetSupportedDataTypesMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Vector.GetSupportedDataTypesRequest, adiom.v1.Vector.GetSupportedDataTypesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSupportedDataTypes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetSupportedDataTypesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetSupportedDataTypesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ChunkingServiceMethodDescriptorSupplier("GetSupportedDataTypes"))
              .build();
        }
      }
    }
    return getGetSupportedDataTypesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Vector.GetChunkedRequest,
      adiom.v1.Vector.GetChunkedResponse> getGetChunkedMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetChunked",
      requestType = adiom.v1.Vector.GetChunkedRequest.class,
      responseType = adiom.v1.Vector.GetChunkedResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Vector.GetChunkedRequest,
      adiom.v1.Vector.GetChunkedResponse> getGetChunkedMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Vector.GetChunkedRequest, adiom.v1.Vector.GetChunkedResponse> getGetChunkedMethod;
    if ((getGetChunkedMethod = ChunkingServiceGrpc.getGetChunkedMethod) == null) {
      synchronized (ChunkingServiceGrpc.class) {
        if ((getGetChunkedMethod = ChunkingServiceGrpc.getGetChunkedMethod) == null) {
          ChunkingServiceGrpc.getGetChunkedMethod = getGetChunkedMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Vector.GetChunkedRequest, adiom.v1.Vector.GetChunkedResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetChunked"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetChunkedRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetChunkedResponse.getDefaultInstance()))
              .setSchemaDescriptor(new ChunkingServiceMethodDescriptorSupplier("GetChunked"))
              .build();
        }
      }
    }
    return getGetChunkedMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static ChunkingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ChunkingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ChunkingServiceStub>() {
        @java.lang.Override
        public ChunkingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ChunkingServiceStub(channel, callOptions);
        }
      };
    return ChunkingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static ChunkingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ChunkingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ChunkingServiceBlockingStub>() {
        @java.lang.Override
        public ChunkingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ChunkingServiceBlockingStub(channel, callOptions);
        }
      };
    return ChunkingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static ChunkingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<ChunkingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<ChunkingServiceFutureStub>() {
        @java.lang.Override
        public ChunkingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new ChunkingServiceFutureStub(channel, callOptions);
        }
      };
    return ChunkingServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void getSupportedDataTypes(adiom.v1.Vector.GetSupportedDataTypesRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Vector.GetSupportedDataTypesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSupportedDataTypesMethod(), responseObserver);
    }

    /**
     */
    default void getChunked(adiom.v1.Vector.GetChunkedRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Vector.GetChunkedResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetChunkedMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service ChunkingService.
   */
  public static abstract class ChunkingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return ChunkingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service ChunkingService.
   */
  public static final class ChunkingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<ChunkingServiceStub> {
    private ChunkingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ChunkingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ChunkingServiceStub(channel, callOptions);
    }

    /**
     */
    public void getSupportedDataTypes(adiom.v1.Vector.GetSupportedDataTypesRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Vector.GetSupportedDataTypesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetSupportedDataTypesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getChunked(adiom.v1.Vector.GetChunkedRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Vector.GetChunkedResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetChunkedMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service ChunkingService.
   */
  public static final class ChunkingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<ChunkingServiceBlockingStub> {
    private ChunkingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ChunkingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ChunkingServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public adiom.v1.Vector.GetSupportedDataTypesResponse getSupportedDataTypes(adiom.v1.Vector.GetSupportedDataTypesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSupportedDataTypesMethod(), getCallOptions(), request);
    }

    /**
     */
    public adiom.v1.Vector.GetChunkedResponse getChunked(adiom.v1.Vector.GetChunkedRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetChunkedMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service ChunkingService.
   */
  public static final class ChunkingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<ChunkingServiceFutureStub> {
    private ChunkingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected ChunkingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new ChunkingServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Vector.GetSupportedDataTypesResponse> getSupportedDataTypes(
        adiom.v1.Vector.GetSupportedDataTypesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetSupportedDataTypesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Vector.GetChunkedResponse> getChunked(
        adiom.v1.Vector.GetChunkedRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetChunkedMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_SUPPORTED_DATA_TYPES = 0;
  private static final int METHODID_GET_CHUNKED = 1;

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
        case METHODID_GET_SUPPORTED_DATA_TYPES:
          serviceImpl.getSupportedDataTypes((adiom.v1.Vector.GetSupportedDataTypesRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Vector.GetSupportedDataTypesResponse>) responseObserver);
          break;
        case METHODID_GET_CHUNKED:
          serviceImpl.getChunked((adiom.v1.Vector.GetChunkedRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Vector.GetChunkedResponse>) responseObserver);
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
          getGetSupportedDataTypesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Vector.GetSupportedDataTypesRequest,
              adiom.v1.Vector.GetSupportedDataTypesResponse>(
                service, METHODID_GET_SUPPORTED_DATA_TYPES)))
        .addMethod(
          getGetChunkedMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Vector.GetChunkedRequest,
              adiom.v1.Vector.GetChunkedResponse>(
                service, METHODID_GET_CHUNKED)))
        .build();
  }

  private static abstract class ChunkingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    ChunkingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return adiom.v1.Vector.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("ChunkingService");
    }
  }

  private static final class ChunkingServiceFileDescriptorSupplier
      extends ChunkingServiceBaseDescriptorSupplier {
    ChunkingServiceFileDescriptorSupplier() {}
  }

  private static final class ChunkingServiceMethodDescriptorSupplier
      extends ChunkingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    ChunkingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (ChunkingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new ChunkingServiceFileDescriptorSupplier())
              .addMethod(getGetSupportedDataTypesMethod())
              .addMethod(getGetChunkedMethod())
              .build();
        }
      }
    }
    return result;
  }
}
