package adiom.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.0)",
    comments = "Source: adiom/v1/vector.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class EmbeddingServiceGrpc {

  private EmbeddingServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "adiom.v1.EmbeddingService";

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
    if ((getGetSupportedDataTypesMethod = EmbeddingServiceGrpc.getGetSupportedDataTypesMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getGetSupportedDataTypesMethod = EmbeddingServiceGrpc.getGetSupportedDataTypesMethod) == null) {
          EmbeddingServiceGrpc.getGetSupportedDataTypesMethod = getGetSupportedDataTypesMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Vector.GetSupportedDataTypesRequest, adiom.v1.Vector.GetSupportedDataTypesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSupportedDataTypes"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetSupportedDataTypesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetSupportedDataTypesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("GetSupportedDataTypes"))
              .build();
        }
      }
    }
    return getGetSupportedDataTypesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Vector.GetEmbeddingRequest,
      adiom.v1.Vector.GetEmbeddingResponse> getGetEmbeddingMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetEmbedding",
      requestType = adiom.v1.Vector.GetEmbeddingRequest.class,
      responseType = adiom.v1.Vector.GetEmbeddingResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Vector.GetEmbeddingRequest,
      adiom.v1.Vector.GetEmbeddingResponse> getGetEmbeddingMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Vector.GetEmbeddingRequest, adiom.v1.Vector.GetEmbeddingResponse> getGetEmbeddingMethod;
    if ((getGetEmbeddingMethod = EmbeddingServiceGrpc.getGetEmbeddingMethod) == null) {
      synchronized (EmbeddingServiceGrpc.class) {
        if ((getGetEmbeddingMethod = EmbeddingServiceGrpc.getGetEmbeddingMethod) == null) {
          EmbeddingServiceGrpc.getGetEmbeddingMethod = getGetEmbeddingMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Vector.GetEmbeddingRequest, adiom.v1.Vector.GetEmbeddingResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetEmbedding"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetEmbeddingRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Vector.GetEmbeddingResponse.getDefaultInstance()))
              .setSchemaDescriptor(new EmbeddingServiceMethodDescriptorSupplier("GetEmbedding"))
              .build();
        }
      }
    }
    return getGetEmbeddingMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static EmbeddingServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceStub>() {
        @java.lang.Override
        public EmbeddingServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceStub(channel, callOptions);
        }
      };
    return EmbeddingServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static EmbeddingServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceBlockingStub>() {
        @java.lang.Override
        public EmbeddingServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceBlockingStub(channel, callOptions);
        }
      };
    return EmbeddingServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static EmbeddingServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<EmbeddingServiceFutureStub>() {
        @java.lang.Override
        public EmbeddingServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new EmbeddingServiceFutureStub(channel, callOptions);
        }
      };
    return EmbeddingServiceFutureStub.newStub(factory, channel);
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
    default void getEmbedding(adiom.v1.Vector.GetEmbeddingRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Vector.GetEmbeddingResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetEmbeddingMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service EmbeddingService.
   */
  public static abstract class EmbeddingServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return EmbeddingServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service EmbeddingService.
   */
  public static final class EmbeddingServiceStub
      extends io.grpc.stub.AbstractAsyncStub<EmbeddingServiceStub> {
    private EmbeddingServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceStub(channel, callOptions);
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
    public void getEmbedding(adiom.v1.Vector.GetEmbeddingRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Vector.GetEmbeddingResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetEmbeddingMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service EmbeddingService.
   */
  public static final class EmbeddingServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<EmbeddingServiceBlockingStub> {
    private EmbeddingServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public adiom.v1.Vector.GetSupportedDataTypesResponse getSupportedDataTypes(adiom.v1.Vector.GetSupportedDataTypesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSupportedDataTypesMethod(), getCallOptions(), request);
    }

    /**
     */
    public adiom.v1.Vector.GetEmbeddingResponse getEmbedding(adiom.v1.Vector.GetEmbeddingRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetEmbeddingMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service EmbeddingService.
   */
  public static final class EmbeddingServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<EmbeddingServiceFutureStub> {
    private EmbeddingServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected EmbeddingServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new EmbeddingServiceFutureStub(channel, callOptions);
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
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Vector.GetEmbeddingResponse> getEmbedding(
        adiom.v1.Vector.GetEmbeddingRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetEmbeddingMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_SUPPORTED_DATA_TYPES = 0;
  private static final int METHODID_GET_EMBEDDING = 1;

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
        case METHODID_GET_EMBEDDING:
          serviceImpl.getEmbedding((adiom.v1.Vector.GetEmbeddingRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Vector.GetEmbeddingResponse>) responseObserver);
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
          getGetEmbeddingMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Vector.GetEmbeddingRequest,
              adiom.v1.Vector.GetEmbeddingResponse>(
                service, METHODID_GET_EMBEDDING)))
        .build();
  }

  private static abstract class EmbeddingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    EmbeddingServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return adiom.v1.Vector.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("EmbeddingService");
    }
  }

  private static final class EmbeddingServiceFileDescriptorSupplier
      extends EmbeddingServiceBaseDescriptorSupplier {
    EmbeddingServiceFileDescriptorSupplier() {}
  }

  private static final class EmbeddingServiceMethodDescriptorSupplier
      extends EmbeddingServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    EmbeddingServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (EmbeddingServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new EmbeddingServiceFileDescriptorSupplier())
              .addMethod(getGetSupportedDataTypesMethod())
              .addMethod(getGetEmbeddingMethod())
              .build();
        }
      }
    }
    return result;
  }
}
