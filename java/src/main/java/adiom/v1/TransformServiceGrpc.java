package adiom.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.69.0)",
    comments = "Source: adiom/v1/adiom.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TransformServiceGrpc {

  private TransformServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "adiom.v1.TransformService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.GetTransformInfoRequest,
      adiom.v1.Messages.GetTransformInfoResponse> getGetTransformInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTransformInfo",
      requestType = adiom.v1.Messages.GetTransformInfoRequest.class,
      responseType = adiom.v1.Messages.GetTransformInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.GetTransformInfoRequest,
      adiom.v1.Messages.GetTransformInfoResponse> getGetTransformInfoMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.GetTransformInfoRequest, adiom.v1.Messages.GetTransformInfoResponse> getGetTransformInfoMethod;
    if ((getGetTransformInfoMethod = TransformServiceGrpc.getGetTransformInfoMethod) == null) {
      synchronized (TransformServiceGrpc.class) {
        if ((getGetTransformInfoMethod = TransformServiceGrpc.getGetTransformInfoMethod) == null) {
          TransformServiceGrpc.getGetTransformInfoMethod = getGetTransformInfoMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.GetTransformInfoRequest, adiom.v1.Messages.GetTransformInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTransformInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetTransformInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetTransformInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TransformServiceMethodDescriptorSupplier("GetTransformInfo"))
              .build();
        }
      }
    }
    return getGetTransformInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<adiom.v1.Messages.GetTransformRequest,
      adiom.v1.Messages.GetTransformResponse> getGetTransformMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTransform",
      requestType = adiom.v1.Messages.GetTransformRequest.class,
      responseType = adiom.v1.Messages.GetTransformResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<adiom.v1.Messages.GetTransformRequest,
      adiom.v1.Messages.GetTransformResponse> getGetTransformMethod() {
    io.grpc.MethodDescriptor<adiom.v1.Messages.GetTransformRequest, adiom.v1.Messages.GetTransformResponse> getGetTransformMethod;
    if ((getGetTransformMethod = TransformServiceGrpc.getGetTransformMethod) == null) {
      synchronized (TransformServiceGrpc.class) {
        if ((getGetTransformMethod = TransformServiceGrpc.getGetTransformMethod) == null) {
          TransformServiceGrpc.getGetTransformMethod = getGetTransformMethod =
              io.grpc.MethodDescriptor.<adiom.v1.Messages.GetTransformRequest, adiom.v1.Messages.GetTransformResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTransform"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetTransformRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  adiom.v1.Messages.GetTransformResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TransformServiceMethodDescriptorSupplier("GetTransform"))
              .build();
        }
      }
    }
    return getGetTransformMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TransformServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TransformServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TransformServiceStub>() {
        @java.lang.Override
        public TransformServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TransformServiceStub(channel, callOptions);
        }
      };
    return TransformServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TransformServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TransformServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TransformServiceBlockingStub>() {
        @java.lang.Override
        public TransformServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TransformServiceBlockingStub(channel, callOptions);
        }
      };
    return TransformServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TransformServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TransformServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TransformServiceFutureStub>() {
        @java.lang.Override
        public TransformServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TransformServiceFutureStub(channel, callOptions);
        }
      };
    return TransformServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void getTransformInfo(adiom.v1.Messages.GetTransformInfoRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetTransformInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTransformInfoMethod(), responseObserver);
    }

    /**
     */
    default void getTransform(adiom.v1.Messages.GetTransformRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetTransformResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTransformMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service TransformService.
   */
  public static abstract class TransformServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return TransformServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service TransformService.
   */
  public static final class TransformServiceStub
      extends io.grpc.stub.AbstractAsyncStub<TransformServiceStub> {
    private TransformServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TransformServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TransformServiceStub(channel, callOptions);
    }

    /**
     */
    public void getTransformInfo(adiom.v1.Messages.GetTransformInfoRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetTransformInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTransformInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTransform(adiom.v1.Messages.GetTransformRequest request,
        io.grpc.stub.StreamObserver<adiom.v1.Messages.GetTransformResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTransformMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service TransformService.
   */
  public static final class TransformServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<TransformServiceBlockingStub> {
    private TransformServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TransformServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TransformServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public adiom.v1.Messages.GetTransformInfoResponse getTransformInfo(adiom.v1.Messages.GetTransformInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTransformInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public adiom.v1.Messages.GetTransformResponse getTransform(adiom.v1.Messages.GetTransformRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTransformMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service TransformService.
   */
  public static final class TransformServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<TransformServiceFutureStub> {
    private TransformServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TransformServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TransformServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.GetTransformInfoResponse> getTransformInfo(
        adiom.v1.Messages.GetTransformInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTransformInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<adiom.v1.Messages.GetTransformResponse> getTransform(
        adiom.v1.Messages.GetTransformRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTransformMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_TRANSFORM_INFO = 0;
  private static final int METHODID_GET_TRANSFORM = 1;

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
        case METHODID_GET_TRANSFORM_INFO:
          serviceImpl.getTransformInfo((adiom.v1.Messages.GetTransformInfoRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.GetTransformInfoResponse>) responseObserver);
          break;
        case METHODID_GET_TRANSFORM:
          serviceImpl.getTransform((adiom.v1.Messages.GetTransformRequest) request,
              (io.grpc.stub.StreamObserver<adiom.v1.Messages.GetTransformResponse>) responseObserver);
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
          getGetTransformInfoMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.GetTransformInfoRequest,
              adiom.v1.Messages.GetTransformInfoResponse>(
                service, METHODID_GET_TRANSFORM_INFO)))
        .addMethod(
          getGetTransformMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              adiom.v1.Messages.GetTransformRequest,
              adiom.v1.Messages.GetTransformResponse>(
                service, METHODID_GET_TRANSFORM)))
        .build();
  }

  private static abstract class TransformServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TransformServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return adiom.v1.Adiom.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TransformService");
    }
  }

  private static final class TransformServiceFileDescriptorSupplier
      extends TransformServiceBaseDescriptorSupplier {
    TransformServiceFileDescriptorSupplier() {}
  }

  private static final class TransformServiceMethodDescriptorSupplier
      extends TransformServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    TransformServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (TransformServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TransformServiceFileDescriptorSupplier())
              .addMethod(getGetTransformInfoMethod())
              .addMethod(getGetTransformMethod())
              .build();
        }
      }
    }
    return result;
  }
}
