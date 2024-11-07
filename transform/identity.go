package transform

import (
	"context"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
)

// Using connect GRPC example
type identityTransform struct {
}

// GetTransform implements adiomv1connect.TransformServiceHandler.
func (i *identityTransform) GetTransform(_ context.Context, r *connect.Request[adiomv1.GetTransformRequest]) (*connect.Response[adiomv1.GetTransformResponse], error) {
	return connect.NewResponse(&adiomv1.GetTransformResponse{
		Namespace: r.Msg.Namespace,
		Updates:   r.Msg.GetUpdates(),
		Data:      r.Msg.GetData(),
	}), nil
}

// GetTransformInfo implements adiomv1connect.TransformServiceHandler.
func (i *identityTransform) GetTransformInfo(context.Context, *connect.Request[adiomv1.GetTransformInfoRequest]) (*connect.Response[adiomv1.GetTransformInfoResponse], error) {
	var infos []*adiomv1.GetTransformInfoResponse_TransformInfo
	for _, v := range adiomv1.DataType_value {
		infos = append(infos, &adiomv1.GetTransformInfoResponse_TransformInfo{
			RequestType:   adiomv1.DataType(v),
			ResponseTypes: []adiomv1.DataType{adiomv1.DataType(v)},
		})
	}
	return connect.NewResponse(&adiomv1.GetTransformInfoResponse{
		Transforms: infos,
	}), nil
}

func NewIdentityTransform() adiomv1connect.TransformServiceHandler {
	return &identityTransform{}
}

// Base GRPC example
type identityTransformGRPC struct {
	adiomv1.UnimplementedTransformServiceServer
}

// GetTransform implements adiomv1.TransformServiceServer.
func (i *identityTransformGRPC) GetTransform(_ context.Context, r *adiomv1.GetTransformRequest) (*adiomv1.GetTransformResponse, error) {
	return &adiomv1.GetTransformResponse{
		Namespace: r.Namespace,
		Updates:   r.GetUpdates(),
		Data:      r.GetData(),
	}, nil
}

// GetTransformInfo implements adiomv1.TransformServiceServer.
func (i *identityTransformGRPC) GetTransformInfo(context.Context, *adiomv1.GetTransformInfoRequest) (*adiomv1.GetTransformInfoResponse, error) {
	var infos []*adiomv1.GetTransformInfoResponse_TransformInfo
	for _, v := range adiomv1.DataType_value {
		infos = append(infos, &adiomv1.GetTransformInfoResponse_TransformInfo{
			RequestType:   adiomv1.DataType(v),
			ResponseTypes: []adiomv1.DataType{adiomv1.DataType(v)},
		})
	}
	return &adiomv1.GetTransformInfoResponse{
		Transforms: infos,
	}, nil
}

func NewIdentityTransformGRPC() adiomv1.TransformServiceServer {
	return &identityTransformGRPC{}
}
