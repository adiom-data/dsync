package transform

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
)

// Using connect GRPC example
type mappingTransform struct {
}

// Hardcoded mappings for 1 GB dataset:
// orders {
//   customer {}
//   lineitems []
// }
// part {
//   partsupp {}
// }
// supplier {
//   partsupp {}
// 	 nation {
//     region {}
//   }
// }

// table_name | row_count
// ------------+-----------
//  customer   |    150000
//  lineitem   |   6001215
//  nation     |        25
//  orders     |   1500000
//  part       |    200000
//  partsupp   |    800000
//  region     |         5
//  supplier   |     10000

// GetTransform implements adiomv1connect.TransformServiceHandler.
func (m *mappingTransform) GetTransform(ctx context.Context, r *connect.Request[adiomv1.GetTransformRequest]) (*connect.Response[adiomv1.GetTransformResponse], error) {
	namespace := r.Msg.Namespace
	data := r.Msg.GetData()
	updates := r.Msg.GetUpdates()

	// process initial sync data
	if len(data) > 0 {
		slog.Debug(fmt.Sprintf("Initial sync, mapping transform: namespace %s to store", namespace))
		transform, err := GetInitialSyncTransform(ctx, namespace, data)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(transform), nil
	} else if len(updates) > 0 { // process change stream updates
		// handle updates
		return connect.NewResponse(&adiomv1.GetTransformResponse{
			Namespace: r.Msg.Namespace,
			Updates:   r.Msg.GetUpdates(),
			Data:      r.Msg.GetData(),
		}), nil

	} else {
		// pass through original request
		return connect.NewResponse(&adiomv1.GetTransformResponse{
			Namespace: r.Msg.Namespace,
			Updates:   r.Msg.GetUpdates(),
			Data:      r.Msg.GetData(),
		}), nil
	}
}

// GetTransformInfo implements adiomv1connect.TransformServiceHandler.
func (m *mappingTransform) GetTransformInfo(context.Context, *connect.Request[adiomv1.GetTransformInfoRequest]) (*connect.Response[adiomv1.GetTransformInfoResponse], error) {
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

func (m *mappingTransform) GetFanOutTransform(ctx context.Context, r *connect.Request[adiomv1.GetFanOutTransformRequest]) (*connect.Response[adiomv1.GetFanOutTransformResponse], error) {
	// For now, implement as a simple pass-through that puts the result in the original namespace
	// You can extend this to implement actual fan-out logic
	// nsData := &adiomv1.NamespaceTransformData{
	// 	Data:    r.Msg.GetData(),
	// 	Updates: r.Msg.GetUpdates(),
	// }
	// return connect.NewResponse(&adiomv1.GetFanOutTransformResponse{
	// 	Namespaces: map[string]*adiomv1.NamespaceTransformData{
	// 		r.Msg.Namespace: nsData,
	// 	},
	// }), nil

	namespace := r.Msg.Namespace

	data := r.Msg.GetData()
	updates := r.Msg.GetUpdates()

	nsData := &adiomv1.NamespaceTransformData{
		Data:    r.Msg.GetData(),
		Updates: r.Msg.GetUpdates(),
	}

	slog.Debug("Getting grpc transform for namespace: " + namespace)

	if len(data) > 0 {
		transform, err := GetFanOutTransformHelper(ctx, namespace, data, TPCHMappingConfig)
		if err != nil {
			return nil, err
		} else {
			return connect.NewResponse(transform), nil
		}
	} else if len(updates) > 0 {
		// handle updates
		return connect.NewResponse(&adiomv1.GetFanOutTransformResponse{
			Namespaces: map[string]*adiomv1.NamespaceTransformData{
				namespace: nsData,
			},
		}), nil
	} else {
		return connect.NewResponse(&adiomv1.GetFanOutTransformResponse{
			Namespaces: map[string]*adiomv1.NamespaceTransformData{
				namespace: nsData,
			},
		}), nil
	}

}

func NewMappingTransform() adiomv1connect.TransformServiceHandler {
	return &mappingTransform{}
}

// Base GRPC example
type mappingTransformGRPC struct {
	adiomv1.UnimplementedTransformServiceServer
}

// GetTransform implements adiomv1.TransformServiceServer.
func (m *mappingTransformGRPC) GetTransform(ctx context.Context, r *adiomv1.GetTransformRequest) (*adiomv1.GetTransformResponse, error) {
	// return &adiomv1.GetTransformResponse{
	// 	Namespace: r.Namespace,
	// 	Updates:   r.GetUpdates(),
	// 	Data:      r.GetData(),
	// }, nil

	namespace := r.Namespace

	data := r.GetData()
	updates := r.GetUpdates()

	slog.Debug("Getting grpc transform for namespace: " + namespace)

	if len(data) > 0 {
		transform, err := GetInitialSyncTransform(ctx, namespace, data)
		if err != nil {
			return nil, err
		} else {
			return transform, nil
		}
	} else if len(updates) > 0 {
		// handle updates
		return &adiomv1.GetTransformResponse{
			Namespace: r.Namespace,
			Updates:   r.GetUpdates(),
			Data:      r.GetData(),
		}, nil
	} else {
		return &adiomv1.GetTransformResponse{
			Namespace: r.Namespace,
			Updates:   r.GetUpdates(),
			Data:      r.GetData(),
		}, nil
	}
}

// GetTransformInfo implements adiomv1.TransformServiceServer.
func (m *mappingTransformGRPC) GetTransformInfo(context.Context, *adiomv1.GetTransformInfoRequest) (*adiomv1.GetTransformInfoResponse, error) {
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

func (m *mappingTransformGRPC) GetFanOutTransform(ctx context.Context, r *adiomv1.GetFanOutTransformRequest) (*adiomv1.GetFanOutTransformResponse, error) {
	// For now, implement as a simple pass-through that puts the result in the original namespace
	// You can extend this to implement actual fan-out logic

	namespace := r.Namespace

	data := r.GetData()
	updates := r.GetUpdates()

	slog.Debug("Getting grpc transform for namespace: " + namespace)

	if len(data) > 0 {
		transform, err := GetFanOutTransformHelper(ctx, namespace, data, TPCHMappingConfig)
		if err != nil {
			return nil, err
		} else {
			return transform, nil
		}
	} else if len(updates) > 0 {
		// handle updates
		nsData := &adiomv1.NamespaceTransformData{
			Data:    r.GetData(),
			Updates: r.GetUpdates(),
		}
		return &adiomv1.GetFanOutTransformResponse{
			Namespaces: map[string]*adiomv1.NamespaceTransformData{
				namespace: nsData,
			},
		}, nil
	} else {
		nsData := &adiomv1.NamespaceTransformData{
			Data:    r.GetData(),
			Updates: r.GetUpdates(),
		}
		return &adiomv1.GetFanOutTransformResponse{
			Namespaces: map[string]*adiomv1.NamespaceTransformData{
				namespace: nsData,
			},
		}, nil
	}

}

func NewMappingTransformGRPC() adiomv1.TransformServiceServer {
	return &mappingTransformGRPC{}
}

// This function maps namespace to proper MongoDB field names
func getArrayFieldName(namespace string) string {
	switch namespace {
	case "public.customer":
		return "customer"
	case "public.nation":
		return "nation"
	case "public.region":
		return "nation.region"
	case "public.lineitem":
		return "lineitems"
	case "public.partsupp":
		return "partsupp"
	default:
		return "items"
	}
}
