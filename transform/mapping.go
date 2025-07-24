package transform

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
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

// helper function to convert base table inserts to update type apply mutations
func insertAsUpdate(data []byte) (*adiomv1.Update, error) {

	var doc bson.M
	err := bson.Unmarshal(data, &doc)
	if err != nil {
		return nil, err
	}
	id, ok := doc["_id"]
	if !ok {
		return nil, fmt.Errorf("no id field")
	}

	// set all fields of original document for the update, filter by id, upsert set to True on connector WriteUpdates fn
	updateMessage := bson.M{
		"filter": bson.M{
			"_id": id,
		},
		"update": bson.M{
			"$set": doc,
		},
		"upsert": true, // upsert to create the document if it doesn't exist
	}

	marshalled, err := bson.Marshal(updateMessage)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	typ, d, err := bson.MarshalValue(id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	keys := []*adiomv1.BsonValue{{
		Name: "_id",
		Data: d,
		Type: uint32(typ),
	}}
	update := &adiomv1.Update{
		Id:   keys,
		Type: adiomv1.UpdateType_UPDATE_TYPE_APPLY, // New update type
		Data: marshalled,
	}
	return update, nil

}

func embeddedDocumentUpdate(namespace string, data []byte, foreignKey string) (*adiomv1.Update, error) {
	// convert embedded document to update type apply mutation
	var doc bson.M
	err := bson.Unmarshal(data, &doc)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	id, ok := doc["_id"]
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no id field"))
	}

	arrayFieldName := getArrayFieldName(namespace)
	//foreignKey := fmt.Sprintf("%s_id", arrayFieldName)

	// filter by base table id, check if array field contains the document by the id field, if not, push to array
	updateOp := bson.M{
		"filter": bson.M{
			foreignKey: id,
		},
		"update": bson.M{
			"$set": bson.M{
				arrayFieldName: doc,
			},
		},
		"upsert": false,
	}

	marshalled, err := bson.Marshal(updateOp)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	typ, d, err := bson.MarshalValue(id)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	keys := []*adiomv1.BsonValue{{
		Name: "_id",
		Data: d,
		Type: uint32(typ),
	}}

	update := &adiomv1.Update{
		Id:   keys,
		Type: adiomv1.UpdateType_UPDATE_TYPE_APPLY,
		Data: marshalled,
	}
	return update, nil
}

func embeddedArrayUpdate(namespace string, data []byte, foreignKey string) (*adiomv1.Update, error) {
	var doc bson.M
	err := bson.Unmarshal(data, &doc)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	id, ok := doc["_id"]
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no primary key mapping"))
	}
	fk_id, ok := doc[foreignKey]
	if !ok || fk_id == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no foreign key mapping for namespace %s", namespace))
	}

	slog.Debug(fmt.Sprintf("Mapping transform: namespace %s, id %v, store_id %v", namespace, id, fk_id))

	arrayFieldName := getArrayFieldName(namespace)

	// filter by base table id, check if array field contains the document by the id field, if not, push to array
	findAndModifyOp := bson.M{
		"filter": bson.M{
			"_id": fk_id,
			arrayFieldName: bson.M{
				"$not": bson.M{
					"$elemMatch": bson.M{
						"_id": id,
					},
				},
			},
		},
		"update": bson.M{
			"$push": bson.M{
				arrayFieldName: doc,
			},
		},
		"upsert": true, // upsert to create the array if it doesn't exist
	}
	// test idempotency: insert specific document multiple times, hardcoded

	marshalled, err := bson.Marshal(findAndModifyOp)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	// // Create the primary key for the update
	// keys, err := bson.Marshal(bson.M{"_id": fk_id})
	// if err != nil {
	//     return nil, connect.NewError(connect.CodeInternal, err)
	// }

	// Create UpdateTypeApply update
	typ, val, err := bson.MarshalValue(fk_id)
	if err != nil {
		return nil, err
	}
	keys := []*adiomv1.BsonValue{{
		Name: "_id",
		Data: val,
		Type: uint32(typ),
	}}

	update := &adiomv1.Update{
		Id:   keys,
		Type: adiomv1.UpdateType_UPDATE_TYPE_APPLY, // New update type
		Data: marshalled,
	}
	return update, nil
}

func GetInitialSyncTransform(_ context.Context, namespace string, data [][]byte) (*adiomv1.GetTransformResponse, error) {
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
	var updates []*adiomv1.Update
	var dstNamespace string
	for _, d := range data {
		var update *adiomv1.Update
		var err error

		switch namespace {
		case "public.customer":
			dstNamespace = "public.orders"
			update, err = embeddedDocumentUpdate(namespace, d, "o_custkey")
			slog.Debug(fmt.Sprintf("Mapping transform: namespace %s, update: %v", namespace, update))
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		case "public.orders", "public.part", "public.supplier":
			dstNamespace = namespace
			update, err = insertAsUpdate(d)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

		case "public.nation":
			dstNamespace = "public.supplier"
			update, err = embeddedDocumentUpdate(namespace, d, "s_nationkey")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		case "public.region":
			dstNamespace = "public.supplier"
			update, err = embeddedDocumentUpdate(namespace, d, "nation.n_regionkey")
		case "public.lineitem":
			dstNamespace = "public.orders"
			update, err = embeddedArrayUpdate(namespace, d, "l_orderkey")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

		case "public.partsupp":
			//findAndModify, add to part and suppliers
			dstNamespace = "public.part"
			update, err = embeddedArrayUpdate(namespace, d, "ps_partkey")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

		}

		updates = append(updates, update)
		slog.Debug(fmt.Sprintf("len of updates for namespace %s: %d", namespace, len(updates)))

	}

	return &adiomv1.GetTransformResponse{
		Namespace: dstNamespace,
		Updates:   updates,
		Data:      nil,
	}, nil
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
