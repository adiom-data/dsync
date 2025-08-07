package transform

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/bson"
)

// helper function to convert base table inserts to update type apply mutations
func InsertAsUpdate(doc bson.M) (*adiomv1.Update, error) {
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

// helper function for embedded document updates, embed document into the base table if foreign key mapping exists
func embeddedDocumentUpdate(fieldName string, doc bson.M, foreignKey string) (*adiomv1.Update, error) {
	// convert embedded document to update type apply mutation
	id, ok := doc["_id"]
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no id field"))
	}

	//foreignKey := fmt.Sprintf("%s_id", arrayFieldName)

	// filter by base table id, check if array field contains the document by the id field, if not, push to array
	updateOp := bson.M{
		"filter": bson.M{
			foreignKey: id,
		},
		"update": bson.M{
			"$set": bson.M{
				fieldName: doc,
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

// helper function for embedded array updates, embed document into the base table if foreign key mapping exists
func embeddedArrayUpdate(srcNamespace string, fieldName string, doc bson.M, foreignKey string) (*adiomv1.Update, error) {
	id, ok := doc["_id"]
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no primary key mapping"))
	}
	fk_id, ok := doc[foreignKey]
	if !ok || fk_id == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no foreign key mapping for namespace %s", srcNamespace))
	}

	slog.Debug(fmt.Sprintf("Mapping transform: namespace %s, id %v, store_id %v", srcNamespace, id, fk_id))

	// filter by base table id, check if array field contains the document by the id field, if not, push to array
	findAndModifyOp := bson.M{
		"filter": bson.M{
			"_id": fk_id,
			fieldName: bson.M{
				"$not": bson.M{
					"$elemMatch": bson.M{
						"_id": id,
					},
				},
			},
		},
		"update": bson.M{
			"$push": bson.M{
				fieldName: doc,
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

// helper function for many to many embedded array updates, embed document into the base table if foreign key mapping exists
func embeddedArrayOfIdsUpdate(srcNamespace string, doc bson.M, primaryKey string, foreignKey string, arrayName string) (*adiomv1.Update, error) {
	id, ok := doc[primaryKey]
	if !ok {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("no primary key mapping"))
	}
	fk_id, ok := doc[foreignKey]
	if !ok || fk_id == nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no foreign key mapping for namespace %s", srcNamespace))
	}

	slog.Debug(fmt.Sprintf("Mapping transform: namespace %s, id %v, store_id %v", srcNamespace, id, fk_id))

	// filter by base table id, check if array field contains the document by the id field, if not, push to array
	findAndModifyOp := bson.M{
		"filter": bson.M{
			"_id": fk_id,
			arrayName: bson.M{
				"$not": bson.M{
					"$elemMatch": bson.M{
						"_id": id,
					},
				},
			},
		},
		"update": bson.M{
			"$push": bson.M{
				arrayName: bson.M{
					"_id": id, // Assuming the array contains objects with an _id field
				},
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

// helper function to get TransformResponse for initial sync, hardcoded mappings for tpch dataset
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
		var doc bson.M
		var err error
		err = bson.Unmarshal(d, &doc)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		var update *adiomv1.Update

		switch namespace {
		case "public.customer":
			dstNamespace = "public.orders"
			update, err = embeddedDocumentUpdate("customer", doc, "o_custkey")
			slog.Debug(fmt.Sprintf("Mapping transform: namespace %s, update: %v", namespace, update))
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		case "public.orders", "public.part", "public.supplier":
			dstNamespace = namespace
			update, err = InsertAsUpdate(doc)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

		case "public.nation":
			dstNamespace = "public.supplier"
			update, err = embeddedDocumentUpdate("nation", doc, "s_nationkey")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		case "public.region":
			dstNamespace = "public.supplier"
			update, err = embeddedDocumentUpdate("nation.region", doc, "nation.n_regionkey")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		case "public.lineitem":
			dstNamespace = "public.orders"
			update, err = embeddedArrayUpdate(namespace, "lineitems", doc, "l_orderkey")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

		case "public.partsupp":
			//findAndModify, add to part and suppliers
			dstNamespace = "public.part"
			update, err = embeddedArrayUpdate(namespace, "partsupp", doc, "ps_partkey")
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

type MappingRule struct {
	TargetNamespace string
	SourceNamespace string
	ForeignKey      string
	PrimaryKey      string
	FieldName       string
	UpdateType      string // "insert", "embeddedDoc", "embeddedArray", "embeddedArrayOfIds"
}

type NamespaceMappingConfig map[string][]MappingRule

func GetFanOutTransformHelper(ctx context.Context, namespace string, data [][]byte, config NamespaceMappingConfig) (*adiomv1.GetFanOutTransformResponse, error) {
	mapNamespaces := make(map[string]*adiomv1.NamespaceTransformData)

	rules, ok := config[namespace]
	if !ok {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no mapping rules found for namespace %s", namespace))
	}

	for _, d := range data {
		var doc bson.M
		err := bson.Unmarshal(d, &doc)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}

		for _, rule := range rules {
			update, err := applyMappingRule(rule, doc)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			if _, ok := mapNamespaces[rule.TargetNamespace]; !ok {
				mapNamespaces[rule.TargetNamespace] = &adiomv1.NamespaceTransformData{
					Updates: []*adiomv1.Update{},
					Data:    nil,
				}
			}
			mapNamespaces[rule.TargetNamespace].Updates = append(mapNamespaces[rule.TargetNamespace].Updates, update)

		}
	}

	return &adiomv1.GetFanOutTransformResponse{
		Namespaces: mapNamespaces,
	}, nil
}

func applyMappingRule(rule MappingRule, doc bson.M) (*adiomv1.Update, error) {
	var update *adiomv1.Update
	var err error

	switch rule.UpdateType {
	case "insert":
		update, err = InsertAsUpdate(doc)
	case "embeddedDoc":
		update, err = embeddedDocumentUpdate(rule.FieldName, doc, rule.ForeignKey)
	case "embeddedArray":
		update, err = embeddedArrayUpdate(rule.SourceNamespace, rule.FieldName, doc, rule.ForeignKey)
	case "embeddedArrayOfIds":
		update, err = embeddedArrayOfIdsUpdate(rule.SourceNamespace, doc, rule.PrimaryKey, rule.ForeignKey, rule.FieldName)
	default:
		return nil, fmt.Errorf("unknown update type: %s", rule.UpdateType)
	}

	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return update, nil
}

// tpch mapping config
var TPCHMappingConfig = NamespaceMappingConfig{
	"public.customer": {
		{
			TargetNamespace: "public.orders",
			SourceNamespace: "public.customer",
			ForeignKey:      "o_custkey",
			PrimaryKey:      "_id",
			FieldName:       "customer",
			UpdateType:      "embeddedDoc",
		},
	},
	"public.orders": {
		{
			TargetNamespace: "public.orders",
			SourceNamespace: "public.orders",
			PrimaryKey:      "_id",
			UpdateType:      "insert",
		},
	},
	"public.part": {
		{
			TargetNamespace: "public.part",
			SourceNamespace: "public.part",
			PrimaryKey:      "_id",
			UpdateType:      "insert",
		},
	},
	"public.supplier": {
		{
			TargetNamespace: "public.supplier",
			SourceNamespace: "public.supplier",
			PrimaryKey:      "_id",
			UpdateType:      "insert",
		},
	},
	"public.nation": {
		{
			TargetNamespace: "public.supplier",
			SourceNamespace: "public.nation",
			ForeignKey:      "s_nationkey",
			PrimaryKey:      "_id",
			FieldName:       "nation",
			UpdateType:      "embeddedDoc",
		},
	},
	"public.region": {
		{
			TargetNamespace: "public.supplier",
			SourceNamespace: "public.region",
			ForeignKey:      "nation.n_regionkey",
			PrimaryKey:      "_id",
			FieldName:       "nation.region",
			UpdateType:      "embeddedDoc",
		},
	},
	"public.lineitem": {
		{
			TargetNamespace: "public.orders",
			SourceNamespace: "public.lineitem",
			ForeignKey:      "l_orderkey",
			PrimaryKey:      "_id",
			FieldName:       "lineitems",
			UpdateType:      "embeddedArray",
		},
	},
	"public.partsupp": {
		{
			TargetNamespace: "public.part",
			SourceNamespace: "public.partsupp",
			ForeignKey:      "ps_partkey",
			PrimaryKey:      "ps_suppkey",
			FieldName:       "suppliers",
			UpdateType:      "embeddedArrayOfIds",
		},
		{
			TargetNamespace: "public.supplier",
			SourceNamespace: "public.partsupp",
			ForeignKey:      "ps_suppkey",
			PrimaryKey:      "ps_partkey",
			FieldName:       "parts",
			UpdateType:      "embeddedArrayOfIds",
		},
	},
}
