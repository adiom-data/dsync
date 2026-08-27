package dynamodb

import (
	"encoding/json"
	"testing"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestStreamRecordToUpdateMongoBSONSetsDocumentIDValue(t *testing.T) {
	record := streamtypes.Record{
		EventName: streamtypes.OperationTypeModify,
		Dynamodb: &streamtypes.StreamRecord{
			Keys: map[string]streamtypes.AttributeValue{
				"pk": &streamtypes.AttributeValueMemberS{Value: "id1"},
			},
			NewImage: map[string]streamtypes.AttributeValue{
				"pk":    &streamtypes.AttributeValueMemberS{Value: "id1"},
				"count": &streamtypes.AttributeValueMemberN{Value: "42"},
			},
		},
	}

	update, err := streamRecordToUpdate(record, adiomv1.DataType_DATA_TYPE_MONGO_BSON, []string{"pk"}, NumberTypeString, BsonIDFormatBinary)
	require.NoError(t, err)
	require.Len(t, update.Id, 1)

	raw := bson.Raw(update.Data)
	require.Equal(t, bson.TypeString, raw.Lookup("_id").Type)
	require.Equal(t, "id1", raw.Lookup("_id").StringValue())
	require.Equal(t, bson.TypeString, raw.Lookup("count").Type)
	require.Equal(t, "42", raw.Lookup("count").StringValue())
}

func TestItemsToBsonEmptyCollectionsStayArraysAndMaps(t *testing.T) {
	items, err := itemsToBson([]map[string]types.AttributeValue{
		{
			"pk":       &types.AttributeValueMemberS{Value: "id1"},
			"list":     &types.AttributeValueMemberL{Value: []types.AttributeValue{}},
			"strings":  &types.AttributeValueMemberSS{Value: []string{}},
			"numbers":  &types.AttributeValueMemberNS{Value: []string{}},
			"binaries": &types.AttributeValueMemberBS{Value: [][]byte{}},
			"map":      &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}},
		},
	}, []string{"pk"}, NumberTypeString, BsonIDFormatBinary)
	require.NoError(t, err)

	raw := bson.Raw(items[0])
	for _, field := range []string{"list", "strings", "numbers", "binaries"} {
		require.Equal(t, bson.TypeArray, raw.Lookup(field).Type, field)
		values, err := raw.Lookup(field).Array().Values()
		require.NoError(t, err, field)
		require.Empty(t, values, field)
	}
	require.Equal(t, bson.TypeEmbeddedDocument, raw.Lookup("map").Type)
	elements, err := raw.Lookup("map").Document().Elements()
	require.NoError(t, err)
	require.Empty(t, elements)
}

func TestItemsToJsonNumberTypeNumberPreservesNumericText(t *testing.T) {
	items, err := itemsToJson([]map[string]types.AttributeValue{
		{
			"pk":  &types.AttributeValueMemberS{Value: "id1"},
			"big": &types.AttributeValueMemberN{Value: "9007199254740993"},
			"set": &types.AttributeValueMemberNS{Value: []string{"9007199254740993"}},
		},
	}, []string{"pk"}, NumberTypeNumber)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(items[0], &raw))
	require.JSONEq(t, `9007199254740993`, string(raw["big"]))
	require.JSONEq(t, `[9007199254740993]`, string(raw["set"]))
}

func TestItemsToJsonNumberTypeStringKeepsAWSDefaultFloat64Behavior(t *testing.T) {
	items, err := itemsToJson([]map[string]types.AttributeValue{
		{
			"pk":  &types.AttributeValueMemberS{Value: "id1"},
			"num": &types.AttributeValueMemberN{Value: "42"},
			"set": &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
		},
	}, []string{"pk"}, NumberTypeString)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(items[0], &raw))
	require.JSONEq(t, `42`, string(raw["num"]))
	require.JSONEq(t, `[1,2]`, string(raw["set"]))
}

func TestItemsToJsonNumberTypeNumberKeepsEmptyCollections(t *testing.T) {
	items, err := itemsToJson([]map[string]types.AttributeValue{
		{
			"pk":    &types.AttributeValueMemberS{Value: "id1"},
			"list":  &types.AttributeValueMemberL{Value: []types.AttributeValue{}},
			"map":   &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{}},
			"nums":  &types.AttributeValueMemberNS{Value: []string{}},
			"strs":  &types.AttributeValueMemberSS{Value: []string{}},
			"bytes": &types.AttributeValueMemberBS{Value: [][]byte{}},
		},
	}, []string{"pk"}, NumberTypeNumber)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(items[0], &raw))
	for _, field := range []string{"list", "nums", "strs", "bytes"} {
		require.JSONEq(t, `[]`, string(raw[field]), field)
	}
	require.JSONEq(t, `{}`, string(raw["map"]))
}

func TestItemsToBsonCompositeIDFormatUsesJSONStyleDocumentID(t *testing.T) {
	items, err := itemsToBson([]map[string]types.AttributeValue{
		{
			"pk": &types.AttributeValueMemberS{Value: "a"},
			"sk": &types.AttributeValueMemberN{Value: "7"},
		},
	}, []string{"pk", "sk"}, NumberTypeString, BsonIDFormatComposite)
	require.NoError(t, err)

	raw := bson.Raw(items[0])
	require.Equal(t, bson.TypeString, raw.Lookup("_id").Type)
	require.Equal(t, "a-7", raw.Lookup("_id").StringValue())
}

func TestStreamRecordToUpdateCompositeBSONIDFormatUsesMultipleIDParts(t *testing.T) {
	record := streamtypes.Record{
		EventName: streamtypes.OperationTypeModify,
		Dynamodb: &streamtypes.StreamRecord{
			Keys: map[string]streamtypes.AttributeValue{
				"pk": &streamtypes.AttributeValueMemberS{Value: "a"},
				"sk": &streamtypes.AttributeValueMemberN{Value: "7"},
			},
			NewImage: map[string]streamtypes.AttributeValue{
				"pk": &streamtypes.AttributeValueMemberS{Value: "a"},
				"sk": &streamtypes.AttributeValueMemberN{Value: "7"},
			},
		},
	}

	update, err := streamRecordToUpdate(record, adiomv1.DataType_DATA_TYPE_MONGO_BSON, []string{"pk", "sk"}, NumberTypeString, BsonIDFormatComposite)
	require.NoError(t, err)
	require.Len(t, update.Id, 3)
	require.Equal(t, "pk", update.Id[0].Name)
	require.Equal(t, "sk", update.Id[1].Name)
	require.Equal(t, "_id", update.Id[2].Name)

	raw := bson.Raw(update.Data)
	require.Equal(t, "a-7", raw.Lookup("_id").StringValue())
}

func TestStreamRecordToUpdateCompositeBSONIDFormatUsesSingleIDPartForSingleKey(t *testing.T) {
	record := streamtypes.Record{
		EventName: streamtypes.OperationTypeModify,
		Dynamodb: &streamtypes.StreamRecord{
			Keys: map[string]streamtypes.AttributeValue{
				"pk": &streamtypes.AttributeValueMemberS{Value: "a"},
			},
			NewImage: map[string]streamtypes.AttributeValue{
				"pk": &streamtypes.AttributeValueMemberS{Value: "a"},
			},
		},
	}

	update, err := streamRecordToUpdate(record, adiomv1.DataType_DATA_TYPE_MONGO_BSON, []string{"pk"}, NumberTypeString, BsonIDFormatComposite)
	require.NoError(t, err)
	require.Len(t, update.Id, 1)
	require.Equal(t, "_id", update.Id[0].Name)

	raw := bson.Raw(update.Data)
	require.Equal(t, "a", raw.Lookup("_id").StringValue())
}

func TestItemsToBsonNumberTypeString(t *testing.T) {
	raw := marshalDynamoItem(t, NumberTypeString)

	require.Equal(t, bson.TypeString, raw.Lookup("_id").Type)
	require.Equal(t, "7", raw.Lookup("_id").StringValue())
	require.Equal(t, bson.TypeString, raw.Lookup("count").Type)
	require.Equal(t, "42", raw.Lookup("count").StringValue())
	require.Equal(t, bson.TypeArray, raw.Lookup("nums").Type)
	require.Equal(t, "1", raw.Lookup("nums").Array().Index(0).StringValue())
}

func TestItemsToBsonNumberTypeInt64(t *testing.T) {
	raw := marshalDynamoItem(t, NumberTypeInt64)

	require.Equal(t, bson.TypeInt64, raw.Lookup("_id").Type)
	require.Equal(t, int64(7), raw.Lookup("_id").Int64())
	require.Equal(t, bson.TypeInt64, raw.Lookup("count").Type)
	require.Equal(t, int64(42), raw.Lookup("count").Int64())
	require.Equal(t, bson.TypeInt64, raw.Lookup("nums").Array().Index(0).Type)
	require.Equal(t, int64(1), raw.Lookup("nums").Array().Index(0).Int64())
}

func TestItemsToBsonNumberTypeInt32(t *testing.T) {
	raw := marshalDynamoItem(t, NumberTypeInt32)

	require.Equal(t, bson.TypeInt32, raw.Lookup("_id").Type)
	require.Equal(t, int32(7), raw.Lookup("_id").Int32())
	require.Equal(t, bson.TypeInt32, raw.Lookup("count").Type)
	require.Equal(t, int32(42), raw.Lookup("count").Int32())
	require.Equal(t, bson.TypeInt32, raw.Lookup("nums").Array().Index(0).Type)
	require.Equal(t, int32(1), raw.Lookup("nums").Array().Index(0).Int32())
}

func TestItemsToBsonNumberTypeInt32RejectsOutOfRange(t *testing.T) {
	_, err := itemsToBson([]map[string]types.AttributeValue{
		{
			"pk": &types.AttributeValueMemberN{Value: "2147483648"},
		},
	}, []string{"pk"}, NumberTypeInt32, BsonIDFormatBinary)
	require.Error(t, err)
}

func marshalDynamoItem(t *testing.T, numberType NumberType) bson.Raw {
	t.Helper()
	items, err := itemsToBson([]map[string]types.AttributeValue{
		{
			"pk":    &types.AttributeValueMemberN{Value: "7"},
			"count": &types.AttributeValueMemberN{Value: "42"},
			"nums":  &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
		},
	}, []string{"pk"}, numberType, BsonIDFormatBinary)
	require.NoError(t, err)
	require.Len(t, items, 1)
	return bson.Raw(items[0])
}
