package dynamodb

import (
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

	update, err := streamRecordToUpdate(record, adiomv1.DataType_DATA_TYPE_MONGO_BSON, []string{"pk"}, NumberTypeString)
	require.NoError(t, err)
	require.Len(t, update.Id, 1)

	raw := bson.Raw(update.Data)
	require.Equal(t, bson.TypeString, raw.Lookup("_id").Type)
	require.Equal(t, "id1", raw.Lookup("_id").StringValue())
	require.Equal(t, bson.TypeString, raw.Lookup("count").Type)
	require.Equal(t, "42", raw.Lookup("count").StringValue())
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
	}, []string{"pk"}, NumberTypeInt32)
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
	}, []string{"pk"}, numberType)
	require.NoError(t, err)
	require.Len(t, items, 1)
	return bson.Raw(items[0])
}
