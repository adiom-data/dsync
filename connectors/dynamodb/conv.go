package dynamodb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// TODO: this is an arbitrary mapping right now
func fromBson(bs interface{}) (types.AttributeValue, error) {
	switch b := bs.(type) {
	case bson.A:
		var arr []types.AttributeValue
		for _, v := range b {
			vv, err := fromBson(v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, vv)
		}
		return &types.AttributeValueMemberL{Value: arr}, nil
	case bson.D:
		m := map[string]types.AttributeValue{}
		for _, v := range b {
			vv, err := fromBson(v.Value)
			if err != nil {
				return nil, err
			}
			m[v.Key] = vv
		}
		return &types.AttributeValueMemberM{Value: m}, nil
	case bson.M:
		m := map[string]types.AttributeValue{}
		for k, v := range b {
			vv, err := fromBson(v)
			if err != nil {
				return nil, err
			}
			m[k] = vv
		}
		return &types.AttributeValueMemberM{Value: m}, nil
	case bool:
		return &types.AttributeValueMemberBOOL{Value: b}, nil
	case int32:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(b), 10)}, nil
	case int64:
		return &types.AttributeValueMemberN{Value: strconv.FormatInt(b, 10)}, nil
	case float64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%f", b)}, nil
	case string:
		return &types.AttributeValueMemberS{Value: b}, nil
	case primitive.DateTime:
		return &types.AttributeValueMemberS{Value: b.Time().Format(time.RFC3339)}, nil
	case primitive.ObjectID:
		return &types.AttributeValueMemberS{Value: b.Hex()}, nil
	case primitive.Binary:
		return &types.AttributeValueMemberB{Value: b.Data}, nil
	case primitive.Decimal128:
		return &types.AttributeValueMemberN{Value: b.String()}, nil
	default:
		return &types.AttributeValueMemberS{Value: "XUnsupportedX"}, nil
	}
}

func itemFromBson(item []byte) (map[string]types.AttributeValue, error) {
	var iface interface{}
	err := bson.Unmarshal(item, &iface)
	if err != nil {
		return nil, err
	}
	maybeMap, err := fromBson(iface)
	if err != nil {
		return nil, err
	}
	m, ok := maybeMap.(*types.AttributeValueMemberM)
	if !ok {
		return nil, errors.New("not the correct format")
	}
	return m.Value, nil
}

func toBson(av types.AttributeValue) (interface{}, error) {
	switch tv := av.(type) {
	case *types.AttributeValueMemberB:
		return primitive.Binary{
			Subtype: bson.TypeBinaryGeneric,
			Data:    tv.Value,
		}, nil

	case *types.AttributeValueMemberBOOL:
		return tv.Value, nil

	case *types.AttributeValueMemberBS:
		var arr bson.A
		for _, v := range tv.Value {
			arr = append(arr, primitive.Binary{
				Subtype: bson.TypeBinaryGeneric,
				Data:    v,
			}, nil)
		}
		return arr, nil

	case *types.AttributeValueMemberL:
		var arr bson.A
		for _, v := range tv.Value {
			entry, err := toBson(v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, entry)
		}
		return arr, nil

	case *types.AttributeValueMemberM:
		m := bson.M{}
		for k, v := range tv.Value {
			entry, err := toBson(v)
			if err != nil {
				return nil, err
			}
			m[k] = entry
		}
		return m, nil

	case *types.AttributeValueMemberN:
		// TODO: Should we convert to an actual number type?
		return tv.Value, nil

	case *types.AttributeValueMemberNS:
		// TODO: Should we convert to an actual number type?
		var arr bson.A
		for _, v := range tv.Value {
			arr = append(arr, v, nil)
		}
		return arr, nil

	case *types.AttributeValueMemberS:
		return tv.Value, nil

	case *types.AttributeValueMemberSS:
		var arr bson.A
		for _, v := range tv.Value {
			arr = append(arr, v, nil)
		}
		return arr, nil

	default:
		return nil, fmt.Errorf("unknown attribute %T", av)
	}
}

func itemsToBson(items []map[string]types.AttributeValue, keySchema []string) ([][]byte, error) {
	bsonItems := make([][]byte, len(items))
	for i, m := range items {
		id, err := dynamoKeyToIdBson(m, keySchema)
		if err != nil {
			return nil, err
		}
		b, err := toBson(&types.AttributeValueMemberM{Value: m})
		if err != nil {
			return nil, err
		}

		// TODO: We currently clobber any existing _id
		bsonMap := b.(bson.M)
		if _, ok := bsonMap["_id"]; ok {
			// Should we relocate to another key?
			_ = ok
		}
		bsonMap["_id"] = id

		bsonItems[i], err = bson.Marshal(b)
		if err != nil {
			return nil, err
		}
	}
	return bsonItems, nil
}

func streamTypeToDynamoType(st streamtypes.AttributeValue) (types.AttributeValue, error) {
	switch tv := st.(type) {
	case *streamtypes.AttributeValueMemberB:
		return &types.AttributeValueMemberB{Value: tv.Value}, nil
	case *streamtypes.AttributeValueMemberBOOL:
		return &types.AttributeValueMemberBOOL{Value: tv.Value}, nil
	case *streamtypes.AttributeValueMemberBS:
		return &types.AttributeValueMemberBS{Value: tv.Value}, nil
	case *streamtypes.AttributeValueMemberL:
		var arr []types.AttributeValue
		for _, v := range tv.Value {
			v2, err := streamTypeToDynamoType(v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, v2)
		}
		return &types.AttributeValueMemberL{Value: arr}, nil
	case *streamtypes.AttributeValueMemberM:
		m := map[string]types.AttributeValue{}
		for k, v := range tv.Value {
			v2, err := streamTypeToDynamoType(v)
			if err != nil {
				return nil, err
			}
			m[k] = v2
		}
		return &types.AttributeValueMemberM{Value: m}, nil
	case *streamtypes.AttributeValueMemberN:
		return &types.AttributeValueMemberN{Value: tv.Value}, nil
	case *streamtypes.AttributeValueMemberNS:
		return &types.AttributeValueMemberNS{Value: tv.Value}, nil
	case *streamtypes.AttributeValueMemberS:
		return &types.AttributeValueMemberS{Value: tv.Value}, nil
	case *streamtypes.AttributeValueMemberSS:
		return &types.AttributeValueMemberSS{Value: tv.Value}, nil
	default:
		return nil, fmt.Errorf("unknown attribute %T", st)
	}
}

func dynamoWriteKeyValue(w io.Writer, av types.AttributeValue) error {
	switch tv := av.(type) {
	case *types.AttributeValueMemberB:
		binary.Write(w, binary.BigEndian, len(tv.Value))
		w.Write(tv.Value)
	case *types.AttributeValueMemberN:
		binary.Write(w, binary.BigEndian, len(tv.Value))
		w.Write([]byte(tv.Value))
	case *types.AttributeValueMemberS:
		binary.Write(w, binary.BigEndian, len(tv.Value))
		w.Write([]byte(tv.Value))
	default:
		return fmt.Errorf("key schema type unexpected %T", av)
	}
	return nil
}

func dynamoKeyToIdBson(attr map[string]types.AttributeValue, keySchema []string) (interface{}, error) {
	v, ok := attr[keySchema[0]]
	if !ok {
		return nil, fmt.Errorf("key schema does not match actual keys")
	}
	if len(keySchema) == 1 {
		return toBson(v)
	}
	v2, ok := attr[keySchema[0]]
	if !ok {
		return nil, fmt.Errorf("key schema does not match actual keys")
	}
	var buf bytes.Buffer
	dynamoWriteKeyValue(&buf, v)
	dynamoWriteKeyValue(&buf, v2)
	return primitive.Binary{
		Subtype: bson.TypeBinaryGeneric,
		Data:    buf.Bytes(),
	}, nil
}

func dynamoKeyToId(attr map[string]types.AttributeValue, keySchema []string) (*adiomv1.BsonValue, error) {
	b, err := dynamoKeyToIdBson(attr, keySchema)
	if err != nil {
		return nil, err
	}
	typ, data, err := bson.MarshalValue(b)
	if err != nil {
		return nil, err
	}
	return &adiomv1.BsonValue{
		Data: data,
		Type: uint32(typ),
	}, nil
}

func streamRecordToUpdate(record streamtypes.Record, keySchema []string) (*adiomv1.Update, error) {
	converted := map[string]types.AttributeValue{}
	for k, v := range record.Dynamodb.Keys {
		v2, err := streamTypeToDynamoType(v)
		if err != nil {
			return nil, err
		}
		converted[k] = v2
	}
	bsonValue, err := dynamoKeyToId(converted, keySchema)
	if err != nil {
		return nil, err
	}

	var typ adiomv1.UpdateType

	switch record.EventName {
	case streamtypes.OperationTypeInsert:
		typ = adiomv1.UpdateType_UPDATE_TYPE_INSERT
	case streamtypes.OperationTypeModify:
		typ = adiomv1.UpdateType_UPDATE_TYPE_UPDATE
	case streamtypes.OperationTypeRemove:
		typ = adiomv1.UpdateType_UPDATE_TYPE_DELETE
		return &adiomv1.Update{
			Id:   bsonValue,
			Type: typ,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported operation type")
	}

	item := record.Dynamodb.NewImage
	r, err := streamTypeToDynamoType(&streamtypes.AttributeValueMemberM{Value: item})
	if err != nil {
		return nil, err
	}
	b, err := toBson(r)
	if err != nil {
		return nil, err
	}
	marshaled, err := bson.Marshal(b)
	if err != nil {
		return nil, err
	}

	return &adiomv1.Update{
		Id:   bsonValue,
		Type: typ,
		Data: marshaled,
	}, nil
}
