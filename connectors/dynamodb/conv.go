package dynamodb

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	streamtypes "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type NumberType string
type BsonIDFormat string

const (
	NumberTypeString  NumberType = "string"
	NumberTypeInt64   NumberType = "int64"
	NumberTypeInt32   NumberType = "int32"
	NumberTypeFloat64 NumberType = "float64"
	NumberTypeNumber  NumberType = "number"

	BsonIDFormatBinary    BsonIDFormat = "binary"
	BsonIDFormatComposite BsonIDFormat = "composite"
)

func ParseNumberType(s string) (NumberType, error) {
	if s == "" {
		return NumberTypeString, nil
	}
	switch NumberType(strings.ToLower(s)) {
	case NumberTypeString:
		return NumberTypeString, nil
	case NumberTypeInt64:
		return NumberTypeInt64, nil
	case NumberTypeInt32:
		return NumberTypeInt32, nil
	case NumberTypeFloat64:
		return NumberTypeFloat64, nil
	case NumberTypeNumber:
		return NumberTypeNumber, nil
	default:
		return "", fmt.Errorf("unsupported DynamoDB number type %q", s)
	}
}

func ParseBsonIDFormat(s string) (BsonIDFormat, error) {
	if s == "" {
		return BsonIDFormatBinary, nil
	}
	switch BsonIDFormat(strings.ToLower(s)) {
	case BsonIDFormatBinary:
		return BsonIDFormatBinary, nil
	case BsonIDFormatComposite:
		return BsonIDFormatComposite, nil
	default:
		return "", fmt.Errorf("unsupported DynamoDB BSON id format %q", s)
	}
}

func (n NumberType) convert(s string) (interface{}, error) {
	switch n {
	case "", NumberTypeString:
		return s, nil
	case NumberTypeInt64:
		return strconv.ParseInt(s, 10, 64)
	case NumberTypeInt32:
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(v), nil
	case NumberTypeFloat64:
		return strconv.ParseFloat(s, 64)
	case NumberTypeNumber:
		return bson.ParseDecimal128(s)
	default:
		return nil, fmt.Errorf("unsupported DynamoDB number type %q", n)
	}
}

// TODO: this is an arbitrary mapping right now
func fromBson(bs interface{}) (types.AttributeValue, error) {
	switch b := bs.(type) {
	case bson.A:
		arr := []types.AttributeValue{}
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
	case bson.DateTime:
		return &types.AttributeValueMemberS{Value: b.Time().Format(time.RFC3339)}, nil
	case bson.ObjectID:
		return &types.AttributeValueMemberS{Value: b.Hex()}, nil
	case bson.Binary:
		return &types.AttributeValueMemberB{Value: b.Data}, nil
	case bson.Decimal128:
		return &types.AttributeValueMemberN{Value: b.String()}, nil
	case nil:
		return &types.AttributeValueMemberNULL{Value: true}, nil
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

func toBson(av types.AttributeValue, numberType NumberType) (interface{}, error) {
	switch tv := av.(type) {
	case *types.AttributeValueMemberB:
		return bson.Binary{
			Subtype: bson.TypeBinaryGeneric,
			Data:    tv.Value,
		}, nil

	case *types.AttributeValueMemberBOOL:
		return tv.Value, nil

	case *types.AttributeValueMemberBS:
		arr := bson.A{}
		for _, v := range tv.Value {
			arr = append(arr, bson.Binary{
				Subtype: bson.TypeBinaryGeneric,
				Data:    v,
			})
		}
		return arr, nil

	case *types.AttributeValueMemberL:
		arr := bson.A{}
		for _, v := range tv.Value {
			entry, err := toBson(v, numberType)
			if err != nil {
				return nil, err
			}
			arr = append(arr, entry)
		}
		return arr, nil

	case *types.AttributeValueMemberM:
		m := bson.M{}
		for k, v := range tv.Value {
			entry, err := toBson(v, numberType)
			if err != nil {
				return nil, err
			}
			m[k] = entry
		}
		return m, nil

	case *types.AttributeValueMemberN:
		return numberType.convert(tv.Value)

	case *types.AttributeValueMemberNS:
		arr := bson.A{}
		for _, v := range tv.Value {
			entry, err := numberType.convert(v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, entry)
		}
		return arr, nil

	case *types.AttributeValueMemberS:
		return tv.Value, nil

	case *types.AttributeValueMemberSS:
		arr := bson.A{}
		for _, v := range tv.Value {
			arr = append(arr, v)
		}
		return arr, nil

	case *types.AttributeValueMemberNULL:
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown attribute %T", av)
	}
}

func normalizeJSONNumbers(v interface{}) interface{} {
	switch tv := v.(type) {
	case attributevalue.Number:
		return json.Number(tv.String())
	case []attributevalue.Number:
		arr := make([]json.Number, 0, len(tv))
		for _, n := range tv {
			arr = append(arr, json.Number(n.String()))
		}
		return arr
	case []interface{}:
		arr := make([]interface{}, 0, len(tv))
		for _, v := range tv {
			arr = append(arr, normalizeJSONNumbers(v))
		}
		return arr
	case map[string]interface{}:
		m := map[string]interface{}{}
		for k, v := range tv {
			m[k] = normalizeJSONNumbers(v)
		}
		return m
	default:
		return v
	}
}

func toInterfaceMap(av types.AttributeValue, numberType NumberType) (map[string]interface{}, error) {
	var jsonable map[string]interface{}
	switch numberType {
	case "", NumberTypeFloat64, NumberTypeString, NumberTypeInt64, NumberTypeInt32:
		if err := attributevalue.Unmarshal(av, &jsonable); err != nil {
			return nil, err
		}
	case NumberTypeNumber:
		if err := attributevalue.UnmarshalWithOptions(av, &jsonable, func(options *attributevalue.DecoderOptions) {
			options.UseNumber = true
		}); err != nil {
			return nil, err
		}
		jsonable = normalizeJSONNumbers(jsonable).(map[string]interface{})
	default:
		return nil, fmt.Errorf("unsupported DynamoDB number type %q", numberType)
	}
	return jsonable, nil
}

func itemsToJson(items []map[string]types.AttributeValue, keySchema []string, numberType NumberType) ([][]byte, error) {
	jsonItems := make([][]byte, 0, len(items))
	for _, m := range items {
		_, id, err := dynamoKeyToJsonId(m, keySchema)
		if err != nil {
			return nil, err
		}

		jsonable, err := toInterfaceMap(&types.AttributeValueMemberM{Value: m}, numberType)
		if err != nil {
			return nil, err
		}

		// TODO: We currently clobber any existing id
		jsonable["id"] = id

		j, err := json.Marshal(jsonable)
		if err != nil {
			return nil, err
		}

		jsonItems = append(jsonItems, j)
	}
	return jsonItems, nil
}

func itemsToBson(items []map[string]types.AttributeValue, keySchema []string, numberType NumberType, bsonIDFormat BsonIDFormat) ([][]byte, error) {
	bsonItems := make([][]byte, len(items))
	for i, m := range items {
		id, err := dynamoKeyToIdBson(m, keySchema, numberType, bsonIDFormat)
		if err != nil {
			return nil, fmt.Errorf("err in key to bson: %w", err)
		}
		b, err := toBson(&types.AttributeValueMemberM{Value: m}, numberType)
		if err != nil {
			return nil, fmt.Errorf("err in to bson: %w", err)
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
	case *streamtypes.AttributeValueMemberNULL:
		return &types.AttributeValueMemberNULL{Value: tv.Value}, nil
	default:
		return nil, fmt.Errorf("unknown attribute %T", st)
	}
}

func dynamoWriteKeyValue(w io.Writer, av types.AttributeValue) error {
	switch tv := av.(type) {
	case *types.AttributeValueMemberB:
		if err := binary.Write(w, binary.BigEndian, int32(len(tv.Value))); err != nil {
			return err
		}
		if _, err := w.Write(tv.Value); err != nil {
			return err
		}
	case *types.AttributeValueMemberN:
		if err := binary.Write(w, binary.BigEndian, int32(len(tv.Value))); err != nil {
			return err
		}
		if _, err := w.Write([]byte(tv.Value)); err != nil {
			return err
		}
	case *types.AttributeValueMemberS:
		if err := binary.Write(w, binary.BigEndian, int32(len(tv.Value))); err != nil {
			return err
		}
		if _, err := w.Write([]byte(tv.Value)); err != nil {
			return err
		}
	default:
		return fmt.Errorf("key schema type unexpected %T", av)
	}
	return nil
}

func dynamoKeyToJsonId(attr map[string]types.AttributeValue, keySchema []string) ([]*adiomv1.BsonValue, string, error) {
	var res []*adiomv1.BsonValue
	var sb strings.Builder
	var id string
	for i, k := range keySchema {
		v, ok := attr[k]
		if !ok {
			return nil, "", fmt.Errorf("key schema does not match actual keys")
		}
		if i > 0 {
			sb.WriteString("-")
		}
		var s string
		switch tv := v.(type) {
		case *types.AttributeValueMemberB:
			s = base64.StdEncoding.EncodeToString(tv.Value)
		case *types.AttributeValueMemberN:
			s = tv.Value
		case *types.AttributeValueMemberS:
			s = tv.Value
		default:
			return nil, "", fmt.Errorf("key schema type unexpected %T", v)
		}
		if k == "id" {
			id = s
		}
		sb.WriteString(s)
		typ, data, err := bson.MarshalValue(s)
		if err != nil {
			return nil, "", err
		}
		res = append(res, &adiomv1.BsonValue{
			Data: data,
			Type: uint32(typ),
			Name: k,
		})
	}
	if id == "" {
		id = sb.String()
	}
	if len(keySchema) > 1 {
		typ, data, err := bson.MarshalValue(id)
		if err != nil {
			return nil, "", err
		}
		res = append(res, &adiomv1.BsonValue{
			Data: data,
			Type: uint32(typ),
			Name: "id",
		})
	}
	return res, id, nil
}

func dynamoKeyToCompositeBsonId(attr map[string]types.AttributeValue, keySchema []string, numberType NumberType) ([]*adiomv1.BsonValue, interface{}, error) {
	var res []*adiomv1.BsonValue
	if len(keySchema) > 1 {
		for _, k := range keySchema {
			v, ok := attr[k]
			if !ok {
				return nil, nil, fmt.Errorf("key schema does not match actual keys")
			}
			b, err := toBson(v, numberType)
			if err != nil {
				return nil, nil, err
			}
			bsonValue, err := bsonValueFromInterface(b)
			if err != nil {
				return nil, nil, err
			}
			bsonValue.Name = k
			res = append(res, bsonValue)
		}
	}
	_, id, err := dynamoKeyToJsonId(attr, keySchema)
	if err != nil {
		return nil, nil, err
	}
	idValue, err := bsonValueFromInterface(id)
	if err != nil {
		return nil, nil, err
	}
	idValue.Name = "_id"
	res = append(res, idValue)
	return res, id, nil
}

func dynamoKeyToIdBson(attr map[string]types.AttributeValue, keySchema []string, numberType NumberType, bsonIDFormat BsonIDFormat) (interface{}, error) {
	switch bsonIDFormat {
	case "", BsonIDFormatBinary:
	case BsonIDFormatComposite:
		_, id, err := dynamoKeyToCompositeBsonId(attr, keySchema, numberType)
		return id, err
	default:
		return nil, fmt.Errorf("unsupported DynamoDB BSON id format %q", bsonIDFormat)
	}

	v, ok := attr[keySchema[0]]
	if !ok {
		return nil, fmt.Errorf("key schema does not match actual keys")
	}
	if len(keySchema) == 1 {
		return toBson(v, numberType)
	}
	v2, ok := attr[keySchema[1]]
	if !ok {
		return nil, fmt.Errorf("key schema does not match actual keys")
	}
	var buf bytes.Buffer
	if err := dynamoWriteKeyValue(&buf, v); err != nil {
		return nil, err
	}
	if err := dynamoWriteKeyValue(&buf, v2); err != nil {
		return nil, err
	}
	return bson.Binary{
		Subtype: bson.TypeBinaryGeneric,
		Data:    buf.Bytes(),
	}, nil
}

func bsonValueFromInterface(b interface{}) (*adiomv1.BsonValue, error) {
	typ, data, err := bson.MarshalValue(b)
	if err != nil {
		return nil, err
	}
	return &adiomv1.BsonValue{
		Data: data,
		Type: uint32(typ),
	}, nil
}

func dynamoKeyToId(attr map[string]types.AttributeValue, keySchema []string, numberType NumberType, bsonIDFormat BsonIDFormat) (*adiomv1.BsonValue, error) {
	b, err := dynamoKeyToIdBson(attr, keySchema, numberType, bsonIDFormat)
	if err != nil {
		return nil, err
	}
	return bsonValueFromInterface(b)
}

func dynamoKeyToUpdateIdBson(attr map[string]types.AttributeValue, keySchema []string, numberType NumberType, bsonIDFormat BsonIDFormat) ([]*adiomv1.BsonValue, error) {
	switch bsonIDFormat {
	case "", BsonIDFormatBinary:
		bsonValue, err := dynamoKeyToId(attr, keySchema, numberType, bsonIDFormat)
		if err != nil {
			return nil, err
		}
		return []*adiomv1.BsonValue{bsonValue}, nil
	case BsonIDFormatComposite:
		id, _, err := dynamoKeyToCompositeBsonId(attr, keySchema, numberType)
		return id, err
	default:
		return nil, fmt.Errorf("unsupported DynamoDB BSON id format %q", bsonIDFormat)
	}
}

func streamRecordToUpdate(record streamtypes.Record, dataType adiomv1.DataType, keySchema []string, numberType NumberType, bsonIDFormat BsonIDFormat) (*adiomv1.Update, error) {
	converted := map[string]types.AttributeValue{}
	for k, v := range record.Dynamodb.Keys {
		v2, err := streamTypeToDynamoType(v)
		if err != nil {
			return nil, err
		}
		converted[k] = v2
	}

	var id []*adiomv1.BsonValue
	var jId string // used for json id type
	switch dataType {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		var err error
		id, err = dynamoKeyToUpdateIdBson(converted, keySchema, numberType, bsonIDFormat)
		if err != nil {
			return nil, err
		}
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		var err error
		id, jId, err = dynamoKeyToJsonId(converted, keySchema)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported data type")
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
			Id:   id,
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

	var marshaled []byte
	switch dataType {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		b, err := toBson(r, numberType)
		if err != nil {
			return nil, err
		}
		idBson, err := dynamoKeyToIdBson(converted, keySchema, numberType, bsonIDFormat)
		if err != nil {
			return nil, err
		}
		b.(bson.M)["_id"] = idBson
		marshaled, err = bson.Marshal(b)
		if err != nil {
			return nil, err
		}
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		j, err := toInterfaceMap(r, numberType)
		if err != nil {
			return nil, err
		}
		j["id"] = jId
		marshaled, err = json.Marshal(j)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported data type")
	}

	return &adiomv1.Update{
		Id:   id,
		Type: typ,
		Data: marshaled,
	}, nil
}
