package postgres

import (
	"bytes"
	"fmt"
	"net/netip"

	"github.com/jackc/pgx/v5/pgtype"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// bsonToMap converts BSON data to a map[string]interface{}. The _id field is
// removed when it is synthetic rather than a destination column.
func bsonToMap(reg *bson.Registry, data []byte, preserveID bool) (map[string]interface{}, error) {
	var m map[string]interface{}
	dec := bson.NewDecoder(bson.NewDocumentReader(bytes.NewReader(data)))
	dec.SetRegistry(reg)
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	if !preserveID {
		delete(m, "_id")
	}
	for key, value := range m {
		converted, err := bsonValueForPostgres(key, value)
		if err != nil {
			return nil, err
		}
		m[key] = converted
	}

	return m, nil
}

func bsonValueForPostgres(field string, value interface{}) (interface{}, error) {
	binary, ok := value.(bson.Binary)
	if !ok {
		return value, nil
	}
	switch binary.Subtype {
	case bson.TypeBinaryGeneric:
		return binary.Data, nil
	case bson.TypeBinaryUUIDOld:
		return nil, fmt.Errorf("BSON legacy UUID subtype 3 is not supported for field %q", field)
	case bson.TypeBinaryUUID:
		if len(binary.Data) != 16 {
			return nil, fmt.Errorf("BSON UUID field %q has %d bytes, expected 16", field, len(binary.Data))
		}
		var uuid [16]byte
		copy(uuid[:], binary.Data)
		return pgtype.UUID{Bytes: uuid, Valid: true}, nil
	default:
		return value, nil
	}
}

func bsonSafeValue(value any) any {
	switch value := value.(type) {
	case netip.Addr:
		return value.String()
	case netip.Prefix:
		return value.String()
	case map[string]interface{}:
		result := make(map[string]interface{}, len(value))
		for key, nested := range value {
			result[key] = bsonSafeValue(nested)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(value))
		for i, nested := range value {
			result[i] = bsonSafeValue(nested)
		}
		return result
	default:
		return value
	}
}

// marshalWithRegistry marshals val using the given registry
func marshalWithRegistry(reg *bson.Registry, val any) ([]byte, error) {
	var buf bytes.Buffer
	enc := bson.NewEncoder(bson.NewDocumentWriter(&buf))
	enc.SetRegistry(reg)
	if err := enc.Encode(bsonSafeValue(val)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// marshalValueWithRegistry marshals a single value using the given registry
func marshalValueWithRegistry(reg *bson.Registry, val any) (bson.Type, []byte, error) {
	var buf bytes.Buffer
	enc := bson.NewEncoder(bson.NewDocumentWriter(&buf))
	enc.SetRegistry(reg)
	if err := enc.Encode(bson.D{{Key: "v", Value: bsonSafeValue(val)}}); err != nil {
		return 0, nil, err
	}
	raw := bson.Raw(buf.Bytes())
	rv := raw.Lookup("v")
	return rv.Type, rv.Value, nil
}
