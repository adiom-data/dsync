package postgres

import (
	"bytes"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// bsonToMap converts BSON data to a map[string]interface{} excluding the _id field
func bsonToMap(reg *bson.Registry, data []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	dec := bson.NewDecoder(bson.NewDocumentReader(bytes.NewReader(data)))
	dec.SetRegistry(reg)
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	// Remove _id as it's a synthetic field for postgres
	delete(m, "_id")

	return m, nil
}

// marshalWithRegistry marshals val using the given registry
func marshalWithRegistry(reg *bson.Registry, val any) ([]byte, error) {
	var buf bytes.Buffer
	enc := bson.NewEncoder(bson.NewDocumentWriter(&buf))
	enc.SetRegistry(reg)
	if err := enc.Encode(val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// marshalValueWithRegistry marshals a single value using the given registry
func marshalValueWithRegistry(reg *bson.Registry, val any) (bson.Type, []byte, error) {
	var buf bytes.Buffer
	enc := bson.NewEncoder(bson.NewDocumentWriter(&buf))
	enc.SetRegistry(reg)
	if err := enc.Encode(bson.D{{Key: "v", Value: val}}); err != nil {
		return 0, nil, err
	}
	raw := bson.Raw(buf.Bytes())
	rv := raw.Lookup("v")
	return rv.Type, rv.Value, nil
}
