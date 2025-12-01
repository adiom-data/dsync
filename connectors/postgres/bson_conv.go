package postgres

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
)

// bsonToMap converts BSON data to a map[string]interface{} excluding the _id field
func bsonToMap(reg *bsoncodec.Registry, data []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := bson.UnmarshalWithRegistry(reg, data, &m); err != nil {
		return nil, err
	}
	// Remove _id as it's a synthetic field for postgres
	delete(m, "_id")

	return m, nil
}
