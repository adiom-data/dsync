package connector

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

func (mc *MongoConnector) processDataMessage(dataMsg iface.DataMessage) error {
	dbName := dataMsg.Loc.Database
	colName := dataMsg.Loc.Collection

	collection := mc.client.Database(dbName).Collection(colName)

	switch dataMsg.MutationType {
	case iface.MutationType_Insert:
		data := *dataMsg.Data
		_, err := collection.InsertOne(mc.ctx, bson.Raw(data))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to insert document into collection: %v", err))
			return err
		}
	case iface.MutationType_InsertBatch:
		dataBatch := dataMsg.DataBatch
		slog.Debug(fmt.Sprintf("Inserting batch of %v documents into collection %v.%v", len(dataBatch), dbName, colName))
		//excplicitly cast to []bson.Raw to avoid type mismatch
		dataBatchBson := make([]interface{}, len(dataBatch))
		for i := range dataBatchBson {
			dataBatchBson[i] = bson.Raw(dataBatch[i])
		}

		_, err := collection.InsertMany(mc.ctx, dataBatchBson)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
			return err
		}
	case iface.MutationType_Update:
		idType := bsontype.Type(dataMsg.IdType)
		data := *dataMsg.Data
		_, err := collection.ReplaceOne(mc.ctx, bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}}, bson.Raw(data))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to update document in the collection: %v", err))
			return err
		}
	case iface.MutationType_Delete:
		idType := bsontype.Type(dataMsg.IdType)
		_, err := collection.DeleteOne(mc.ctx, bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to delete document from collection: %v", err))
			return err
		}
	default:
		return fmt.Errorf("unsupported operation type: %v", dataMsg.MutationType)
	}

	return nil
}
