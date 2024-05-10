package connector

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (mc *MongoConnector) processDataMessage(dataMsg iface.DataMessage, collection *mongo.Collection) error {
	data := *dataMsg.Data

	switch dataMsg.OpType {
	case iface.OpType_Insert:
		_, err := collection.InsertOne(mc.ctx, bson.Raw(data))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to insert document into collection: %v", err))
			return err
		}
	default:
		return fmt.Errorf("unsupported operation type: %v", dataMsg.OpType)
	}

	return nil
}
