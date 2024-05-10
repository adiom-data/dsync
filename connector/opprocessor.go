package connector

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
)

func (mc *MongoConnector) processDataMessage(dataMsg iface.DataMessage) error {
	data := *dataMsg.Data
	dbName := dataMsg.Loc.Database
	colName := dataMsg.Loc.Collection

	//slog.Info(fmt.Sprintf("Overwriting db '%v' with test value 'test'", dbName))
	dbName = "test"

	collection := mc.client.Database(dbName).Collection(colName)

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
