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
	//XXX: remove this
	dbName = "test"

	collection := mc.client.Database(dbName).Collection(colName)

	switch dataMsg.MutationType {
	case iface.MutationType_Insert:
		_, err := collection.InsertOne(mc.ctx, bson.Raw(data))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to insert document into collection: %v", err))
			return err
		}
	default:
		return fmt.Errorf("unsupported operation type: %v", dataMsg.MutationType)
	}

	return nil
}
