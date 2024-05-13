package connector

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
)

func (mc *MongoConnector) convertChangeStreamEventToDataMessage(change bson.M) (iface.DataMessage, error) {
	docId := change["documentKey"].(bson.M)["_id"]
	slog.Debug(fmt.Sprintf("Change stream event for document %v: %v", docId, change))

	db := change["ns"].(bson.M)["db"].(string)
	col := change["ns"].(bson.M)["coll"].(string)
	optype := change["operationType"].(string)

	loc := iface.Location{Database: db, Collection: col}
	var dataMsg iface.DataMessage

	switch optype {
	case "insert":
		fullDocument := change["fullDocument"].(bson.M)
		// convert fulldocument to BSON.Raw
		fullDocumentRaw, err := bson.Marshal(fullDocument)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal full document: %v", err)
		}
		dataMsg = iface.DataMessage{Loc: loc, Data: &fullDocumentRaw, MutationType: iface.MutationType_Insert}
	default:
		return iface.DataMessage{}, fmt.Errorf("unsupported change event operation type: %v", optype)
	}

	return dataMsg, nil
}
