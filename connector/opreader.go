package connector

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
)

func (mc *MongoConnector) shouldIgnoreChangeStreamEvent(change bson.M) bool {
	db := change["ns"].(bson.M)["db"].(string)
	col := change["ns"].(bson.M)["coll"].(string)

	//We need to filter out the dummy collection
	//TODO: is it the best way to do it?
	if (db == dummyDB) && (col == dummyCol) {
		return true
	}

	return false
}

func (mc *MongoConnector) convertChangeStreamEventToDataMessage(change bson.M) (iface.DataMessage, error) {
	slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

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
	case "update":
		// get the id of the document that was changed
		id := change["documentKey"].(bson.M)["_id"]
		// convert id to raw bson
		idType, idVal, err := bson.MarshalValue(id)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal _id: %v", err)
		}
		// get the full state of the document after the change
		if change["fullDocument"] == nil {
			//TODO: find a better way to report this
			return iface.DataMessage{MutationType: iface.MutationType_Reserved}, nil // no full document, nothing to do (probably got deleted before we got to the event in the change stream)
		}
		fullDocument := change["fullDocument"].(bson.M)
		// convert fulldocument to BSON.Raw
		fullDocumentRaw, err := bson.Marshal(fullDocument)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal full document: %v", err)
		}
		dataMsg = iface.DataMessage{Loc: loc, Id: &idVal, IdType: byte(idType), Data: &fullDocumentRaw, MutationType: iface.MutationType_Update}
	case "delete":
		// get the id of the document that was deleted
		id := change["documentKey"].(bson.M)["_id"]
		// convert id to raw bson
		idType, idVal, err := bson.MarshalValue(id)
		if err != nil {
			return iface.DataMessage{}, fmt.Errorf("failed to marshal _id: %v", err)
		}
		dataMsg = iface.DataMessage{Loc: loc, Id: &idVal, IdType: byte(idType), MutationType: iface.MutationType_Delete}
	default:
		return iface.DataMessage{}, fmt.Errorf("unsupported change event operation type: %v", optype)
	}

	return dataMsg, nil
}
