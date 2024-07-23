package connectorCosmos

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/mitchellh/hashstructure"
	"go.mongodb.org/mongo-driver/bson"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

const (
	progressReportingIntervalSec = 10
)

type ReaderProgress struct {
	initialSyncDocs    atomic.Uint64
	changeStreamEvents uint64
	tasksTotal         uint64
	tasksCompleted     uint64
}

func (cc *CosmosConnector) printProgress(readerProgress *ReaderProgress) {
	ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
	defer ticker.Stop()
	startTime := time.Now()
	operations := uint64(0)
	for {
		select {
		case <-cc.flowCtx.Done():
			return
		case <-ticker.C:
			elapsedTime := time.Since(startTime).Seconds()
			operations_delta := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents - operations
			opsPerSec := math.Floor(float64(operations_delta) / elapsedTime)
			// Print reader progress
			slog.Info(fmt.Sprintf("Reader Progress: Initial Sync Docs - %d (%d/%d tasks completed), Change Stream Events - %d, Operations per Second - %.2f",
				readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted, readerProgress.tasksTotal, readerProgress.changeStreamEvents, opsPerSec))

			startTime = time.Now()
			operations = readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
		}
	}
}

func (cc *CosmosConnector) getLatestResumeToken(location iface.Location) (bson.Raw, error) {
	slog.Debug(fmt.Sprintf("Getting latest resume token for location: %v\n", location))
	opts := moptions.ChangeStream().SetFullDocument(moptions.UpdateLookup)
	changeStream, err := cc.createChangeStream(location, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(cc.ctx)

	// we need ANY event to get the resume token that we can use to extract the cluster time
	var id interface{}
	col := cc.client.Database(location.Database).Collection(location.Collection)
	go func() {
		result, err := col.InsertOne(cc.ctx, bson.M{})
		if err != nil {
			slog.Error(fmt.Sprintf("Error inserting dummy record: %v", err.Error()))
		}
		id = result.InsertedID
	}()

	//get the resume token from the change stream event, then delete the inserted document
	changeStream.Next(cc.ctx)
	resumeToken := changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}
	col.DeleteOne(cc.ctx, bson.M{"_id": id})
	return resumeToken, nil
}

// Generates static connector ID based on connection string
// XXX: is this the best place to do this? - move to overall connector util file
func generateConnectorID(connectionString string) iface.ConnectorID {
	id, err := hashstructure.Hash(connectionString, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.ConnectorID(strconv.FormatUint(id, 16))
}

func (cc *CosmosConnector) convertChangeStreamEventToDataMessage(change bson.M) (iface.DataMessage, error) {
	slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

	db := change["ns"].(bson.M)["db"].(string)
	col := change["ns"].(bson.M)["coll"].(string)
	loc := iface.Location{Database: db, Collection: col}
	var dataMsg iface.DataMessage

	// treat all change stream events as updates
	// get the id of the document that was changed
	id := change["documentKey"].(bson.M)["_id"]
	// convert id to raw bson
	idType, idVal, err := bson.MarshalValue(id)
	if err != nil {
		return iface.DataMessage{}, fmt.Errorf("failed to marshal _id: %v", err)
	}
	// get the full state of the document after the change
	if change["fullDocument"] == nil {
		//TODO (AK, 6/2024): find a better way to report that we need to ignore this event
		return iface.DataMessage{MutationType: iface.MutationType_Reserved}, nil // no full document, nothing to do (probably got deleted before we got to the event in the change stream)
	}
	fullDocument := change["fullDocument"].(bson.M)
	// convert fulldocument to BSON.Raw
	fullDocumentRaw, err := bson.Marshal(fullDocument)
	if err != nil {
		return iface.DataMessage{}, fmt.Errorf("failed to marshal full document: %v", err)
	}
	dataMsg = iface.DataMessage{Loc: loc, Id: &idVal, IdType: byte(idType), Data: &fullDocumentRaw, MutationType: iface.MutationType_Update}

	slog.Debug(fmt.Sprintf("Converted change stream event to data message %v", dataMsg))
	return dataMsg, nil
}
