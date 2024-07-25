package connectorCosmos

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/mitchellh/hashstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
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

// Generates static connector ID based on connection string
// XXX: is this the best place to do this? - move to overall connector util file
func generateConnectorID(connectionString string) iface.ConnectorID {
	id, err := hashstructure.Hash(connectionString, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.ConnectorID(strconv.FormatUint(id, 16))
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

// Creates a single changestream compatible with CosmosDB with the provided options
func (cc *CosmosConnector) createChangeStream(namespace iface.Location, opts *moptions.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	db := namespace.Database
	col := namespace.Collection
	collection := cc.client.Database(db).Collection(col)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace"}}}}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}, {Key: "fullDocument", Value: 1}, {Key: "ns", Value: 1}, {Key: "documentKey", Value: 1}}}}}

	changeStream, err := collection.Watch(cc.ctx, pipeline, opts)
	if err != nil {
		return nil, err
	}
	return changeStream, nil
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
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		result, err := col.InsertOne(cc.ctx, bson.M{})
		if err != nil {
			slog.Error(fmt.Sprintf("Error inserting dummy record: %v", err.Error()))
		}
		id = result.InsertedID
	}()
	wg.Wait()
	//get the resume token from the change stream event, then delete the inserted document
	changeStream.Next(cc.ctx)
	resumeToken := changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}
	col.DeleteOne(cc.ctx, bson.M{"_id": id})
	return resumeToken, nil
}

func (cc *CosmosConnector) convertChangeStreamEventToDataMessage(change bson.M) (iface.DataMessage, error) {
	//slog.Debug(fmt.Sprintf("Converting change stream event %v", change))

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

	//slog.Debug(fmt.Sprintf("Converted change stream event to data message %v", dataMsg))
	return dataMsg, nil
}

// Creates parallel change streams for each task in the read plan, and processes the events concurrently
func (cc *CosmosConnector) StartConcurrentChangeStreams(tasks []iface.ReadPlanTask, readerProgress *ReaderProgress, channel chan<- iface.DataMessage) error {
	var wg sync.WaitGroup
	// global atomic lsn counter
	var lsn int64 = 0

	// iterate over all tasks and start a change stream for each
	for _, task := range tasks {
		wg.Add(1)
		go func(task iface.ReadPlanTask) {
			defer wg.Done()
			//get task location and retrieve resume token
			loc := iface.Location{Database: task.Def.Db, Collection: task.Def.Col}
			slog.Info(fmt.Sprintf("Connector %s is starting to read change stream for flow %s at namespace %s.%s", cc.id, cc.flowId, loc.Database, loc.Collection))

			token, err := cc.flowCDCResumeTokenMap.GetToken(loc)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get resume token for location %v: %v", loc, err))
			}
			//set the change stream options to start from the resume token
			opts := moptions.ChangeStream().SetResumeAfter(token).SetFullDocument(moptions.UpdateLookup)
			changeStream, err := cc.createChangeStream(loc, opts)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to create change stream for task %v, namespace %s.%s: %v", task.Id, task.Def.Db, task.Def.Col, err))
				slog.Info(fmt.Sprintf("Failed change stream, Resume token map: %v", cc.flowCDCResumeTokenMap.Map)) //Debug for __test431 rid mismatch error
				return
			}
			defer changeStream.Close(cc.flowCtx)

			cc.status.CDCActive = true
			//process the change stream events for this change stream
			cc.processChangeStreamEvent(readerProgress, changeStream, loc, channel, &lsn)

			if err := changeStream.Err(); err != nil {
				if cc.flowCtx.Err() == context.Canceled {
					slog.Debug(fmt.Sprintf("Change stream error: %v, but the context was cancelled", err))
				} else {
					slog.Error(fmt.Sprintf("Change stream error: %v", err))
				}
			}

		}(task)
	}
	wg.Wait()
	return nil
}

func (cc *CosmosConnector) processChangeStreamEvent(readerProgress *ReaderProgress, changeStream *mongo.ChangeStream, changeStreamLoc iface.Location, dataChannel chan<- iface.DataMessage, lsn *int64) {

	for changeStream.Next(cc.flowCtx) {
		var change bson.M
		if err := changeStream.Decode(&change); err != nil {
			slog.Error(fmt.Sprintf("Failed to decode change stream event: %v", err))
			continue
		}

		dataMsg, err := cc.convertChangeStreamEventToDataMessage(change)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to convert change stream event to data message: %v", err))
			continue
		}
		if dataMsg.MutationType == iface.MutationType_Reserved { //TODO (AK, 6/2024): find a better way to indicate that we need to skip this event
			slog.Debug(fmt.Sprintf("Skipping the event: %v", change))
			continue
		}
		//send the data message
		currLSN := cc.updateLSNTracking(readerProgress, lsn)
		dataMsg.SeqNum = currLSN
		dataChannel <- dataMsg

		//update the last seen resume token
		cc.flowCDCResumeTokenMap.AddToken(changeStreamLoc, changeStream.ResumeToken())
	}

}

// update LSN and changeStreamEvents counters atomically
func (cc *CosmosConnector) updateLSNTracking(reader *ReaderProgress, lsn *int64) int64 {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()
	reader.changeStreamEvents++
	*lsn++
	cc.status.WriteLSN++
	return cc.status.WriteLSN
}
