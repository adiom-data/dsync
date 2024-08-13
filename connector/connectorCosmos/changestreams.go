/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorCosmos

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

// Creates a single changestream compatible with CosmosDB with the provided options
func (cc *CosmosConnector) createChangeStream(ctx context.Context, namespace iface.Location, opts *moptions.ChangeStreamOptions) (*mongo.ChangeStream, error) {
	db := namespace.Database
	col := namespace.Collection
	collection := cc.client.Database(db).Collection(col)
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: bson.A{"insert", "update", "replace"}}}}}}},
		bson.D{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}, {Key: "fullDocument", Value: 1}, {Key: "ns", Value: 1}, {Key: "documentKey", Value: 1}}}}}

	changeStream, err := collection.Watch(ctx, pipeline, opts)
	if err != nil {
		return nil, err
	}
	return changeStream, nil
}

// Creates parallel change streams for each task in the read plan, and processes the events concurrently
func (cc *CosmosConnector) StartConcurrentChangeStreams(ctx context.Context, namespaces []namespace, readerProgress *ReaderProgress, channel chan<- iface.DataMessage) error {
	var wg sync.WaitGroup
	// global atomic lsn counter
	var lsn int64 = 0

	cc.status.CDCActive = true
	// iterate over all tasks and start a change stream for each
	for _, ns := range namespaces {
		wg.Add(1)
		go func(ns namespace) {
			defer wg.Done()
			//get task location and retrieve resume token
			loc := iface.Location{Database: ns.db, Collection: ns.col}
			slog.Info(fmt.Sprintf("Connector %s is starting to read change stream for flow %s at namespace %s.%s", cc.id, cc.flowId, loc.Database, loc.Collection))

			token, err := cc.flowCDCResumeTokenMap.GetToken(loc)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to get resume token for location %v: %v", loc, err))
			}
			//set the change stream options to start from the resume token
			opts := moptions.ChangeStream().SetResumeAfter(token).SetFullDocument(moptions.UpdateLookup)
			changeStream, err := cc.createChangeStream(ctx, loc, opts)
			if err != nil {
				if ctx.Err() == context.Canceled {
					slog.Debug(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v, but the context was cancelled", loc.Database, loc.Collection, err))
				} else {
					slog.Error(fmt.Sprintf("Failed to create change stream for namespace %s.%s: %v", loc.Database, loc.Collection, err))
				}
				return
			}
			defer changeStream.Close(ctx)

			//process the change stream events for this change stream
			cc.processChangeStreamEvents(ctx, readerProgress, changeStream, loc, channel, &lsn)

			if err := changeStream.Err(); err != nil {
				if ctx.Err() == context.Canceled {
					slog.Debug(fmt.Sprintf("Change stream error: %v, but the context was cancelled", err))
				} else {
					slog.Error(fmt.Sprintf("Change stream error: %v", err))
				}
			}

		}(ns)
	}
	wg.Wait()
	return nil
}

// Reads and processes change stream events, and sends messages to the data channel
func (cc *CosmosConnector) processChangeStreamEvents(ctx context.Context, readerProgress *ReaderProgress, changeStream *mongo.ChangeStream, changeStreamLoc iface.Location, dataChannel chan<- iface.DataMessage, lsn *int64) {
	for changeStream.Next(ctx) {
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

	return dataMsg, nil
}
