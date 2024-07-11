/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorMongo

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TODO: this needs to be synchronized with the actual processing of the data messages
func (mc *MongoConnector) handleBarrierMessage(barrierMsg iface.DataMessage) error {
	// print barrier message
	slog.Debug(fmt.Sprintf("Received barrier message of type %v for id %v", barrierMsg.BarrierType, barrierMsg.BarrierTaskId))
	// notify the coordinator that the task is done from our side
	mc.coord.NotifyTaskDone(mc.flowId, mc.id, (iface.ReadPlanTaskID)(barrierMsg.BarrierTaskId))
	return nil
}

// TODO (AK, 6/2024): this should be parallelized with a batch assembly (e.g. per-namespace) and a worker pool
func (mc *MongoConnector) processDataMessage(dataMsg iface.DataMessage) error {
	dbName := dataMsg.Loc.Database
	colName := dataMsg.Loc.Collection

	collection := mc.client.Database(dbName).Collection(colName)

	switch dataMsg.MutationType {
	case iface.MutationType_Insert:
		data := *dataMsg.Data
		err := insertBatchOverwrite(mc.ctx, collection, []interface{}{bson.Raw(data)})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to insert document into collection: %v", err))
			return err
		}
	case iface.MutationType_InsertBatch:
		dataBatch := dataMsg.DataBatch
		slog.Debug(fmt.Sprintf("Inserting batch of %v documents into collection %v.%v", len(dataBatch), dbName, colName))
		//explicitly cast to []bson.Raw to avoid type mismatch
		dataBatchBson := make([]interface{}, len(dataBatch))
		for i := range dataBatchBson {
			dataBatchBson[i] = bson.Raw(dataBatch[i])
		}

		//XXX (AK, 6/2024): ugly hack to deal with rate limiting in Cosmos but might also be good for controlling impact on the dst
		// we split in subbatches of mc.settings.writerMaxBatchSize if it's not 0
		if (mc.settings.writerMaxBatchSize <= 0) || (len(dataBatch) <= mc.settings.writerMaxBatchSize) {
			err := insertBatchOverwrite(mc.ctx, collection, dataBatchBson)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
				return err
			}
		} else {
			slog.Debug(fmt.Sprintf("Splitting the batch because it's bigger than max size set: %v", mc.settings.writerMaxBatchSize))
			batchSizeLeft := len(dataBatch)
			idx := 0
			for batchSizeLeft > mc.settings.writerMaxBatchSize {
				slog.Debug(fmt.Sprintf("Inserting subbatch of %v documents (idx %v) into collection %v.%v", mc.settings.writerMaxBatchSize, idx, dbName, colName))
				batchPart := dataBatchBson[idx : idx+mc.settings.writerMaxBatchSize]
				err := insertBatchOverwrite(mc.ctx, collection, batchPart)
				if err != nil {
					slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
					return err
				}
				batchSizeLeft -= mc.settings.writerMaxBatchSize
				idx += mc.settings.writerMaxBatchSize
			}
			if batchSizeLeft > 0 {
				slog.Debug(fmt.Sprintf("Inserting subbatch(tail-end) of %v documents (idx %v) into collection %v.%v", len(dataBatchBson[idx:]), idx, dbName, colName))
				err := insertBatchOverwrite(mc.ctx, collection, dataBatchBson[idx:])
				if err != nil {
					slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
					return err
				}
			}
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

	mc.status.WriteLSN = max(dataMsg.SeqNum, mc.status.WriteLSN) //XXX (AK, 6/2024): this is just a placeholder for now that won't work well if things are processed out of order or if they are parallelized

	return nil
}

// inserts data and overwrites on conflict
func insertBatchOverwrite(ctx context.Context, collection *mongo.Collection, documents []interface{}) error {
	// eagerly attempt an unordered insert
	_, bwErr := collection.InsertMany(ctx, documents, options.InsertMany().SetOrdered(false))

	// check the errors and collect those that errored out due to duplicate key errors
	// we will skip all the other errors for now
	if bwErr != nil {
		var bulkOverwrite []mongo.WriteModel

		if bwErrWriteErrors, ok := bwErr.(mongo.BulkWriteException); ok {
			for _, we := range bwErrWriteErrors.WriteErrors {
				if mongo.IsDuplicateKeyError(we.WriteError) {
					doc := documents[we.Index]
					id := doc.(bson.Raw).Lookup("_id") //we know it's there because there was a conflict on _id
					bulkOverwrite = append(bulkOverwrite, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": id}).SetReplacement(doc).SetUpsert(true))
				} else {
					slog.Error(fmt.Sprintf("Skipping failure to insert document into collection: %v", we.WriteError))
				}
			}
		}

		// redo them all as a bulk replace
		if len(bulkOverwrite) > 0 {
			_, err := collection.BulkWrite(ctx, bulkOverwrite, options.BulkWrite().SetOrdered(false))
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to overwrite documents in collection: %v", err))
				return err
			}
		}
	}
	return nil
}
