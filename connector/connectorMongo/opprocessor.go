/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorMongo

import (
	"fmt"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
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
		_, err := collection.InsertOne(mc.ctx, bson.Raw(data)) //TODO (AK, 6/2024): For this and other inserts we need to handle duplicate key exceptions
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
			_, err := collection.InsertMany(mc.ctx, dataBatchBson)
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
				_, err := collection.InsertMany(mc.ctx, batchPart)
				if err != nil {
					slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
					return err
				}
				batchSizeLeft -= mc.settings.writerMaxBatchSize
				idx += mc.settings.writerMaxBatchSize
			}
			if batchSizeLeft > 0 {
				slog.Debug(fmt.Sprintf("Inserting subbatch(tail-end) of %v documents (idx %v) into collection %v.%v", len(dataBatchBson[idx:]), idx, dbName, colName))
				_, err := collection.InsertMany(mc.ctx, dataBatchBson[idx:])
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
