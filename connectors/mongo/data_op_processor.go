/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// TODO: this needs to be synchronized with the actual processing of the data messages
func (mc *BaseMongoConnector) HandleBarrierMessage(barrierMsg iface.DataMessage) error {
	switch barrierMsg.BarrierType {
	case iface.BarrierType_TaskComplete:
		// notify the coordinator that the task is done from our side
		if err := mc.Coord.NotifyTaskDone(mc.FlowId, mc.ID, (iface.ReadPlanTaskID)(barrierMsg.BarrierTaskId), nil); err != nil {
			return err
		}
		return nil
	case iface.BarrierType_CdcResumeTokenUpdate:
		// notify the coordinator that the task is done from our side
		mc.Coord.UpdateCDCResumeToken(mc.FlowId, mc.ID, barrierMsg.BarrierCdcResumeToken)
		if err := mc.Coord.UpdateCDCResumeToken(mc.FlowId, mc.ID, barrierMsg.BarrierCdcResumeToken); err != nil {
			return err
		}
		return nil
	}

	return nil
}

type dataMsgIdIndex struct {
	dataMsgId []byte
	index     int
}

// returns the new item or existing item, and whether or not a new item was added
func addToIdIndexMap(m map[int][]*dataMsgIdIndex, dataMsg iface.DataMessage) (*dataMsgIdIndex, bool) {
	h := hashDataMsgId(dataMsg)
	items, found := m[h]
	if found {
		for _, item := range items {
			if slices.Equal(item.dataMsgId, *dataMsg.Id) {
				return item, false
			}
		}
	}
	item := &dataMsgIdIndex{*dataMsg.Id, -1}
	m[h] = append(items, item)
	return item, true
}

// ProcesDataMessages assumes all have the same Database and Collection
func (mc *BaseMongoConnector) ProcessDataMessages(dataMsgs []iface.DataMessage) error {
	if len(dataMsgs) == 0 {
		return nil
	}
	dataMsg := dataMsgs[0]
	dbName := dataMsg.Loc.Database
	colName := dataMsg.Loc.Collection
	maxSeqNum := max(dataMsg.SeqNum, mc.Status.WriteLSN)

	collection := mc.Client.Database(dbName).Collection(colName)

	var models []mongo.WriteModel
	// keeps track of the index in models for a particular document because last write wins
	hashToDataMsgIdIndex := map[int][]*dataMsgIdIndex{}

	for _, dataMsg := range dataMsgs {
		idType := bsontype.Type(dataMsg.IdType)
		maxSeqNum = max(maxSeqNum, dataMsg.SeqNum)

		switch dataMsg.MutationType {
		case iface.MutationType_Insert, iface.MutationType_Update:
			dmii, isNew := addToIdIndexMap(hashToDataMsgIdIndex, dataMsg)
			idFilter := bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}}
			data := *dataMsg.Data
			model := mongo.NewReplaceOneModel().SetFilter(idFilter).SetReplacement(bson.Raw(data)).SetUpsert(true)
			if isNew {
				dmii.index = len(models)
				models = append(models, model)
			} else {
				models[dmii.index] = model
			}
		case iface.MutationType_Delete:
			dmii, isNew := addToIdIndexMap(hashToDataMsgIdIndex, dataMsg)
			idFilter := bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}}
			model := mongo.NewDeleteOneModel().SetFilter(idFilter)
			if isNew {
				dmii.index = len(models)
				models = append(models, model)
			} else {
				models[dmii.index] = model
			}
		case iface.MutationType_InsertBatch:
			// Do nothing because we will handle this with a legacy method
		default:
			slog.Error(fmt.Sprintf("unsupported operation type during batch: %v", dataMsg.MutationType))
		}

		if (mc.Settings.WriterMaxBatchSize > 0 && len(models) >= mc.Settings.WriterMaxBatchSize) || (dataMsg.MutationType == iface.MutationType_InsertBatch && len(models) > 0) {
			_, err := collection.BulkWrite(mc.Ctx, models, options.BulkWrite().SetOrdered(false))
			if err != nil {
				slog.Error(fmt.Sprintf("Failed batch of %v documents into collection %v.%v", len(models[:mc.Settings.WriterMaxBatchSize]), dbName, colName))
				return err
			}
			mc.Status.WriteLSN = maxSeqNum //XXX (AK, 6/2024): this is just a placeholder for now that won't work well if things are processed out of order or if they are parallelized

			models = nil
			hashToDataMsgIdIndex = map[int][]*dataMsgIdIndex{}
		}

		// Specially process an insert batch due to some optimizations
		if dataMsg.MutationType == iface.MutationType_InsertBatch {
			err := mc.legacyProcessDataMessage(dataMsg)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to process embedded batch: %v", err))
				return err
			}
		}
	}

	if len(models) > 0 {
		_, err := collection.BulkWrite(mc.Ctx, models, options.BulkWrite().SetOrdered(false))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed batch of %v documents into collection %v.%v", len(models), dbName, colName))
			return err
		}
		mc.Status.WriteLSN = maxSeqNum //XXX (AK, 6/2024): this is just a placeholder for now that won't work well if things are processed out of order or if they are parallelized
	}

	return nil
}

// legacyProcessDataMessage has an optimization for batched inserts
func (mc *BaseMongoConnector) legacyProcessDataMessage(dataMsg iface.DataMessage) error {
	dbName := dataMsg.Loc.Database
	colName := dataMsg.Loc.Collection

	collection := mc.Client.Database(dbName).Collection(colName)

	switch dataMsg.MutationType {
	case iface.MutationType_Insert:
		data := *dataMsg.Data
		err := insertBatchOverwrite(mc.Ctx, collection, []interface{}{bson.Raw(data)})
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
		// we split in subbatches of mc.Settings.WriterMaxBatchSize if it's not 0
		if (mc.Settings.WriterMaxBatchSize <= 0) || (len(dataBatch) <= mc.Settings.WriterMaxBatchSize) {
			err := insertBatchOverwrite(mc.Ctx, collection, dataBatchBson)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
				return err
			}
		} else {
			slog.Debug(fmt.Sprintf("Splitting the batch because it's bigger than max size set: %v", mc.Settings.WriterMaxBatchSize))
			batchSizeLeft := len(dataBatch)
			idx := 0
			for batchSizeLeft > mc.Settings.WriterMaxBatchSize {
				slog.Debug(fmt.Sprintf("Inserting subbatch of %v documents (idx %v) into collection %v.%v", mc.Settings.WriterMaxBatchSize, idx, dbName, colName))
				batchPart := dataBatchBson[idx : idx+mc.Settings.WriterMaxBatchSize]
				err := insertBatchOverwrite(mc.Ctx, collection, batchPart)
				if err != nil {
					slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
					return err
				}
				batchSizeLeft -= mc.Settings.WriterMaxBatchSize
				idx += mc.Settings.WriterMaxBatchSize
			}
			if batchSizeLeft > 0 {
				slog.Debug(fmt.Sprintf("Inserting subbatch(tail-end) of %v documents (idx %v) into collection %v.%v", len(dataBatchBson[idx:]), idx, dbName, colName))
				err := insertBatchOverwrite(mc.Ctx, collection, dataBatchBson[idx:])
				if err != nil {
					slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
					return err
				}
			}
		}
	case iface.MutationType_Update:
		idType := bsontype.Type(dataMsg.IdType)
		data := *dataMsg.Data
		opts := options.Replace().SetUpsert(true) //compatibility with Cosmos, all change stream events are generalized to upserts
		_, err := collection.ReplaceOne(mc.Ctx, bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}}, bson.Raw(data), opts)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to update document in the collection: %v", err))
			return err
		}
	case iface.MutationType_Delete:
		idType := bsontype.Type(dataMsg.IdType)
		_, err := collection.DeleteOne(mc.Ctx, bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to delete document from collection: %v", err))
			return err
		}
	default:
		return fmt.Errorf("unsupported operation type: %v", dataMsg.MutationType)
	}

	mc.Status.WriteLSN = max(dataMsg.SeqNum, mc.Status.WriteLSN) //XXX (AK, 6/2024): this is just a placeholder for now that won't work well if things are processed out of order or if they are parallelized

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

		// check if it's a bulk write exception
		var bwErrWriteErrors mongo.BulkWriteException
		if errors.As(bwErr, &bwErrWriteErrors) {
			for _, we := range bwErrWriteErrors.WriteErrors {
				if mongo.IsDuplicateKeyError(we.WriteError) {
					doc := documents[we.Index]
					id := doc.(bson.Raw).Lookup("_id") //we know it's there because there was a conflict on _id //XXX: should we check that it's the right type?
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
