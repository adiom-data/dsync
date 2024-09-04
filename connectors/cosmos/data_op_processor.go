/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
 package cosmos

 import (
	 "context"
	 "errors"
	 "fmt"
	 "log/slog"
 
	 "github.com/adiom-data/dsync/protocol/iface"
	 "go.mongodb.org/mongo-driver/bson"
	 "go.mongodb.org/mongo-driver/bson/bsontype"
	 "go.mongodb.org/mongo-driver/mongo"
	 "go.mongodb.org/mongo-driver/mongo/options"
 )
 
 // TODO: this needs to be synchronized with the actual processing of the data messages
 func (cc *Connector) handleBarrierMessage(barrierMsg iface.DataMessage) error {
	 switch barrierMsg.BarrierType {
	 case iface.BarrierType_TaskComplete:
		 // notify the coordinator that the task is done from our side
		 if err := cc.coord.NotifyTaskDone(cc.flowId, cc.id, (iface.ReadPlanTaskID)(barrierMsg.BarrierTaskId), nil); err != nil {
			 return err
		 }
		 return nil
	 case iface.BarrierType_CdcResumeTokenUpdate:
		 // notify the coordinator that the task is done from our side
		 cc.coord.UpdateCDCResumeToken(cc.flowId, cc.id, barrierMsg.BarrierCdcResumeToken)
		 if err := cc.coord.UpdateCDCResumeToken(cc.flowId, cc.id, barrierMsg.BarrierCdcResumeToken); err != nil {
			 return err
		 }
		 return nil
	 }
 
	 return nil
 }
 
 func (cc *Connector) processDataMessage(dataMsg iface.DataMessage) error {
	 dbName := dataMsg.Loc.Database
	 colName := dataMsg.Loc.Collection
 
	 collection := cc.client.Database(dbName).Collection(colName)
 
	 switch dataMsg.MutationType {
	 case iface.MutationType_Insert:
		 data := *dataMsg.Data
		 err := insertBatchOverwrite(cc.ctx, collection, []interface{}{bson.Raw(data)})
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
		 // we split in subbatches of cc.settings.writerMaxBatchSize if it's not 0
		 if (cc.settings.WriterMaxBatchSize <= 0) || (len(dataBatch) <= cc.settings.WriterMaxBatchSize) {
			 err := insertBatchOverwrite(cc.ctx, collection, dataBatchBson)
			 if err != nil {
				 slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
				 return err
			 }
		 } else {
			 slog.Debug(fmt.Sprintf("Splitting the batch because it's bigger than max size set: %v", cc.settings.WriterMaxBatchSize))
			 batchSizeLeft := len(dataBatch)
			 idx := 0
			 for batchSizeLeft > cc.settings.WriterMaxBatchSize {
				 slog.Debug(fmt.Sprintf("Inserting subbatch of %v documents (idx %v) into collection %v.%v", cc.settings.WriterMaxBatchSize, idx, dbName, colName))
				 batchPart := dataBatchBson[idx : idx+cc.settings.WriterMaxBatchSize]
				 err := insertBatchOverwrite(cc.ctx, collection, batchPart)
				 if err != nil {
					 slog.Error(fmt.Sprintf("Failed inserting documents into collection: %v", err))
					 return err
				 }
				 batchSizeLeft -= cc.settings.WriterMaxBatchSize
				 idx += cc.settings.WriterMaxBatchSize
			 }
			 if batchSizeLeft > 0 {
				 slog.Debug(fmt.Sprintf("Inserting subbatch(tail-end) of %v documents (idx %v) into collection %v.%v", len(dataBatchBson[idx:]), idx, dbName, colName))
				 err := insertBatchOverwrite(cc.ctx, collection, dataBatchBson[idx:])
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
		 _, err := collection.ReplaceOne(cc.ctx, bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}}, bson.Raw(data), opts)
		 if err != nil {
			 slog.Error(fmt.Sprintf("Failed to update document in the collection: %v", err))
			 return err
		 }
	 case iface.MutationType_Delete:
		 idType := bsontype.Type(dataMsg.IdType)
		 _, err := collection.DeleteOne(cc.ctx, bson.D{{Key: "_id", Value: bson.RawValue{Type: idType, Value: *dataMsg.Id}}})
		 if err != nil {
			 slog.Error(fmt.Sprintf("Failed to delete document from collection: %v", err))
			 return err
		 }
	 default:
		 return fmt.Errorf("unsupported operation type: %v", dataMsg.MutationType)
	 }
 
	 cc.status.WriteLSN = max(dataMsg.SeqNum, cc.status.WriteLSN) //XXX (AK, 6/2024): this is just a placeholder for now that won't work well if things are processed out of order or if they are parallelized
 
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
 