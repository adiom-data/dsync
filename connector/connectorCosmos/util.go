/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package connectorCosmos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
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
	tasksStarted       uint64
	tasksCompleted     uint64
	deletesCaught      uint64
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
			if !cc.settings.EmulateDeletes {
				slog.Info(fmt.Sprintf("Reader Progress: Initial Sync Docs - %d (%d/%d tasks completed), Change Stream Events - %d, Operations per Second - %.2f",
					readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted, readerProgress.tasksTotal, readerProgress.changeStreamEvents, opsPerSec))
			} else {
				slog.Info(fmt.Sprintf("Reader Progress: Initial Sync Docs - %d (%d/%d tasks completed), Change Stream Events - %d, Deletes - %d, Operations per Second - %.2f",
					readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted, readerProgress.tasksTotal, readerProgress.changeStreamEvents, readerProgress.deletesCaught, opsPerSec))

			}

			startTime = time.Now()
			operations = readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
		}
	}
}

func (cc *CosmosConnector) getLatestResumeToken(ctx context.Context, location iface.Location) (bson.Raw, error) {
	slog.Debug(fmt.Sprintf("Getting latest resume token for location: %v\n", location))
	opts := moptions.ChangeStream().SetFullDocument(moptions.UpdateLookup)
	changeStream, err := cc.createChangeStream(ctx, location, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	// we need ANY event to get the resume token that we can use to extract the cluster time
	var id interface{}
	col := cc.client.Database(location.Database).Collection(location.Collection)

	result, err := col.InsertOne(ctx, bson.M{})
	if err != nil {
		slog.Error(fmt.Sprintf("Error inserting dummy record: %v", err.Error()))
		return nil, fmt.Errorf("failed to insert dummy record")
	}

	id = result.InsertedID
	//get the resume token from the change stream event, then delete the inserted document
	changeStream.Next(ctx)
	resumeToken := changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}
	col.DeleteOne(ctx, bson.M{"_id": id})

	//print Rid for debugging purposes as we've seen Cosmos giving Rid mismatch errors
	rid, err := extractRidFromResumeToken(resumeToken)
	if err != nil {
		slog.Debug(fmt.Sprintf("Failed to extract Rid from resume token: %v", err))
	} else {
		slog.Debug(fmt.Sprintf("Rid for namespace %v: %v", location, rid))
	}

	return resumeToken, nil
}

// extractRidFromResumeToken extracts the Cosmos Resource Id (collection Id) from the resume token
func extractRidFromResumeToken(resumeToken bson.Raw) (string, error) {
	data := resumeToken.Lookup("_data").Value[5:] //Skip the first 5 bytes because it's some Cosmic garbage

	var keyJsonMap map[string]interface{}
	err := json.Unmarshal(data, &keyJsonMap)
	if err != nil {
		return "", fmt.Errorf("failed to parse resume token from JSON: %v", err)
	}

	return fmt.Sprintf("%v", keyJsonMap["Rid"]), nil
}

// update LSN and changeStreamEvents counters atomically, returns the updated WriteLSN value after incrementing to use as the SeqNum
func (cc *CosmosConnector) updateLSNTracking(reader *ReaderProgress, lsn *int64) int64 {
	cc.muProgressMetrics.Lock()
	defer cc.muProgressMetrics.Unlock()
	reader.changeStreamEvents++
	*lsn++
	cc.status.WriteLSN++
	cc.status.ProgressMetrics.ChangeStreamEvents++
	return cc.status.WriteLSN
}

// create a find query for a task
func createFindQuery(ctx context.Context, collection *mongo.Collection, task iface.ReadPlanTask) (cur *mongo.Cursor, err error) {
	if task.Def.Low == nil && task.Def.High == nil { //no boundaries

		return collection.Find(ctx, bson.D{})
	} else if task.Def.Low == nil && task.Def.High != nil { //only upper boundary
		if task.Def.PartitionKey == "" {
			return nil, fmt.Errorf("Invalid task definition: %v", task)
		}

		return collection.Find(ctx, bson.D{
			{task.Def.PartitionKey, bson.D{
				{"$lt", task.Def.High},
			}},
		})
	} else if task.Def.Low != nil && task.Def.High == nil { //only lower boundary
		if task.Def.PartitionKey == "" {
			return nil, fmt.Errorf("Invalid task definition: %v", task)
		}

		return collection.Find(ctx, bson.D{
			{task.Def.PartitionKey, bson.D{
				{"$gte", task.Def.Low},
			}},
		})
	} else { //both boundaries
		if task.Def.PartitionKey == "" {
			return nil, fmt.Errorf("Invalid task definition: %v", task)
		}

		return collection.Find(ctx, bson.D{
			{task.Def.PartitionKey, bson.D{
				{"$gte", task.Def.Low},
				{"$lt", task.Def.High},
			}},
		})
	}
}

func nsToString(ns iface.Namespace) string {
	return fmt.Sprintf("%s.%s", ns.Db, ns.Col)
}

// restoreProgressDetails restores the progress metrics from the persisted tasks and progress
func (cc *CosmosConnector) restoreProgressDetails(tasks []iface.ReadPlanTask) { //XXX: can parallelize this
	slog.Debug("Restoring progress metrics from tasks")
	cc.status.ProgressMetrics.TasksTotal = int64(len(tasks))
	for _, task := range tasks {
		ns := iface.Namespace{Db: task.Def.Db, Col: task.Def.Col}
		nsStatus := cc.status.ProgressMetrics.NamespaceProgress[ns]
		//check if the namespace status exists, if not create it
		if nsStatus == nil {
			nsStatus = &iface.NamespaceStatus{
				EstimatedDocCount:   0,
				Throughput:          0,
				Tasks:               []iface.ReadPlanTask{},
				TasksCompleted:      0,
				TasksStarted:        0,
				DocsCopied:          0,
				EstimatedDocsCopied: 0,
			}
			cc.status.ProgressMetrics.NamespaceProgress[ns] = nsStatus
		}
		nsStatus.Tasks = append(nsStatus.Tasks, task)
		nsStatus.EstimatedDocCount += task.EstimatedDocCount
		//if the task is completed, update the document counters
		if task.Status == iface.ReadPlanTaskStatus_Completed {
			cc.status.ProgressMetrics.TasksCompleted++
			cc.status.ProgressMetrics.NumDocsSynced += task.DocsCopied

			nsStatus.TasksCompleted++
			nsStatus.DocsCopied += task.DocsCopied

			nsStatus.EstimatedDocsCopied += task.EstimatedDocCount
			slog.Debug(fmt.Sprintf("totalDocsCopied: %v, ns docs copied: %v", cc.status.ProgressMetrics.NumDocsSynced, nsStatus.DocsCopied))
		}
	}
	cc.status.ProgressMetrics.NumNamespaces = int64(len(cc.status.ProgressMetrics.NamespaceProgress))

	for ns, nsStatus := range cc.status.ProgressMetrics.NamespaceProgress {
		if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
			cc.status.ProgressMetrics.NumNamespacesCompleted++
		}
		cc.status.ProgressMetrics.Namespaces = append(cc.status.ProgressMetrics.Namespaces, ns)
	}

	slog.Debug(fmt.Sprintf("Restored progress metrics: %+v", cc.status.ProgressMetrics))

}

// Updates the progress metrics once a task has been completed
func (cc *CosmosConnector) taskDoneProgressUpdate(nsStatus *iface.NamespaceStatus, task iface.ReadPlanTask) *iface.TaskDoneMeta {
	cc.muProgressMetrics.Lock()
	//update progress counters: num tasks completed
	cc.status.ProgressMetrics.TasksCompleted++
	nsStatus.TasksCompleted++
	//update the estimated docs copied count for the namespace to keep percentage proportional
	nsStatus.EstimatedDocsCopied = int64(math.Max(float64(nsStatus.EstimatedDocsCopied), float64(nsStatus.TasksCompleted*cc.settings.targetDocCountPerPartition)))
	//check if namespace has been completed
	if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
		cc.status.ProgressMetrics.NumNamespacesCompleted++
	}
	taskData := &iface.TaskDoneMeta{DocsCopied: task.DocsCopied}
	cc.muProgressMetrics.Unlock()
	return taskData
}

// Updates the progress metrics once a task has been started
func (cc *CosmosConnector) taskInProgressUpdate(nsStatus *iface.NamespaceStatus, task iface.ReadPlanTask) {
	cc.muProgressMetrics.Lock()
	nsStatus.DocsCopied++
	nsStatus.EstimatedDocsCopied++
	cc.status.ProgressMetrics.NumDocsSynced++
	task.DocsCopied++
	cc.muProgressMetrics.Unlock()
}
