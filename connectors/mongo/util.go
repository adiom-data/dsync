/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/mitchellh/hashstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

//XXX (AK, 6/2024): this is not going to work on anything but a dedicated Mongo cluster
/*
func getLastOpTime(ctx context.Context, client *mongo.Client) (*primitive.Timestamp, error) {
	appendOplogNoteCmd := bson.D{
		{"appendOplogNote", 1},
		{"data", bson.D{{"adiom-connector", "lastOpTime"}}},
	}
	res := client.Database("admin").RunCommand(ctx, appendOplogNoteCmd)

	var responseRaw bson.Raw
	var err error
	if responseRaw, err = res.Raw(); err != nil {
		return nil, fmt.Errorf("failed to append oplog note: %v", err)
	}

	opTimeRaw, lookupErr := responseRaw.LookupErr("operationTime")
	if lookupErr != nil {
		return nil, fmt.Errorf("failed to get operationTime from appendOplogNote response: %v", lookupErr)
	}

	t, i := opTimeRaw.Timestamp()
	return &primitive.Timestamp{T: t, I: i}, nil
}
*/

const (
	connectorDBType string = "MongoDB" // We're a MongoDB-compatible connector
	connectorSpec   string = "Genuine"
	// specific, not compatible with Cosmos DB
	dummyDB                      string = "adiom-internal-dummy" //note that this must be different from the metadata DB - that one is excluded from copying, while this one isn't
	dummyCol                     string = "dummy"
	progressReportingIntervalSec        = 10
)

func insertDummyRecord(ctx context.Context, client *mongo.Client) error {
	//set id to a string client address (to make it random)
	id := fmt.Sprintf("%v", client)
	col := client.Database(dummyDB).Collection(dummyCol)
	_, err := col.InsertOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("failed to insert dummy record: %v", err)
	}
	//delete the record
	_, err = col.DeleteOne(ctx, bson.M{"_id": id})
	if err != nil {
		return fmt.Errorf("failed to delete dummy record: %v", err)
	}

	return nil
}

func getLatestResumeToken(ctx context.Context, client *mongo.Client) (bson.Raw, error) {
	slog.Debug("Getting latest resume token...")
	changeStream, err := client.Watch(ctx, mongo.Pipeline{}) //TODO (AK, 6/2024): We should limit this to just the dummy collection or we can catch something that we don't want :)
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	// we need ANY event to get the resume token that we can use to extract the cluster time
	go func() {
		if err := insertDummyRecord(ctx, client); err != nil {
			slog.Error(fmt.Sprintf("Error inserting dummy record: %v", err.Error()))
		}
	}()

	changeStream.Next(ctx)
	resumeToken := changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}

	return resumeToken, nil
}

// Generates static connector ID based on connection string
// XXX: is this the best place to do this? - move to overall connector util file
// TODO: this should be just the hostname:port
func generateConnectorID(connectionString string) iface.ConnectorID {
	id, err := hashstructure.Hash(connectionString, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.ConnectorID(strconv.FormatUint(id, 16))
}

// COSMOS_DB_REGEX Checks if the MongoDB is genuine based on the connection string
var COSMOS_DB_REGEX = regexp.MustCompile(`(?i)\.cosmos\.azure\.com$`)
var DOCUMENT_DB_REGEX = regexp.MustCompile(`(?i)docdb(-elastic)?\.amazonaws\.com$`)

func getHostnameFromHost(host string) string {
	if strings.HasPrefix(host, "[") {
		// If it's ipv6 return what's in the brackets.
		return strings.Split(strings.TrimPrefix(host, "["), "]")[0]
	}
	return strings.Split(host, ":")[0]
}

func getHostnameFromUrl(url string) string {
	var host string

	connString, err := connstring.Parse(url)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to parse connection string: %v", err))
		host = url //assume it's a hostname
	} else {
		host = connString.Hosts[0]
	}

	return getHostnameFromHost(host)
}

type MongoFlavor string

const (
	FlavorCosmosDB   MongoFlavor = "COSMOS"
	FlavorDocumentDB MongoFlavor = "DOCDB"
	FlavorMongoDB    MongoFlavor = "MONGODB"
)

// GetMongoFlavor returns the flavor of the MongoDB instance based on the connection string
func GetMongoFlavor(connectionString string) MongoFlavor {
	hostname := getHostnameFromUrl(connectionString)

	//check if the connection string matches the regex for Cosmos DB
	if COSMOS_DB_REGEX.MatchString(hostname) {
		return FlavorCosmosDB
	}
	//check if the connection string matches the regex for Document DB
	if DOCUMENT_DB_REGEX.MatchString(hostname) {
		return FlavorDocumentDB
	}
	return FlavorMongoDB
}

// restoreProgressDetails restores the progress metrics from the persisted tasks and progress
func (mc *Connector) restoreProgressDetails(tasks []iface.ReadPlanTask) { 
	slog.Debug("Restoring progress metrics from tasks")
	mc.status.ProgressMetrics.TasksTotal = int64(len(tasks))
	for _, task := range tasks {
		ns := iface.Namespace{Db: task.Def.Db, Col: task.Def.Col}
		nsStatus := mc.status.ProgressMetrics.NamespaceProgress[ns]
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
				ActiveTasksList:     make(map[iface.ReadPlanTaskID]bool),
			}
			mc.status.ProgressMetrics.NamespaceProgress[ns] = nsStatus
		}
		nsStatus.Tasks = append(nsStatus.Tasks, task)
		nsStatus.EstimatedDocCount += task.EstimatedDocCount
		//if the task is completed, update the document counters
		if task.Status == iface.ReadPlanTaskStatus_Completed {
			mc.status.ProgressMetrics.TasksCompleted++
			mc.status.ProgressMetrics.NumDocsSynced += task.DocsCopied

			nsStatus.TasksCompleted++
			nsStatus.DocsCopied += task.DocsCopied

			nsStatus.EstimatedDocsCopied += task.EstimatedDocCount
		}
	}
	mc.status.ProgressMetrics.NumNamespaces = int64(len(mc.status.ProgressMetrics.NamespaceProgress))

	for ns, nsStatus := range mc.status.ProgressMetrics.NamespaceProgress {
		if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
			mc.status.ProgressMetrics.NumNamespacesCompleted++
		}
		mc.status.ProgressMetrics.Namespaces = append(mc.status.ProgressMetrics.Namespaces, ns)
	}

	slog.Debug(fmt.Sprintf("Restored progress metrics: %+v", mc.status.ProgressMetrics))
}

// update estimated namespace doc counts from the actual database
func (mc *Connector) resetNsProgressEstimatedDocCounts() error {
	for ns, nsStatus := range mc.status.ProgressMetrics.NamespaceProgress {
		collection := mc.client.Database(ns.Db).Collection(ns.Col)
		count, err := collection.EstimatedDocumentCount(mc.ctx)
		if err != nil {
			return fmt.Errorf("failed to count documents: %v", err)
		}
		nsStatus.EstimatedDocCount = int64(count)
	}
	return nil
}

// Updates the progress metrics once a task has been started
func (mc *Connector) taskStartedProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	mc.muProgressMetrics.Lock()
	nsStatus.ActiveTasksList[taskId] = true
	mc.status.ProgressMetrics.TasksStarted++
	nsStatus.TasksStarted++
	mc.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been started
func (mc *Connector) taskInProgressUpdate(nsStatus *iface.NamespaceStatus) {
	mc.muProgressMetrics.Lock()
	nsStatus.DocsCopied++
	nsStatus.EstimatedDocsCopied++
	mc.status.ProgressMetrics.NumDocsSynced++
	mc.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been completed
func (mc *Connector) taskDoneProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	mc.muProgressMetrics.Lock()
	// update progress counters: num tasks completed
	mc.status.ProgressMetrics.TasksCompleted++
	nsStatus.TasksCompleted++

	// go through all the tasks
	// - mark ours as completed
	// - calculate the approximate number of docs copied based on per-task estimates
	approxDocsCopied := int64(0)
	for i, task := range nsStatus.Tasks {
		if task.Id == taskId {
			nsStatus.Tasks[i].Status = iface.ReadPlanTaskStatus_Completed
		}
		if task.Status == iface.ReadPlanTaskStatus_Completed {
			approxDocsCopied += task.EstimatedDocCount
		}
	}
	// update the estimated docs copied count for the namespace to keep percentage proportional
	nsStatus.EstimatedDocsCopied = int64(math.Max(float64(nsStatus.EstimatedDocsCopied), float64(approxDocsCopied)))
	// check if namespace has been completed
	if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
		mc.status.ProgressMetrics.NumNamespacesCompleted++
	}
	// decrement the tasks started counter
	mc.status.ProgressMetrics.TasksStarted--
	nsStatus.TasksStarted--
	// remove the task from the active tasks list
	delete(nsStatus.ActiveTasksList, taskId)
	mc.muProgressMetrics.Unlock()
}