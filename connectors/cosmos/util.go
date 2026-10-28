/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/mitchellh/hashstructure"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	moptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	progressReportingIntervalSec = 10
)

// Cleanup failures are fatal to planning: continuing could copy the dummy
// document, and falling back would discard an already obtained resume token.
var errDummyRecordCleanup = errors.New("failed to remove resume-token dummy record")

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
// TODO: this should be just the hostname:port
func generateConnectorID(connectionString string) iface.ConnectorID {
	id, err := hashstructure.Hash(connectionString, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.ConnectorID(strconv.FormatUint(id, 16))
}

func getLatestResumeToken(ctx context.Context, client *mongo.Client, location iface.Location, withDelete bool) (resumeToken bson.Raw, retErr error) {
	slog.Debug(fmt.Sprintf("Getting latest resume token for location: %v\n", location))
	opts := moptions.ChangeStream().SetFullDocument(moptions.UpdateLookup)
	changeStream, err := createChangeStream(ctx, client, location, opts, withDelete)
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	// we need ANY event to get the resume token that we can use to extract the cluster time
	col := client.Database(location.Database).Collection(location.Collection)

	cmdResult := client.Database(location.Database).RunCommand(ctx, bson.D{{"customAction", "GetCollection"}, {"collection", location.Collection}})
	if cmdResult.Err() != nil {
		return nil, fmt.Errorf("Error retrieving possible shardKeyDefinition %v", cmdResult.Err())
	}
	var collectionInfo bson.Raw
	if err := cmdResult.Decode(&collectionInfo); err != nil {
		return nil, fmt.Errorf("error decoding possible shardKeyDefinition: %w", err)
	}
	shardKeyFields, err := extractShardKeyFields(collectionInfo)
	if err != nil {
		return nil, err
	}
	dummyInsertFilter := makeDummyDocument(shardKeyFields)
	slog.Debug("Dummy Insert", "filter", dummyInsertFilter)

	result, err := col.InsertOne(ctx, dummyInsertFilter)
	if err != nil {
		if isUnauthorizedWrite(err) {
			slog.Warn(fmt.Sprintf("Not authorized to insert dummy record for %v, skipping resume token retrieval - is the namespace read-only?", location))
			return nil, nil
		}
		return nil, fmt.Errorf("failed to insert dummy record: %w", err)
	}

	deleteFilter := makeDummyDeleteFilter(result.InsertedID, dummyInsertFilter, shardKeyFields)
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if _, deleteErr := col.DeleteOne(cleanupCtx, deleteFilter); deleteErr != nil {
			retErr = errors.Join(retErr, fmt.Errorf("%w for %s.%s: %w", errDummyRecordCleanup, location.Database, location.Collection, deleteErr))
		}
	}()

	// Get the resume token from the change stream event. The deferred cleanup
	// includes the partition-key values Cosmos requires when deleting it.
	if !changeStream.Next(ctx) {
		if err := changeStream.Err(); err != nil {
			return nil, fmt.Errorf("failed while waiting for dummy-record change event: %w", err)
		}
		return nil, fmt.Errorf("change stream ended before the dummy-record event")
	}
	resumeToken = changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}

	//print Rid for debugging purposes as we've seen Cosmos giving Rid mismatch errors
	rid, err := extractRidFromResumeToken(resumeToken)
	if err != nil {
		slog.Debug(fmt.Sprintf("Failed to extract Rid from resume token: %v", err))
	} else {
		slog.Debug(fmt.Sprintf("Rid for namespace %v: %v", location, rid))
	}

	return resumeToken, nil
}

func extractShardKeyFields(collectionInfo bson.Raw) ([]string, error) {
	value, err := collectionInfo.LookupErr("shardKeyDefinition")
	if err != nil {
		// Unpartitioned collections may omit the field entirely.
		return nil, nil
	}
	definition, ok := value.DocumentOK()
	if !ok {
		return nil, fmt.Errorf("shardKeyDefinition must be a BSON document, got %s", value.Type)
	}
	elements, err := definition.Elements()
	if err != nil {
		return nil, fmt.Errorf("invalid shardKeyDefinition: %w", err)
	}
	fields := make([]string, 0, len(elements))
	for _, element := range elements {
		if element.Key() != "_id" {
			fields = append(fields, element.Key())
		}
	}
	return fields, nil
}

func makeDummyDocument(shardKeyFields []string) bson.M {
	document := make(bson.M, len(shardKeyFields))
	for _, field := range shardKeyFields {
		document[field] = ""
	}
	return document
}

func makeDummyDeleteFilter(id interface{}, document bson.M, shardKeyFields []string) bson.M {
	filter := make(bson.M, len(shardKeyFields)+1)
	filter["_id"] = id
	for _, field := range shardKeyFields {
		filter[field] = document[field]
	}
	return filter
}

func isUnauthorizedWrite(err error) bool {
	var writeException mongo.WriteException
	if errors.As(err, &writeException) {
		for _, writeErr := range writeException.WriteErrors {
			if writeErr.Code == 13 {
				return true
			}
		}
	}
	var commandError mongo.CommandError
	return errors.As(err, &commandError) && commandError.Code == 13
}

// extractRidFromResumeToken extracts the Cosmos Resource Id (collection Id) from the resume token
func extractRidFromResumeToken(resumeToken bson.Raw) (string, error) {
	if err := resumeToken.Validate(); err != nil {
		return "", fmt.Errorf("invalid resume token BSON: %w", err)
	}
	_, data, ok := resumeToken.Lookup("_data").BinaryOK()
	if !ok {
		return "", fmt.Errorf("resume token _data must be BSON binary")
	}
	var payload struct {
		Rid string `json:"Rid"`
	}
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("failed to parse resume token from JSON: %w", err)
	}
	if payload.Rid == "" {
		return "", fmt.Errorf("resume token is missing Rid")
	}

	return payload.Rid, nil
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

// Get the continuation value from a change stream resume token
// Example format of _id._data.Data: {"V":2,"Rid":"nGERANjum1c=","Continuation":[{"FeedRange":{"type":"Effective Partition Key Range","value":{"min":"","max":"FF"}},"State":{"type":"continuation","value":"\"291514\""}}]}
func extractJSONChangeStreamContinuationValue(jsonBytes []byte) (int, error) {
	var result map[string]interface{}

	// Decode the JSON string
	err := json.Unmarshal(jsonBytes, &result)
	if err != nil {
		return 0, fmt.Errorf("error decoding JSON: %v", err)
	}
	// Extract the continuation value
	contArray := result["Continuation"].([]interface{})
	contTotal := 0
	for _, contValue := range contArray {
		contString := contValue.(map[string]interface{})["State"].(map[string]interface{})["value"].(string)
		cont, err := strconv.Atoi(strings.Trim(contString, `"`))
		if err != nil {
			return 0, fmt.Errorf("error converting continuation value to int: %v", err)
		}
		contTotal += cont
	}
	return contTotal, nil
}
