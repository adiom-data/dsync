/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"

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
// TODO: this should be just the hostname:port
func generateConnectorID(connectionString string) iface.ConnectorID {
	id, err := hashstructure.Hash(connectionString, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.ConnectorID(strconv.FormatUint(id, 16))
}

func getLatestResumeToken(ctx context.Context, client *mongo.Client, location iface.Location, withDelete bool) (bson.Raw, error) {
	slog.Debug(fmt.Sprintf("Getting latest resume token for location: %v\n", location))
	opts := moptions.ChangeStream().SetFullDocument(moptions.UpdateLookup)
	changeStream, err := createChangeStream(ctx, client, location, opts, withDelete)
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(ctx)

	// we need ANY event to get the resume token that we can use to extract the cluster time
	var id interface{}
	col := client.Database(location.Database).Collection(location.Collection)

	cmdResult := client.Database(location.Database).RunCommand(ctx, bson.D{{"customAction", "GetCollection"}, {"collection", location.Collection}})
	if cmdResult.Err() != nil {
		return nil, fmt.Errorf("Error retrieving possible shardKeyDefinition %v", cmdResult.Err())
	}
	var m map[string]interface{}
	if err := cmdResult.Decode(&m); err != nil {
		return nil, fmt.Errorf("Error decoding possible shardKeyDefinition %v", cmdResult.Err())
	}
	dummyInsertFilter := bson.M{}
	if shardKeyDefinition, ok := m["shardKeyDefinition"]; ok {
		if shardKeyDefinitionMap, ok := shardKeyDefinition.(map[string]interface{}); ok {
			for k := range shardKeyDefinitionMap {
				if k == "_id" {
					continue
				}
				dummyInsertFilter[k] = ""
			}
		}
	}
	slog.Debug("Dummy Insert", "filter", dummyInsertFilter)

	result, err := col.InsertOne(ctx, dummyInsertFilter)
	if err != nil {
		if err.(mongo.WriteException).WriteErrors[0].Code == 13 { //unauthorized
			slog.Warn(fmt.Sprintf("Not authorized to insert dummy record for %v, skipping resume token retrieval - is the namespace read-only?", location))
			return nil, nil
		}
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
	if _, err := col.DeleteOne(ctx, bson.M{"_id": id}); err != nil {
		return nil, err
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
