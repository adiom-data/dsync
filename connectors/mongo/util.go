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

	done := make(chan struct{})
	// we need ANY event to get the resume token that we can use to extract the cluster time
	go func() {
		if err := insertDummyRecord(ctx, client); err != nil {
			slog.Error(fmt.Sprintf("Error inserting dummy record: %v", err.Error()))
		}
		close(done)
	}()

	changeStream.Next(ctx)
	resumeToken := changeStream.ResumeToken()
	if resumeToken == nil {
		return nil, fmt.Errorf("failed to get resume token from change stream")
	}

	<-done
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

func stringToQuery(queryStr string) (bson.D, error) {
	if queryStr == "" {
		return bson.D{}, nil
	}

	content := []byte(queryStr)

	var query bson.D
	err := bson.UnmarshalExtJSON(content, false, &query)
	if err != nil {
		return bson.D{}, fmt.Errorf("error parsing query as Extended JSON: %v", err)
	}
	return query, nil
}
