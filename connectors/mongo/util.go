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
	"regexp"
	"strconv"
	"strings"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/mitchellh/hashstructure"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

// startAtOperationTimeKey is a sentinel field we use to encode a
// primitive.Timestamp inside the opaque CDC cursor bytes. This allows us
// to distinguish a genuine change-stream resume token from a fallback
// "startAtOperationTime" marker used when the server (e.g. AWS DocumentDB
// 4.0) does not populate a resume token after an empty TryNext.
const startAtOperationTimeKey = "_adiomStartAtOperationTime"

func encodeStartAtOperationTime(ts primitive.Timestamp) (bson.Raw, error) {
	raw, err := bson.Marshal(bson.D{{Key: startAtOperationTimeKey, Value: ts}})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// decodeStartAtOperationTime returns the encoded operation time and true if
// the cursor is a sentinel-wrapped timestamp, otherwise returns zero and false.
func decodeStartAtOperationTime(cursor bson.Raw) (primitive.Timestamp, bool) {
	if len(cursor) == 0 {
		return primitive.Timestamp{}, false
	}
	v, err := cursor.LookupErr(startAtOperationTimeKey)
	if err != nil || v.Type != bson.TypeTimestamp {
		return primitive.Timestamp{}, false
	}
	t, i := v.Timestamp()
	return primitive.Timestamp{T: t, I: i}, true
}

// getClusterOperationTime retrieves the current cluster operationTime via the
// `hello` admin command. Used as a fallback when no resume token is available.
func getClusterOperationTime(ctx context.Context, client *mongo.Client) (primitive.Timestamp, error) {
	raw, err := client.Database("admin").RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Raw()
	if err != nil {
		return primitive.Timestamp{}, fmt.Errorf("failed to run hello: %w", err)
	}
	v, lerr := raw.LookupErr("operationTime")
	if lerr != nil {
		return primitive.Timestamp{}, fmt.Errorf("hello response missing operationTime: %w", lerr)
	}
	if v.Type != bson.TypeTimestamp {
		return primitive.Timestamp{}, fmt.Errorf("hello operationTime is not a Timestamp (type=%v)", v.Type)
	}
	t, i := v.Timestamp()
	return primitive.Timestamp{T: t, I: i}, nil
}

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

type Watchable interface {
	Watch(ctx context.Context, pipeline interface{}, opts ...*options.ChangeStreamOptions) (*mongo.ChangeStream, error)
}

// getLatestResumeToken opens a change stream on the given Watchable and tries
// to obtain a current resume token. When the server does not populate a resume
// token after TryNext (observed on AWS DocumentDB 4.0), it falls back to
// fetching the cluster operationTime via `hello` and returns a sentinel-wrapped
// timestamp that callers can later decode into ChangeStream's
// SetStartAtOperationTime option.
func getLatestResumeToken(ctx context.Context, client *mongo.Client, watchable Watchable) (bson.Raw, error) {
	slog.Debug("Getting latest resume token...")
	changeStream, err := watchable.Watch(ctx, mongo.Pipeline{})
	if err != nil {
		return nil, fmt.Errorf("failed to open change stream: %v", err)
	}
	defer changeStream.Close(ctx)
	_ = changeStream.TryNext(ctx)
	if token := changeStream.ResumeToken(); len(token) > 0 {
		return token, nil
	}
	if changeStream.Err() != nil {
		return nil, fmt.Errorf("failed to get a resume token: %w", changeStream.Err())
	}
	// Fallback: some servers (notably AWS DocumentDB 4.0) do not populate a
	// resume token after TryNext when the oplog is idle. Use the cluster
	// operationTime as a startAtOperationTime marker instead.
	ts, err := getClusterOperationTime(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to get a resume token and could not fall back to operationTime: %w", err)
	}
	slog.Warn("resume token empty; falling back to startAtOperationTime", "operationTime", ts)
	encoded, err := encodeStartAtOperationTime(ts)
	if err != nil {
		return nil, fmt.Errorf("failed to encode startAtOperationTime fallback: %w", err)
	}
	return encoded, nil
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

// COSMOS_DB_*_REGEX Checks if the MongoDB is genuine based on the connection string
var COSMOS_DB_RU_REGEX = regexp.MustCompile(`(?i)\.mongo\.cosmos\.azure\.com$`)
var COSMOS_DB_VCORE_REGEX = regexp.MustCompile(`(?i)\.mongocluster\.cosmos\.azure\.com$`)
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
	FlavorCosmosDB_RU    MongoFlavor = "COSMOS_RU"
	FlavorCosmosDB_VCORE MongoFlavor = "COSMOS_VCORE"
	FlavorDocumentDB     MongoFlavor = "DOCDB"
	FlavorMongoDB        MongoFlavor = "MONGODB"
)

// GetMongoFlavor returns the flavor of the MongoDB instance based on the connection string
func GetMongoFlavor(connectionString string) MongoFlavor {
	hostname := getHostnameFromUrl(connectionString)

	//check if the connection string matches the regex for Cosmos DB
	if COSMOS_DB_RU_REGEX.MatchString(hostname) {
		return FlavorCosmosDB_RU
	}

	if COSMOS_DB_VCORE_REGEX.MatchString(hostname) {
		return FlavorCosmosDB_VCORE
	}

	//check if the connection string matches the regex for Document DB
	if DOCUMENT_DB_REGEX.MatchString(hostname) {
		return FlavorDocumentDB
	}

	//default to MongoDB
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

func isServerErrorWithCode(err error, errorCode int) bool {
	if err == nil {
		return false
	}

	var serverError mongo.ServerError

	if !errors.As(err, &serverError) {
		return false
	}

	return serverError.HasErrorCode(errorCode)
}

// isBSONObjectTooLargeError checks if the error is due to a BSON document being too large
func isBSONObjectTooLargeError(err error) bool {
	const BSONObjectTooLarge = 10334
	return isServerErrorWithCode(err, BSONObjectTooLarge)
}
