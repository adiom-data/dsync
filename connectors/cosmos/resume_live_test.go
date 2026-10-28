/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.akshayshah.org/memhttp"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Opt-in: creates an isolated 400 RU/s Cosmos collection and removes its entire
// test database afterward. Both connection strings are supplied only via env.
func TestCosmosLivePartitionedResume(t *testing.T) {
	uri, sinkURI := os.Getenv("COSMOS_RESUME_TEST_URI"), os.Getenv("COSMOS_RESUME_SINK_URI")
	if uri == "" || sinkURI == "" {
		t.Skip("set COSMOS_RESUME_TEST_URI and COSMOS_RESUME_SINK_URI for live Cosmos-to-Mongo validation")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	source := NewConn(ConnectorSettings{ConnectorSettings: mongoconn.ConnectorSettings{
		ConnectionString: uri, PingTimeout: 15 * time.Second, MaxPageSize: 2,
	}}).(*conn)
	t.Cleanup(source.Teardown)
	sink, err := mongoconn.NewConn(mongoconn.ConnectorSettings{ConnectionString: sinkURI, PingTimeout: 15 * time.Second})
	require.NoError(t, err)
	t.Cleanup(sink.(interface{ Teardown() }).Teardown)
	sinkDBClient, err := mongo.Connect(options.Client().ApplyURI(sinkURI))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sinkDBClient.Disconnect(context.Background()) })

	databaseName := "dsync_resume_validation_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	database := source.client.Database(databaseName)
	const collectionName = "partitioned"
	namespace := databaseName + "." + collectionName
	require.NoError(t, database.RunCommand(ctx, bson.D{{Key: "customAction", Value: "CreateDatabase"}}).Err())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		require.NoError(t, database.Drop(cleanupCtx), "remove the isolated Cosmos test database")
		require.NoError(t, sinkDBClient.Database(databaseName).Drop(cleanupCtx), "remove the isolated Mongo test database")
		t.Logf("Removed isolated test database %s from source and destination", databaseName)
	})
	t.Logf("Created isolated test database %s", databaseName)
	require.NoError(t, database.RunCommand(ctx, bson.D{
		{Key: "customAction", Value: "CreateCollection"},
		{Key: "collection", Value: collectionName},
		{Key: "shardKey", Value: "partitionKey"},
		{Key: "offerThroughput", Value: 400},
	}).Err())
	collection := database.Collection(collectionName)

	var info bson.M
	require.NoError(t, database.RunCommand(ctx, bson.D{
		{Key: "customAction", Value: "GetCollection"}, {Key: "collection", Value: collectionName},
	}).Decode(&info))
	require.IsType(t, bson.D{}, info["shardKeyDefinition"])
	t.Logf("Live shardKeyDefinition representation: %T", info["shardKeyDefinition"])

	_, err = collection.InsertOne(ctx, bson.M{"_id": "missing-partition-key"})
	require.ErrorContains(t, err, "shard key", "reproduce the original rejected dummy insert")
	t.Log("Confirmed Cosmos rejects the original dummy document without partitionKey")
	for i := 0; i < 3; i++ {
		_, err = collection.InsertOne(ctx, bson.M{"_id": fmt.Sprintf("seed-%d", i), "partitionKey": "tenant", "value": i})
		require.NoError(t, err)
	}

	location := iface.Location{Database: databaseName, Collection: collectionName}
	token, err := getLatestResumeToken(ctx, source.client, location, false)
	require.NoError(t, err)
	require.NotEmpty(t, token)
	count, err := collection.CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "dummy record must be removed")
	t.Log("Partition-aware dummy insert, resume-token retrieval, and cleanup succeeded")

	plan, err := source.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces: []string{namespace}, InitialSync: true, Updates: true,
	}))
	require.NoError(t, err)
	require.Len(t, plan.Msg.GetUpdatesPartitions(), 1)
	planCursor := plan.Msg.GetUpdatesPartitions()[0].GetCursor()
	epoch, encodedMap, err := decodeResumeToken(planCursor)
	require.NoError(t, err)
	tokens := NewTokenMap()
	require.NoError(t, tokens.decodeMap(encodedMap))
	planToken, err := tokens.GetToken(location)
	require.NoError(t, err)
	require.NotEmpty(t, planToken, "planning must obtain a token rather than silently falling back")
	count, err = collection.CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	require.Equal(t, int64(3), count, "planning must remove its dummy record")

	var copied int
	for _, partition := range plan.Msg.GetPartitions() {
		var cursor []byte
		for {
			page, err := source.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{
				Partition: partition, Cursor: cursor, Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
			}))
			require.NoError(t, err)
			if len(page.Msg.GetData()) > 0 {
				_, err = sink.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
					Namespace: namespace, Data: page.Msg.GetData(), Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				}))
				require.NoError(t, err)
				copied += len(page.Msg.GetData())
			}
			cursor = page.Msg.GetNextCursor()
			if len(cursor) == 0 {
				break
			}
		}
	}
	require.Equal(t, 3, copied)
	count, err = sinkDBClient.Database(databaseName).Collection(collectionName).CountDocuments(ctx, bson.D{})
	require.NoError(t, err)
	require.Equal(t, int64(3), count)
	t.Log("GeneratePlan and initial copy of 3 documents to MongoDB succeeded")

	_, httpHandler := adiomv1connect.NewConnectorServiceHandler(source)
	server, err := memhttp.New(httpHandler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Shutdown(context.Background()) })
	client := adiomv1connect.NewConnectorServiceClient(server.Client(), server.URL())
	emptyMap, err := NewTokenMap().encodeMap()
	require.NoError(t, err)
	fallbackCursor, err := encodeResumeToken(epoch, emptyMap)
	require.NoError(t, err)

	for _, test := range []struct {
		name   string
		cursor []byte
	}{
		{name: "resume_token", cursor: planCursor},
		{name: "timestamp_fallback", cursor: fallbackCursor},
	} {
		t.Run(test.name, func(t *testing.T) {
			streamCtx, streamCancel := context.WithTimeout(ctx, 30*time.Second)
			defer streamCancel()
			id := "cdc-" + test.name
			_, err := collection.InsertOne(streamCtx, bson.M{"_id": id, "partitionKey": "tenant", "value": test.name})
			require.NoError(t, err)
			stream, err := client.StreamUpdates(streamCtx, connect.NewRequest(&adiomv1.StreamUpdatesRequest{
				Namespaces: []string{namespace}, Cursor: test.cursor, Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
			}))
			require.NoError(t, err)
			defer stream.Close()
			found := false
			for stream.Receive() {
				message := stream.Msg()
				_, err = sink.WriteUpdates(streamCtx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
					Namespace: namespace, Updates: message.GetUpdates(), Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				}))
				require.NoError(t, err)
				for _, update := range message.GetUpdates() {
					if bson.Raw(update.GetData()).Lookup("_id").StringValue() == id {
						found = true
					}
				}
				if found {
					break
				}
			}
			require.True(t, found, "CDC marker not received: %v", stream.Err())
			var document bson.M
			require.NoError(t, sinkDBClient.Database(databaseName).Collection(collectionName).FindOne(streamCtx, bson.M{"_id": id}).Decode(&document))
			require.Equal(t, test.name, document["value"])

			lsnStream, err := client.StreamLSN(streamCtx, connect.NewRequest(&adiomv1.StreamLSNRequest{
				Namespaces: []string{namespace}, Cursor: test.cursor,
			}))
			require.NoError(t, err)
			defer lsnStream.Close()
			require.True(t, lsnStream.Receive(), "LSN stream did not emit: %v", lsnStream.Err())
			require.Positive(t, lsnStream.Msg().GetLsn())
			t.Log("StreamUpdates reached MongoDB and StreamLSN emitted progress")
		})
	}
}
