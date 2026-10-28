/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"
)

func resumePlanningResponses(deleteResponse bson.D) []bson.D {
	return []bson.D{
		{{Key: "ok", Value: 1}, {Key: "cursor", Value: bson.D{
			{Key: "id", Value: int64(0)}, {Key: "ns", Value: "db.col"},
			{Key: "firstBatch", Value: bson.A{bson.D{
				{Key: "_id", Value: bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{"Rid":"collection"}`)}}}},
			}}},
		}}},
		{{Key: "ok", Value: 1}, {Key: "shardKeyDefinition", Value: bson.D{{Key: "partitionKey", Value: "Hash"}}}},
		{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}, // dummy insert
		deleteResponse,
	}
}

func resumePlanningClient(t *testing.T, responses ...bson.D) *mongo.Client {
	t.Helper()
	opts := options.Client().SetRetryReads(false).SetRetryWrites(false)
	opts.Deployment = drivertest.NewMockDeployment(responses...)
	client, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Disconnect(context.Background())) })
	return client
}

func TestLatestResumeTokenReportsCleanupFailureWithValidToken(t *testing.T) {
	client := resumePlanningClient(t, resumePlanningResponses(bson.D{
		{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "dummy deletion denied"},
	})...)

	token, err := getLatestResumeToken(context.Background(), client, iface.Location{Database: "db", Collection: "col"}, false)
	require.NotEmpty(t, token, "the token was obtained before cleanup failed")
	require.ErrorIs(t, err, errDummyRecordCleanup)
	require.ErrorContains(t, err, "db.col")
	require.ErrorContains(t, err, "dummy deletion denied")
}

func TestGeneratePlanCleanupFailureIsFatal(t *testing.T) {
	for _, test := range []struct {
		name      string
		responses []bson.D
		fatal     bool
		hasToken  bool
	}{
		{
			name: "successful cleanup preserves token",
			responses: resumePlanningResponses(bson.D{
				{Key: "ok", Value: 1}, {Key: "n", Value: 1},
			}),
			hasToken: true,
		},
		{
			name: "failed cleanup rejects plan instead of falling back",
			responses: resumePlanningResponses(bson.D{
				{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "dummy deletion denied"},
			}),
			fatal: true,
		},
		{
			name: "unavailable token retains timestamp fallback",
			responses: []bson.D{{
				{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "change stream unavailable"},
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connector := &conn{
				client:            resumePlanningClient(t, test.responses...),
				connectorSettings: ConnectorSettings{MaxNumNamespaces: 1},
			}
			plan, err := connector.GeneratePlan(context.Background(), connect.NewRequest(&adiomv1.GeneratePlanRequest{
				Namespaces: []string{"db.col"}, Updates: true,
			}))
			if test.fatal {
				require.Nil(t, plan)
				require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
				require.ErrorIs(t, err, errDummyRecordCleanup)
				require.ErrorContains(t, err, "dummy deletion denied")
				return
			}
			require.NoError(t, err)
			require.Len(t, plan.Msg.GetUpdatesPartitions(), 1)
			epoch, encodedTokens, err := decodeResumeToken(plan.Msg.GetUpdatesPartitions()[0].GetCursor())
			require.NoError(t, err)
			require.Positive(t, epoch)
			tokens := NewTokenMap()
			require.NoError(t, tokens.decodeMap(encodedTokens))
			token, _ := tokens.GetToken(iface.Location{Database: "db", Collection: "col"})
			if test.hasToken {
				require.NotEmpty(t, token)
			} else {
				require.Empty(t, tokens.Map)
			}
		})
	}
}
