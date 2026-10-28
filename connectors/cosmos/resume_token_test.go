/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"math"
	"testing"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	moptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func TestResumeCursorEmptyTokenMapRoundTrip(t *testing.T) {
	tokenMap := NewTokenMap()
	encodedMap, err := tokenMap.encodeMap()
	require.NoError(t, err)
	require.Empty(t, encodedMap)

	cursor, err := encodeResumeToken(1234, encodedMap)
	require.NoError(t, err)

	epoch, decodedMap, err := decodeResumeToken(cursor)
	require.NoError(t, err)
	require.Equal(t, int64(1234), epoch)
	require.NotNil(t, decodedMap)
	require.Empty(t, decodedMap)

	result := NewTokenMap()
	require.NoError(t, result.decodeMap(decodedMap))
	require.Empty(t, result.Map)
}

func TestDecodeResumeTokenRejectsMalformedInput(t *testing.T) {
	tests := map[string][]byte{
		"empty":          nil,
		"not BSON":       {1, 2, 3},
		"missing epoch":  mustMarshalBSON(t, bson.D{{Key: "b", Value: bson.Binary{}}}),
		"invalid epoch":  mustMarshalBSON(t, bson.D{{Key: "a", Value: "now"}, {Key: "b", Value: bson.Binary{}}}),
		"missing tokens": mustMarshalBSON(t, bson.D{{Key: "a", Value: int64(1)}}),
		"invalid tokens": mustMarshalBSON(t, bson.D{{Key: "a", Value: int64(1)}, {Key: "b", Value: "tokens"}}),
	}
	for name, epoch := range map[string]interface{}{
		"fractional epoch": 1234.5,
		"double epoch":     float64(1234),
		"negative epoch":   int64(-1),
		"overflow epoch":   int64(math.MaxUint32) + 1,
	} {
		tests[name] = mustMarshalBSON(t, bson.D{{Key: "a", Value: epoch}, {Key: "b", Value: bson.Binary{}}})
	}

	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := decodeResumeToken(input)
			require.Error(t, err)

			// Both streaming endpoints must reject the cursor before accessing
			// the database or server stream; neither is configured here.
			connector := &conn{}
			err = connector.StreamLSN(context.Background(), connect.NewRequest(&adiomv1.StreamLSNRequest{Cursor: input}), nil)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			err = connector.StreamUpdates(context.Background(), connect.NewRequest(&adiomv1.StreamUpdatesRequest{Cursor: input}), nil)
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
		})
	}
}

func TestResumeCursorTimestampBoundaries(t *testing.T) {
	for _, epoch := range []interface{}{int32(0), int32(1234), int64(math.MaxUint32)} {
		cursor := mustMarshalBSON(t, bson.D{{Key: "a", Value: epoch}, {Key: "b", Value: bson.Binary{}}})
		_, _, err := decodeResumeToken(cursor)
		require.NoError(t, err)
	}
}

func TestResumeChangeStreamOptions(t *testing.T) {
	loc := iface.Location{Database: "db", Collection: "col"}
	token := mustMarshalBSON(t, bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{"Rid":"collection"}`)}}})
	for _, withToken := range []bool{false, true} {
		t.Run(map[bool]string{false: "timestamp fallback", true: "resume after token"}[withToken], func(t *testing.T) {
			tokens := NewTokenMap()
			if withToken {
				tokens.AddToken(loc, token)
			}
			encodedMap, err := tokens.encodeMap()
			require.NoError(t, err)
			cursor, err := encodeResumeToken(1234, encodedMap)
			require.NoError(t, err)
			epoch, decodedMap, err := decodeResumeToken(cursor)
			require.NoError(t, err)
			decodedTokens := NewTokenMap()
			require.NoError(t, decodedTokens.decodeMap(decodedMap))
			decodedToken, _ := decodedTokens.GetToken(loc)

			// Exercise the options shared by StreamLSN and StreamUpdates.
			opts := &moptions.ChangeStreamOptions{}
			for _, apply := range resumeChangeStreamOptions(epoch, decodedToken).List() {
				require.NoError(t, apply(opts))
			}
			if withToken {
				require.Equal(t, token, opts.ResumeAfter)
				require.Nil(t, opts.StartAtOperationTime)
			} else {
				require.Nil(t, opts.ResumeAfter)
				require.Equal(t, &bson.Timestamp{T: 1234}, opts.StartAtOperationTime)
			}
		})
	}
}

func TestExtractRidFromResumeToken(t *testing.T) {
	token := mustMarshalBSON(t, bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{"Rid":"collection"}`)}}})
	rid, err := extractRidFromResumeToken(token)
	require.NoError(t, err)
	require.Equal(t, "collection", rid)

	for name, malformed := range map[string]bson.Raw{
		"empty":          nil,
		"invalid BSON":   {1, 2, 3},
		"missing data":   mustMarshalBSON(t, bson.D{}),
		"wrong type":     mustMarshalBSON(t, bson.D{{Key: "_data", Value: int32(1)}}),
		"empty binary":   mustMarshalBSON(t, bson.D{{Key: "_data", Value: bson.Binary{}}}),
		"invalid JSON":   mustMarshalBSON(t, bson.D{{Key: "_data", Value: bson.Binary{Data: []byte("{")}}}),
		"missing Rid":    mustMarshalBSON(t, bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{}`)}}}),
		"wrong Rid type": mustMarshalBSON(t, bson.D{{Key: "_data", Value: bson.Binary{Data: []byte(`{"Rid":123}`)}}}),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := extractRidFromResumeToken(malformed)
			require.Error(t, err)
		})
	}
}

func TestDecodeResumeTokenAcceptsLegacyNullEmptyMap(t *testing.T) {
	cursor := mustMarshalBSON(t, bson.D{
		{Key: "a", Value: int64(1234)},
		{Key: "b", Value: nil},
	})

	epoch, tokenMap, err := decodeResumeToken(cursor)
	require.NoError(t, err)
	require.Equal(t, int64(1234), epoch)
	require.Empty(t, tokenMap)
}

func TestTokenMapRejectsMalformedEncoding(t *testing.T) {
	cursor, err := encodeResumeToken(1234, []byte("not a gob token map"))
	require.NoError(t, err)

	_, encodedMap, err := decodeResumeToken(cursor)
	require.NoError(t, err)
	require.Error(t, NewTokenMap().decodeMap(encodedMap))
}

func TestShardKeyDefinitionAndDummyFilters(t *testing.T) {
	// Marshal from bson.D to exercise the driver v2 representation Cosmos returns.
	collectionInfo := mustMarshalBSON(t, bson.D{
		{Key: "id", Value: "collectionRid"},
		{Key: "shardKeyDefinition", Value: bson.D{
			{Key: "partitionKey", Value: "Hash"},
			{Key: "tenant", Value: "Hash"},
			{Key: "_id", Value: "Hash"},
		}},
	})

	fields, err := extractShardKeyFields(collectionInfo)
	require.NoError(t, err)
	require.Equal(t, []string{"partitionKey", "tenant"}, fields)

	document := makeDummyDocument(fields)
	require.Equal(t, bson.M{"partitionKey": "", "tenant": ""}, document)

	filter := makeDummyDeleteFilter("dummy-id", document, fields)
	require.Equal(t, bson.M{"_id": "dummy-id", "partitionKey": "", "tenant": ""}, filter)
}

func TestTokenMapRoundTrip(t *testing.T) {
	original := NewTokenMap()
	original.AddToken(iface.Location{Database: "db", Collection: "col"}, bson.Raw{5, 0, 0, 0, 0})
	encoded, err := original.encodeMap()
	require.NoError(t, err)

	decoded := NewTokenMap()
	require.NoError(t, decoded.decodeMap(encoded))
	require.Equal(t, original.Map, decoded.Map)
}

func mustMarshalBSON(t *testing.T, value interface{}) bson.Raw {
	t.Helper()
	encoded, err := bson.Marshal(value)
	require.NoError(t, err)
	return encoded
}
