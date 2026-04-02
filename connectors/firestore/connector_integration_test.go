//go:build external
// +build external

/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package firestore

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"cloud.google.com/go/firestore"
	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	pkgtest "github.com/adiom-data/dsync/pkg/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	// Set FIRESTORE_EMULATOR_HOST=localhost:8080 to use emulator
	// Set FIRESTORE_PROJECT_ID=test-project for the project ID
	testProjectID  = "test-project"
	testDatabase   = "(default)"
	testCollection = "test_collection"
	testNamespace  = "test.collection"
)

func getTestProjectID() string {
	if p := os.Getenv("FIRESTORE_PROJECT_ID"); p != "" {
		return p
	}
	return testProjectID
}

func getEmulatorHost() string {
	return os.Getenv("FIRESTORE_EMULATOR_HOST")
}

func newTestClient(ctx context.Context) (*firestore.Client, error) {
	projectID := getTestProjectID()
	var opts []option.ClientOption

	// When using emulator, no credentials needed
	if getEmulatorHost() != "" {
		opts = append(opts, option.WithoutAuthentication())
	}

	return firestore.NewClient(ctx, projectID, opts...)
}

func clearCollection(ctx context.Context, client *firestore.Client, collectionName string) error {
	col := client.Collection(collectionName)
	iter := col.Documents(ctx)
	defer iter.Stop()

	batch := client.Batch()
	count := 0

	for {
		doc, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		batch.Delete(doc.Ref)
		count++

		// Firestore batch limit is 500
		if count >= 500 {
			if _, err := batch.Commit(ctx); err != nil {
				return err
			}
			batch = client.Batch()
			count = 0
		}
	}

	if count > 0 {
		if _, err := batch.Commit(ctx); err != nil {
			return err
		}
	}

	return nil
}

func TestFirestoreConnectorSuite(t *testing.T) {
	if getEmulatorHost() == "" {
		t.Skip("Skipping integration test: FIRESTORE_EMULATOR_HOST not set")
	}

	ctx := context.Background()
	projectID := getTestProjectID()
	collectionName := namespaceToCollection(testNamespace)

	// Create a direct client for test setup/verification
	testClient, err := newTestClient(ctx)
	if err != nil {
		t.Fatalf("Failed to create test client: %v", err)
	}
	defer testClient.Close()

	tSuite := pkgtest.NewConnectorTestSuite(
		testNamespace,
		func() adiomv1connect.ConnectorServiceClient {
			conn, err := NewConn(ctx, ConnectorSettings{
				Uri:       "firestore://" + projectID,
				BatchSize: 500,
			})
			if err != nil {
				panic(err)
			}
			return pkgtest.ClientFromHandler(conn)
		},
		// Bootstrap: clear collection
		func(ctx context.Context) error {
			return clearCollection(ctx, testClient, collectionName)
		},
		// InsertUpdates: not applicable for sink-only
		nil,
		0, // NumPages - sink only, no source
		0, // NumItems - sink only, no source
	)

	// Sink-only connector, skip source tests
	tSuite.SkipDuplicateTest = true
	tSuite.SkipWriteUpdatesTest = false

	// Custom assertion to verify document exists in Firestore
	tSuite.AssertExists = func(ctx context.Context, a *assert.Assertions, id []*adiomv1.BsonValue, exists bool) error {
		docID, err := extractIDFromBsonValues(id)
		if err != nil {
			return err
		}

		docRef := testClient.Collection(collectionName).Doc(docID)
		_, err = docRef.Get(ctx)

		if exists {
			a.NoError(err, "Document should exist")
		} else {
			a.Error(err, "Document should not exist")
		}

		return nil
	}

	suite.Run(t, tSuite)
}

// TestFirestoreBatchWrite tests efficient batch writing
func TestFirestoreBatchWrite(t *testing.T) {
	if getEmulatorHost() == "" {
		t.Skip("Skipping integration test: FIRESTORE_EMULATOR_HOST not set")
	}

	ctx := context.Background()
	projectID := getTestProjectID()
	collectionName := "batch_test"

	// Create connector
	connector, err := NewConn(ctx, ConnectorSettings{
		Uri:       "firestore://" + projectID,
		BatchSize: 100,
	})
	assert.NoError(t, err)
	defer connector.(interface{ Teardown() }).Teardown()

	// Create test client for verification
	testClient, err := newTestClient(ctx)
	assert.NoError(t, err)
	defer testClient.Close()

	// Clear collection first
	err = clearCollection(ctx, testClient, collectionName)
	assert.NoError(t, err)

	// Create test data - 250 documents to test batching (will need 3 batches with batch size 100)
	client := pkgtest.ClientFromHandler(connector)
	var data [][]byte
	for i := 0; i < 250; i++ {
		doc := map[string]any{
			"id":    i, // JSON_ID uses "id" field
			"value": i * 10,
		}
		encoded, _ := encodeJSON(doc)
		data = append(data, encoded)
	}

	// Write data
	_, err = client.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: collectionName,
		Data:      data,
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify all documents were written
	iter := testClient.Collection(collectionName).Documents(ctx)
	defer iter.Stop()

	count := 0
	for {
		_, err := iter.Next()
		if err == iterator.Done {
			break
		}
		assert.NoError(t, err)
		count++
	}
	assert.Equal(t, 250, count, "All 250 documents should be written")
}

// TestFirestoreDeterministicOverwrite tests that writes deterministically overwrite existing data
func TestFirestoreDeterministicOverwrite(t *testing.T) {
	if getEmulatorHost() == "" {
		t.Skip("Skipping integration test: FIRESTORE_EMULATOR_HOST not set")
	}

	ctx := context.Background()
	projectID := getTestProjectID()
	collectionName := "overwrite_test"

	// Create connector
	connector, err := NewConn(ctx, ConnectorSettings{
		Uri:       "firestore://" + projectID,
		BatchSize: 500,
	})
	assert.NoError(t, err)
	defer connector.(interface{ Teardown() }).Teardown()

	// Create test client for verification
	testClient, err := newTestClient(ctx)
	assert.NoError(t, err)
	defer testClient.Close()

	// Clear collection first
	err = clearCollection(ctx, testClient, collectionName)
	assert.NoError(t, err)

	client := pkgtest.ClientFromHandler(connector)

	// Write initial document - JSON_ID uses "id" field
	doc1 := map[string]any{
		"id":     "test-doc",
		"field1": "value1",
		"field2": "value2",
	}
	encoded1, _ := encodeJSON(doc1)
	_, err = client.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: collectionName,
		Data:      [][]byte{encoded1},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Overwrite with different data (missing field2) - JSON_ID uses "id" field
	doc2 := map[string]any{
		"id":     "test-doc",
		"field1": "updated_value",
		"field3": "new_field",
	}
	encoded2, _ := encodeJSON(doc2)
	_, err = client.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: collectionName,
		Data:      [][]byte{encoded2},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify the document was completely overwritten
	docSnap, err := testClient.Collection(collectionName).Doc("test-doc").Get(ctx)
	assert.NoError(t, err)

	data := docSnap.Data()
	assert.Equal(t, "updated_value", data["field1"], "field1 should be updated")
	assert.Equal(t, "new_field", data["field3"], "field3 should exist")
	_, hasField2 := data["field2"]
	assert.False(t, hasField2, "field2 should NOT exist (complete overwrite)")
}

// TestFirestoreWriteUpdatesDelete tests delete operations
func TestFirestoreWriteUpdatesDelete(t *testing.T) {
	if getEmulatorHost() == "" {
		t.Skip("Skipping integration test: FIRESTORE_EMULATOR_HOST not set")
	}

	ctx := context.Background()
	projectID := getTestProjectID()
	collectionName := "delete_test"

	// Create connector
	connector, err := NewConn(ctx, ConnectorSettings{
		Uri:       "firestore://" + projectID,
		BatchSize: 500,
	})
	assert.NoError(t, err)
	defer connector.(interface{ Teardown() }).Teardown()

	// Create test client for verification
	testClient, err := newTestClient(ctx)
	assert.NoError(t, err)
	defer testClient.Close()

	// Clear collection first
	err = clearCollection(ctx, testClient, collectionName)
	assert.NoError(t, err)

	client := pkgtest.ClientFromHandler(connector)

	// Write a document first - JSON_ID uses "id" field
	doc := map[string]any{
		"id":   "to-delete",
		"data": "some data",
	}
	encoded, _ := encodeJSON(doc)
	_, err = client.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: collectionName,
		Data:      [][]byte{encoded},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify document exists
	_, err = testClient.Collection(collectionName).Doc("to-delete").Get(ctx)
	assert.NoError(t, err, "Document should exist before delete")

	// Delete using WriteUpdates - need to properly encode BSON value
	idType, idData, _ := bson.MarshalValue("to-delete")
	_, err = client.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: collectionName,
		Updates: []*adiomv1.Update{{
			Id: []*adiomv1.BsonValue{{
				Data: idData,
				Type: uint32(idType),
				Name: "_id",
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}},
		Type: adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify document was deleted
	_, err = testClient.Collection(collectionName).Doc("to-delete").Get(ctx)
	assert.Error(t, err, "Document should not exist after delete")
}

// Helper to encode map to JSON
func encodeJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}
