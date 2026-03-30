//go:build external
// +build external

/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package couchdb

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	pkgtest "github.com/adiom-data/dsync/pkg/test"
	"github.com/go-kivik/kivik/v4"
	_ "github.com/go-kivik/kivik/v4/couchdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const (
	CouchDBEnvironmentVariable = "COUCHDB_TEST"
)

var TestCouchDBConnectionString = os.Getenv(CouchDBEnvironmentVariable)

func DBString() string {
	if r := os.Getenv("COUCHDB_TEST_DB"); r != "" {
		return r
	}
	return "dsync_test"
}

func TestCouchDBConnectorSuite(t *testing.T) {
	if TestCouchDBConnectionString == "" {
		t.Skip("COUCHDB_TEST environment variable not set")
	}

	dsn, err := convertUriToDSN(TestCouchDBConnectionString)
	if err != nil {
		t.Fatalf("Failed to convert URI: %v", err)
	}

	client, err := kivik.New("couch", dsn)
	if err != nil {
		t.Fatalf("Failed to create CouchDB client: %v", err)
	}

	dbName := DBString()
	db := client.DB(dbName)

	tSuite := pkgtest.NewConnectorTestSuite(dbName, func() adiomv1connect.ConnectorServiceClient {
		conn, err := NewConn(ConnectorSettings{
			Uri:         TestCouchDBConnectionString,
			MaxPageSize: 2,
		})
		if err != nil {
			t.FailNow()
		}
		return pkgtest.ClientFromHandler(conn)
	}, func(ctx context.Context) error {
		// Bootstrap: create database and insert test data
		_ = client.DestroyDB(ctx, dbName)
		if err := client.CreateDB(ctx, dbName); err != nil {
			return fmt.Errorf("failed to create database: %w", err)
		}

		db = client.DB(dbName)

		// Insert 3 test documents
		docs := []map[string]interface{}{
			{"_id": "doc1", "data": "hi"},
			{"_id": "doc2", "data": "hi2"},
			{"_id": "doc3", "data": "hi3"},
		}
		for _, doc := range docs {
			if _, err := db.Put(ctx, doc["_id"].(string), doc); err != nil {
				return fmt.Errorf("failed to insert document: %w", err)
			}
		}

		return nil
	}, func(ctx context.Context) error {
		// Insert update for CDC test
		doc := map[string]interface{}{"_id": "update_doc", "data": "update"}
		if _, err := db.Put(ctx, "update_doc", doc); err != nil {
			return fmt.Errorf("failed to insert update document: %w", err)
		}
		// Small delay to ensure change is propagated
		time.Sleep(100 * time.Millisecond)
		return nil
	}, 2, 3) // 2 pages (with pageSize=2, 3 docs = 2 pages), 3 items total

	tSuite.AssertExists = func(ctx context.Context, a *assert.Assertions, id []*adiomv1.BsonValue, exists bool) error {
		docID := string(id[0].GetData())
		row := db.Get(ctx, docID)
		var doc map[string]interface{}
		err := row.ScanDoc(&doc)
		if exists {
			a.NoError(err, "Document should exist")
		} else {
			a.Error(err, "Document should not exist")
		}
		return nil
	}

	// CouchDB uses JSON, not BSON - skip the BSON-specific write tests
	tSuite.SkipWriteUpdatesTest = true

	suite.Run(t, tSuite)
}

func TestCouchDBWriteDataJSON(t *testing.T) {
	if TestCouchDBConnectionString == "" {
		t.Skip("COUCHDB_TEST environment variable not set")
	}

	ctx := context.Background()

	dsn, err := convertUriToDSN(TestCouchDBConnectionString)
	assert.NoError(t, err)

	client, err := kivik.New("couch", dsn)
	assert.NoError(t, err)

	dbName := "dsync_write_test"
	_ = client.DestroyDB(ctx, dbName)
	err = client.CreateDB(ctx, dbName)
	assert.NoError(t, err)
	defer client.DestroyDB(ctx, dbName)

	conn, err := NewConn(ConnectorSettings{Uri: TestCouchDBConnectionString})
	assert.NoError(t, err)

	c := pkgtest.ClientFromHandler(conn)

	// Test WriteData with JSON
	doc := map[string]interface{}{
		"_id":  "json_test_doc",
		"name": "test",
		"value": 123,
	}
	jsonBytes, _ := json.Marshal(doc)

	_, err = c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: dbName,
		Data:      [][]byte{jsonBytes},
		Type:      adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify document was written
	db := client.DB(dbName)
	row := db.Get(ctx, "json_test_doc")
	var result map[string]interface{}
	err = row.ScanDoc(&result)
	assert.NoError(t, err)
	assert.Equal(t, "test", result["name"])
	assert.Equal(t, float64(123), result["value"])
}

func TestCouchDBWriteUpdatesJSON(t *testing.T) {
	if TestCouchDBConnectionString == "" {
		t.Skip("COUCHDB_TEST environment variable not set")
	}

	ctx := context.Background()

	dsn, err := convertUriToDSN(TestCouchDBConnectionString)
	assert.NoError(t, err)

	client, err := kivik.New("couch", dsn)
	assert.NoError(t, err)

	dbName := "dsync_updates_test"
	_ = client.DestroyDB(ctx, dbName)
	err = client.CreateDB(ctx, dbName)
	assert.NoError(t, err)
	defer client.DestroyDB(ctx, dbName)

	conn, err := NewConn(ConnectorSettings{Uri: TestCouchDBConnectionString})
	assert.NoError(t, err)

	c := pkgtest.ClientFromHandler(conn)

	// Test INSERT
	doc := map[string]interface{}{
		"_id":  "update_test_doc",
		"name": "initial",
	}
	jsonBytes, _ := json.Marshal(doc)

	_, err = c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: dbName,
		Updates: []*adiomv1.Update{{
			Id: []*adiomv1.BsonValue{{
				Data: []byte("update_test_doc"),
				Type: 2, // string
				Name: "_id",
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: jsonBytes,
		}},
		Type: adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify insert
	db := client.DB(dbName)
	row := db.Get(ctx, "update_test_doc")
	var result map[string]interface{}
	err = row.ScanDoc(&result)
	assert.NoError(t, err)
	assert.Equal(t, "initial", result["name"])

	// Test UPDATE
	doc["name"] = "updated"
	jsonBytes, _ = json.Marshal(doc)

	_, err = c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: dbName,
		Updates: []*adiomv1.Update{{
			Id: []*adiomv1.BsonValue{{
				Data: []byte("update_test_doc"),
				Type: 2,
				Name: "_id",
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: jsonBytes,
		}},
		Type: adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify update
	row = db.Get(ctx, "update_test_doc")
	err = row.ScanDoc(&result)
	assert.NoError(t, err)
	assert.Equal(t, "updated", result["name"])

	// Test DELETE
	_, err = c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: dbName,
		Updates: []*adiomv1.Update{{
			Id: []*adiomv1.BsonValue{{
				Data: []byte("update_test_doc"),
				Type: 2,
				Name: "_id",
			}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}},
		Type: adiomv1.DataType_DATA_TYPE_JSON_ID,
	}))
	assert.NoError(t, err)

	// Verify delete
	row = db.Get(ctx, "update_test_doc")
	err = row.ScanDoc(&result)
	assert.Error(t, err, "Document should be deleted")
}
