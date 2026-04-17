//go:build external
// +build external

/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	test2 "github.com/adiom-data/dsync/pkg/test"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	MongoEnvironmentVariable = "MONGO_TEST"
)

var TestMongoConnectionString = os.Getenv(MongoEnvironmentVariable)

func DBString() string {
	if r := os.Getenv("MONGO_TEST_DB"); r != "" {
		return r
	}
	return "test"
}

func ColString() string {
	if r := os.Getenv("MONGO_TEST_COL"); r != "" {
		return r
	}
	return "test"
}

// Standard test suite for the connector interface
func TestMongoConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(
		func() iface.Connector {
			conn, err := NewConn(ConnectorSettings{ConnectionString: TestMongoConnectionString})
			if err != nil {
				t.FailNow()
			}
			return common.NewLocalConnector("test", conn, common.ConnectorSettings{ResumeTokenUpdateInterval: 5 * time.Second})
		},
		func() test.TestDataStore {
			return NewMongoTestDataStore(TestMongoConnectionString)
		})
	suite.Run(t, tSuite)
}

func assertDoc(t *testing.T, col *mongo.Collection, expected map[string]string) {
	var res map[string]string
	col.FindOne(t.Context(), bson.M{"_id": expected["_id"]}).Decode(&res)
	assert.Equal(t, expected, res)
}

func assertNoDoc(t *testing.T, col *mongo.Collection, id string) {
	err := col.FindOne(t.Context(), bson.M{"_id": id}).Decode(nil)
	assert.ErrorIs(t, err, mongo.ErrNoDocuments)
}

func toBson(t *testing.T, v interface{}) []byte {
	b, err := bson.Marshal(v)
	assert.NoError(t, err)
	return b
}

func toBsonID(t *testing.T, s string) []*adiomv1.BsonValue {
	typ, d, err := bson.MarshalValue(s)
	assert.NoError(t, err)
	return []*adiomv1.BsonValue{{
		Data: d,
		Type: uint32(typ),
		Name: "_id",
	}}
}

func TestMongoConnectorUpdates(t *testing.T) {
	client, err := MongoClient(context.Background(), ConnectorSettings{ConnectionString: TestMongoConnectionString})
	assert.NoError(t, err)
	col := client.Database(DBString()).Collection(ColString())

	if err := col.Database().Drop(t.Context()); err != nil {
		assert.NoError(t, err)
	}
	defer col.Database().Drop(t.Context())

	id1 := toBsonID(t, "id1")
	id2 := toBsonID(t, "id2")
	id3 := toBsonID(t, "id3")
	id4 := toBsonID(t, "id4")

	conn, err := NewConn(ConnectorSettings{ConnectionString: TestMongoConnectionString, MaxPageSize: 2})
	if err != nil {
		assert.NoError(t, err)
	}
	ns := fmt.Sprintf("%s.%s", DBString(), ColString())

	updateSet := []*adiomv1.Update{
		{
			Id:   id1,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: toBson(t, bson.M{"a": "a"}),
		},
		{
			Id:   id2,
			Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE,
			Data: toBson(t, bson.M{"a": "b"}),
		},
		{
			Id:   id3,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: toBson(t, bson.M{"a": "a"}),
		},
		{
			Id:   id4,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: toBson(t, bson.M{"a": "a"}),
		},
		{
			Id:   id3,
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		},
		{
			Id:   id3,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: toBson(t, bson.M{"a": "a"}),
		},
		{
			Id:   id4,
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		},
		{
			Id:   id1,
			Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE,
			Data: toBson(t, bson.M{"a": "b"}),
		},
		{
			Id:   id1,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: toBson(t, bson.M{"b": "c"}),
		},
	}

	// Test unordered (no apply is last unless it is unique)
	if _, err := conn.WriteUpdates(t.Context(), connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: ns,
		Updates:   updateSet,
	})); err != nil {
		assert.NoError(t, err)
	}
	assertDoc(t, col, map[string]string{"_id": "id1", "b": "c"})
	assertDoc(t, col, map[string]string{"_id": "id2", "a": "b"})
	assertDoc(t, col, map[string]string{"_id": "id3", "a": "a"})
	assertNoDoc(t, col, "id4")

	if err := col.Database().Drop(t.Context()); err != nil {
		assert.NoError(t, err)
	}

	// Test ordered
	if _, err := conn.WriteUpdates(t.Context(), connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: ns,
		Updates: append(updateSet,
			&adiomv1.Update{
				Id:   id1,
				Type: adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE,
				Data: toBson(t, bson.M{"c": "d"}),
			},
		),
	})); err != nil {
		assert.NoError(t, err)
	}
	assertDoc(t, col, map[string]string{"_id": "id1", "b": "c", "c": "d"})
	assertDoc(t, col, map[string]string{"_id": "id2", "a": "b"})
	assertDoc(t, col, map[string]string{"_id": "id3", "a": "a"})
	assertNoDoc(t, col, "id4")
}

func TestMongoConnectorSuite2(t *testing.T) {
	client, err := MongoClient(context.Background(), ConnectorSettings{ConnectionString: TestMongoConnectionString})
	assert.NoError(t, err)
	col := client.Database(DBString()).Collection(ColString())
	ns := fmt.Sprintf("%s.%s", DBString(), ColString())

	tSuite := test2.NewConnectorTestSuite(ns, func() adiomv1connect.ConnectorServiceClient {
		conn, err := NewConn(ConnectorSettings{ConnectionString: TestMongoConnectionString, MaxPageSize: 2})
		if err != nil {
			t.FailNow()
		}
		return test2.ClientFromHandler(conn)
	}, func(ctx context.Context) error {
		if err := col.Database().Drop(ctx); err != nil {
			return err
		}

		_, err := col.InsertOne(ctx, bson.D{{"data", "hi"}})
		if err != nil {
			return err
		}

		_, err = col.InsertOne(ctx, bson.D{{"data", "hi2"}})
		if err != nil {
			return err
		}

		_, err = col.InsertOne(ctx, bson.D{{"data", "hi3"}})
		if err != nil {
			return err
		}

		return nil
	}, func(ctx context.Context) error {
		_, err := col.InsertOne(ctx, bson.D{{"data", "update"}})
		if err != nil {
			return err
		}
		return nil
	}, 3, 3)

	tSuite.AssertExists = func(ctx context.Context, a *assert.Assertions, id []*adiomv1.BsonValue, exists bool) error {
		mongoID := bson.RawValue{
			Type:  bson.Type(id[0].GetType()),
			Value: id[0].GetData(),
		}
		idFilter := bson.D{{Key: "_id", Value: mongoID}}
		res := col.FindOne(ctx, idFilter)
		if exists {
			a.NoError(res.Err())
		} else {
			a.ErrorIs(res.Err(), mongo.ErrNoDocuments)
		}

		return nil
	}
	suite.Run(t, tSuite)
}

/**
* Implement a Mongo-specific test data store - we will use this to insert dummy data in some tests
 */
func NewMongoTestDataStore(TestMongoConnectionString string) test.TestDataStore {
	return &MongoTestDataStore{ConnectionString: TestMongoConnectionString}
}

type MongoTestDataStore struct {
	ConnectionString string
	client           *mongo.Client
}

func (m *MongoTestDataStore) Setup() error {
	// connect to the underlying database
	clientOptions := options.Client().ApplyURI(m.ConnectionString)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		return err
	}
	m.client = client

	return nil
}

func (m *MongoTestDataStore) InsertDummy(dbName string, colName string, data interface{}) error {
	db := m.client.Database(dbName)
	coll := db.Collection(colName)
	_, err := coll.InsertOne(context.TODO(), data)

	return err
}

func (m *MongoTestDataStore) Teardown() error {
	if err := m.client.Disconnect(context.TODO()); err != nil {
		return err
	}
	return nil
}

func (m *MongoTestDataStore) DeleteNamespace(dbName string, colName string) error {
	db := m.client.Database(dbName)
	coll := db.Collection(colName)
	err := coll.Drop(context.TODO())
	return err
}
