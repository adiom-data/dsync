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
	"os"
	"testing"
	"time"

	"github.com/adiom-data/dsync/connectors/common"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	test2 "github.com/adiom-data/dsync/pkg/test"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	MongoEnvironmentVariable = "MONGO_TEST"
)

var TestMongoConnectionString = os.Getenv(MongoEnvironmentVariable)

// Standard test suite for the connector interface
func TestMongoConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(
		func() iface.Connector {
			return common.NewLocalConnector("test", NewConn(ConnectorSettings{ConnectionString: TestMongoConnectionString}), common.ConnectorSettings{ResumeTokenUpdateInterval: 5 * time.Second})
		},
		func() test.TestDataStore {
			return NewMongoTestDataStore(TestMongoConnectionString)
		})
	suite.Run(t, tSuite)
}

func TestMongoConnectorSuite2(t *testing.T) {
	client, err := MongoClient(context.Background(), ConnectorSettings{ConnectionString: TestMongoConnectionString})
	assert.NoError(t, err)
	col := client.Database("test").Collection("test")

	tSuite := test2.NewConnectorTestSuite("test.test", func() adiomv1connect.ConnectorServiceClient {
		return test2.ClientFromHandler(NewConn(ConnectorSettings{ConnectionString: TestMongoConnectionString, MaxPageSize: 2}))
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
			Type:  bsontype.Type(id[0].GetType()),
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
	client, err := mongo.Connect(context.TODO(), clientOptions)
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
