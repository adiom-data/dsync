/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorMongo

import (
	"context"
	"testing"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/test"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// TestMongoConnectionString is the connection string for the test MongoDB
	TestMongoConnectionString = "mongodb://localhost:27017"
)

// Standard test suite for the connector interface
func TestMongoConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(
		func() iface.Connector {
			return NewMongoConnector("test", MongoConnectorSettings{ConnectionString: TestMongoConnectionString, CdcResumeTokenUpdateInterval: 5 * time.Second})
		},
		func() test.TestDataStore {
			return NewMongoTestDataStore(TestMongoConnectionString)
		})
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
	m.client.Disconnect(context.TODO())
	return nil
}
