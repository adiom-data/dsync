/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorCosmos

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/test"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Standard test suite for the connector interface
func TestCosmosConnectorSuite(t *testing.T) {
	// get the connection string from the environment variable COSMOS, if not set, fail the test
	TestCosmosConnectionString := os.Getenv("COSMOS")
	if TestCosmosConnectionString == "" {
		t.Fatal("COSMOS environment variable not set")
	}
	tSuite := test.NewConnectorTestSuite(
		func() iface.Connector {
			return NewCosmosConnector("test", CosmosConnectorSettings{ConnectionString: TestCosmosConnectionString, CdcResumeTokenUpdateInterval: 5 * time.Second})
		},
		func() test.TestDataStore {
			return NewCosmosTestDataStore(TestCosmosConnectionString)
		})
	suite.Run(t, tSuite)
}

/**
 * Implement a Mongo-specific test data store - we will use this to insert dummy data in some tests
 */
func NewCosmosTestDataStore(TestCosmosConnectionString string) test.TestDataStore {
	return &CosmosTestDataStore{ConnectionString: TestCosmosConnectionString}
}

type CosmosTestDataStore struct {
	ConnectionString string
	client           *mongo.Client
}

func (c *CosmosTestDataStore) Setup() error {
	// connect to the underlying database
	clientOptions := options.Client().ApplyURI(c.ConnectionString)
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		return err
	}
	c.client = client

	return nil
}

func (c *CosmosTestDataStore) InsertDummy(dbName string, colName string, data interface{}) error {
	db := c.client.Database(dbName)
	coll := db.Collection(colName)
	_, err := coll.InsertOne(context.TODO(), data)

	return err
}

func (c *CosmosTestDataStore) Teardown() error {
	c.client.Disconnect(context.TODO())
	return nil
}

func (c *CosmosTestDataStore) DeleteNamespace(dbName string, colName string) error {
	db := c.client.Database(dbName)
	coll := db.Collection(colName)
	err := coll.Drop(context.TODO())
	return err
}
