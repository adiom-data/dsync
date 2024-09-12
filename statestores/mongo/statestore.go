/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	connectorMongo "github.com/adiom-data/dsync/connectors/mongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

// default db name if not set in the connection string
const defaultInternalDbName = "adiom-internal"

type StateStore struct {
	settings StateStoreSettings
	client   *mongo.Client
	ctx      context.Context

	db *mongo.Database
}

type StateStoreSettings struct {
	ConnectionString string

	serverConnectTimeout time.Duration
	pingTimeout          time.Duration
}

func NewMongoStateStore(settings StateStoreSettings) *StateStore {
	settings.serverConnectTimeout = 10 * time.Second
	settings.pingTimeout = 10 * time.Second

	return &StateStore{settings: settings}
}

func (s *StateStore) Setup(ctx context.Context) error {
	s.ctx = ctx

	// Check that the provided connection string is pointing to a genuine MongoDB or CosmosDB instance
	// Otherwise we might get strange errors later on
	if connectorMongo.GetMongoFlavor(s.settings.ConnectionString) != connectorMongo.FlavorMongoDB && connectorMongo.GetMongoFlavor(s.settings.ConnectionString) != connectorMongo.FlavorCosmosDB {
		return fmt.Errorf("statestore connection string should point to a genuine MongoDB or CosmosDB instance")
	}

	// Register bson.M as a type map entry to ensure proper decoding of interface{} types
	tM := reflect.TypeOf(bson.M{})
	reg := bson.NewRegistry()
	reg.RegisterTypeMapEntry(bson.TypeEmbeddedDocument, tM)

	// Connect to the MongoDB instance
	ctxConnect, cancelConnectCtx := context.WithTimeout(s.ctx, s.settings.serverConnectTimeout)
	defer cancelConnectCtx()
	clientOptions := options.Client().ApplyURI(s.settings.ConnectionString).SetConnectTimeout(s.settings.serverConnectTimeout).SetRegistry(reg)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	s.client = client

	// Check the connection
	ctxPing, cancelPingCtx := context.WithTimeout(s.ctx, s.settings.pingTimeout)
	defer cancelPingCtx()
	err = s.client.Ping(ctxPing, nil)
	if err != nil {
		return err
	}

	// Set the working database
	// No need to handle error as it would've failed before in the options parsing
	cs, _ := connstring.ParseAndValidate(s.settings.ConnectionString)
	db_name := defaultInternalDbName
	if cs.Database != "" {
		db_name = cs.Database
	}
	slog.Debug(fmt.Sprintf("Using %v as the metadata database name", db_name))
	s.db = s.client.Database(db_name)

	return nil
}

func (s *StateStore) Teardown() {
	if s.client != nil {
		if err := s.client.Disconnect(s.ctx); err != nil {
			panic(err)
		}
	}
}

func (s *StateStore) getStore(name string) *mongo.Collection {
	return s.db.Collection(name)
}

func (s *StateStore) PersistObject(storeName string, id interface{}, obj interface{}) error {
	coll := s.getStore(storeName)
	_, err := coll.ReplaceOne(s.ctx, bson.M{"_id": id}, obj, options.Replace().SetUpsert(true))
	return err
}

func (s *StateStore) RetrieveObject(storeName string, id interface{}, obj interface{}) error {
	coll := s.getStore(storeName)
	result := coll.FindOne(s.ctx, bson.M{"_id": id})
	if result.Err() != nil {
		return result.Err()
	}

	err := result.Decode(obj)
	if err != nil {
		return err
	}

	return nil
}

func (s *StateStore) DeleteObject(storeName string, id interface{}) error {
	coll := s.getStore(storeName)
	_, err := coll.DeleteOne(s.ctx, bson.M{"_id": id})
	return err
}
