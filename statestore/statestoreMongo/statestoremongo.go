/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package statestoreMongo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
)

// default db name if not set in the connection string
const DEFAULT_DB_NAME = "adiom-internal"

type MongoStateStore struct {
	settings MongoStateStoreSettings
	client   *mongo.Client
	ctx      context.Context

	db *mongo.Database
}

type MongoStateStoreSettings struct {
	ConnectionString string

	serverConnectTimeout time.Duration
	pingTimeout          time.Duration
}

func NewMongoStateStore(settings MongoStateStoreSettings) *MongoStateStore {
	settings.serverConnectTimeout = 10 * time.Second
	settings.pingTimeout = 2 * time.Second

	return &MongoStateStore{settings: settings}
}

func (s *MongoStateStore) Setup(ctx context.Context) error {
	s.ctx = ctx

	// Connect to the MongoDB instance
	ctxConnect, cancel := context.WithTimeout(s.ctx, s.settings.serverConnectTimeout)
	defer cancel()
	clientOptions := options.Client().ApplyURI(s.settings.ConnectionString)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	s.client = client

	// Check the connection
	ctxPing, cancel := context.WithTimeout(s.ctx, s.settings.pingTimeout)
	defer cancel()
	err = s.client.Ping(ctxPing, nil)
	if err != nil {
		return err
	}

	// Set the working database
	// No need to handle error as it would've failed before in the options parsing
	cs, _ := connstring.ParseAndValidate(s.settings.ConnectionString)
	db_name := DEFAULT_DB_NAME
	if cs.Database != "" {
		db_name = cs.Database
	}
	slog.Debug(fmt.Sprintf("Using %v as the metadata database name", db_name))
	s.db = s.client.Database(db_name)

	return nil
}

func (s *MongoStateStore) Teardown() {
	if s.client != nil {
		s.client.Disconnect(s.ctx)
	}
}

func (s *MongoStateStore) GetStore(name string) *mongo.Collection {
	return s.db.Collection(name)
}

func (s *MongoStateStore) FlowPlanPersist(fid iface.FlowID, plan iface.ConnectorReadPlan) error {
	coll := s.GetStore("flow_plan")
	_, err := coll.UpdateOne(s.ctx, bson.M{"_id": fid.ID}, bson.M{"$set": plan}, options.Update().SetUpsert(true))
	return err
}
