package statestoreMongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStateStore struct {
	settings MongoStateStoreSettings
	client   *mongo.Client
	ctx      context.Context
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

	return nil
}

func (s *MongoStateStore) Teardown() {
	if s.client != nil {
		s.client.Disconnect(s.ctx)
	}
}
