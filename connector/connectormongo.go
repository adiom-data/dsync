package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoConnector struct {
	desc string

	settings MongoConnectorSettings
	client   *mongo.Client
	ctx      context.Context

	t  iface.Transport
	id iface.ConnectorID

	connectorType         iface.ConnectorType
	connectorCapabilities iface.ConnectorCapabilities

	coord iface.CoordinatorIConnectorSignal
}

type MongoConnectorSettings struct {
	ConnectionString string

	serverConnectTimeout time.Duration
	pingTimeout          time.Duration
}

func NewMongoConnector(desc string, settings MongoConnectorSettings) *MongoConnector {
	// Set default values
	settings.serverConnectTimeout = 10 * time.Second
	settings.pingTimeout = 2 * time.Second

	return &MongoConnector{desc: desc, settings: settings}
}

func (mc *MongoConnector) Setup(ctx context.Context, t iface.Transport) error {
	mc.ctx = ctx
	mc.t = t

	// Connect to the MongoDB instance
	ctxConnect, cancel := context.WithTimeout(mc.ctx, mc.settings.serverConnectTimeout)
	defer cancel()
	clientOptions := options.Client().ApplyURI(mc.settings.ConnectionString)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	mc.client = client

	// Check the connection
	ctxPing, cancel := context.WithTimeout(mc.ctx, mc.settings.pingTimeout)
	defer cancel()
	err = mc.client.Ping(ctxPing, nil)
	if err != nil {
		return err
	}

	// Get version of the MongoDB server
	var commandResult bson.M
	err = mc.client.Database("admin").RunCommand(mc.ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&commandResult)
	if err != nil {
		return err
	}
	version := commandResult["version"]

	// Instantiate ConnectorType
	mc.connectorType = iface.ConnectorType{DbType: "MongoDB", Version: version.(string)}
	// Instantiate ConnectorCapabilities
	mc.connectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: true}

	// Get the coordinator endpoint
	coord, err := mc.t.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	mc.coord = coord

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: mc.desc, Type: mc.connectorType, Cap: mc.connectorCapabilities}
	// Register the connector
	mc.id, err = coord.RegisterConnector(connectorDetails, mc)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	slog.Info("MongoConnector has been configured with ID " + mc.id.ID)

	return nil
}

func (mc *MongoConnector) Teardown() {
	if mc.client != nil {
		mc.client.Disconnect(mc.ctx)
	}
}

func (mc *MongoConnector) SetParameters(reqCap iface.ConnectorCapabilities) {
	// Implement SetParameters logic specific to MongoConnector
}

func (mc *MongoConnector) StartReadToChannel(flowId iface.FlowID, dataChannel iface.DataChannelID) {
	go func() {
		select {
		case <-mc.ctx.Done():
			return
		case <-time.After(10 * time.Second):
			// continue with the rest of the code
		}

		slog.Info(fmt.Sprintf("Connector %s is done reading for flow %s", mc.id, flowId))
		err := mc.coord.NotifyDone(flowId, mc.id)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", mc.id, flowId, err))
		}
	}()
}

func (mc *MongoConnector) StartWriteFromChannel(flowId iface.FlowID, dataChannel iface.DataChannelID) {
	go func() {
		select {
		case <-mc.ctx.Done():
			return
		case <-time.After(15 * time.Second):
			// continue with the rest of the code
		}

		slog.Info(fmt.Sprintf("Connector %s is done writing for flow %s", mc.id, flowId))
		err := mc.coord.NotifyDone(flowId, mc.id)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", mc.id, flowId, err))
		}
	}()
}
