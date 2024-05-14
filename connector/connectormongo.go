package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
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

func (mc *MongoConnector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, dataChannelId iface.DataChannelID) error {
	var db, col string
	if options.Namespace == "" { //TODO: need to be all the namespaces or just error out?
		db = "sample_mflix"
		col = "theaters"
	} else {
		// Split the namespace into database and collection
		namespaceParts := strings.Split(options.Namespace, ".")
		if len(namespaceParts) != 2 {
			return errors.New("invalid namespace format")
		}
		db = namespaceParts[0]
		col = namespaceParts[1]
	}

	collection := mc.client.Database(db).Collection(col)

	cursor, err := collection.Find(mc.ctx, bson.D{})
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to find documents in collection: %v", err))
		return err
	}
	// Get data channel from transport interface based on the provided ID
	dataChannel, err := mc.t.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	// Declare two channels to wait for the change stream reader and the initial sync to finish
	changeStreamDone := make(chan struct{})
	initialSyncDone := make(chan struct{})

	//TODO: This and the writer should be logging progress

	// kick off the change stream reader
	go func() {
		//wait for the initial sync to finish
		<-initialSyncDone
		slog.Info(fmt.Sprintf("Connector %s is starting to read change stream for flow %s", mc.id, flowId))

		changeStream, err := collection.Watch(mc.ctx, mongo.Pipeline{
			{{"$match", bson.D{{"ns.db", db}, {"ns.coll", col}}}},
		}, moptions.ChangeStream().SetFullDocument("updateLookup"))
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to open change stream: %v", err))
		}
		defer changeStream.Close(mc.ctx)

		for changeStream.Next(mc.ctx) {
			// event := changeStream.Current
			// eventCopy := make(bson.Raw, len(event))
			// copy(eventCopy, event)
			// data := []byte(eventCopy)

			var change bson.M
			if err := changeStream.Decode(&change); err != nil {
				slog.Error(fmt.Sprintf("Failed to decode change stream event: %v", err))
				continue
			}

			dataMsg, err := mc.convertChangeStreamEventToDataMessage(change)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to convert change stream event to data message: %v", err))
				continue
			}
			//send the data message
			dataChannel <- dataMsg
		}

		if err := changeStream.Err(); err != nil {
			slog.Error(fmt.Sprintf("Change stream error: %v", err))
		}

		close(changeStreamDone)
	}()

	// kick off the initial sync
	go func() {
		slog.Info(fmt.Sprintf("Connector %s is starting initial sync for flow %s", mc.id, flowId))

		loc := iface.Location{Database: db, Collection: col}
		defer cursor.Close(mc.ctx)
		for cursor.Next(mc.ctx) {
			rawData := cursor.Current
			data := []byte(rawData)                                                                          //TODO: this should probably be serialized in Avro, Protobuf or something in the future?
			dataChannel <- iface.DataMessage{Data: &data, MutationType: iface.MutationType_Insert, Loc: loc} //TODO: is it ok that this blocks until the app is terminated if no one reads? (e.g. reader crashes)
		}
		if err := cursor.Err(); err != nil {
			slog.Error(fmt.Sprintf("Cursor error: %v", err))
		}

		close(initialSyncDone)
	}()

	// wait for both the change stream reader and the initial sync to finish
	go func() {
		<-initialSyncDone
		<-changeStreamDone

		close(dataChannel) //send a signal downstream that we are done sending data //TODO: is this the right way to do it?

		slog.Info(fmt.Sprintf("Connector %s is done reading for flow %s", mc.id, flowId))
		err := mc.coord.NotifyDone(flowId, mc.id) //TODO: Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", mc.id, flowId, err))
		}
	}()

	return nil
}

func (mc *MongoConnector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	// select {
	// case <-mc.ctx.Done():
	// 	return nil
	// case <-time.After(15 * time.Second):
	// 	return fmt.Errorf("timeout waiting for data")
	// }

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := mc.t.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	go func() {
		for loop := true; loop; {
			select {
			case <-mc.ctx.Done():
				return
			case dataMsg, ok := <-dataChannel:
				if !ok {
					// channel is closed which is a signal for us to stop
					loop = false
					break
				}
				// Process the data message
				mc.processDataMessage(dataMsg) //TODO: handle errors
			}
		}

		slog.Info(fmt.Sprintf("Connector %s is done writing for flow %s", mc.id, flowId))
		err := mc.coord.NotifyDone(flowId, mc.id)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", mc.id, flowId, err))
		}
	}()

	return nil
}
