//go:build external
// +build external

/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package cosmos

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/adiom-data/dsync/connectors/common"
	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	test2 "github.com/adiom-data/dsync/pkg/test"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/adiom-data/dsync/protocol/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/tryvium-travels/memongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	CosmosEnvironmentVariable = "COSMOS_TEST"
)

var TestCosmosConnectionString = os.Getenv(CosmosEnvironmentVariable)

func DBString() string {
	if r := os.Getenv("COSMOS_TEST_DB"); r != "" {
		return r
	}
	return "test"
}

func ColString() string {
	if r := os.Getenv("COSMOS_TEST_COL"); r != "" {
		return r
	}
	return "test"
}

type LocalConnector interface {
	Impl() adiomv1connect.ConnectorServiceHandler
}

var connectorFactoryFunc = func() iface.Connector {
	return common.NewLocalConnector("test", NewConn(ConnectorSettings{ConnectorSettings: mongoconn.ConnectorSettings{ConnectionString: TestCosmosConnectionString}, MaxNumNamespaces: 10}), common.ConnectorSettings{ResumeTokenUpdateInterval: 5 * time.Second})
}
var connectorDeletesEmuFactoryFunc = func(TestWitnessConnectionString string) iface.Connector {
	return common.NewLocalConnector("test", NewConn(ConnectorSettings{ConnectorSettings: mongoconn.ConnectorSettings{ConnectionString: TestCosmosConnectionString}, MaxNumNamespaces: 10, EmulateDeletes: true, WitnessMongoConnString: TestWitnessConnectionString}), common.ConnectorSettings{ResumeTokenUpdateInterval: 5 * time.Second})
}
var datastoreFactoryFunc = func() test.TestDataStore {
	return NewCosmosTestDataStore(TestCosmosConnectionString)
}

// Standard test suite for the connector interface
func TestCosmosConnectorSuite(t *testing.T) {
	// get the connection string from the environment variable COSMOS, if not set, fail the test
	if TestCosmosConnectionString == "" {
		t.Fatal("COSMOS environment variable not set")
	}
	tSuite := test.NewConnectorTestSuite(
		connectorFactoryFunc,
		datastoreFactoryFunc,
	)
	suite.Run(t, tSuite)
}

func TestCosmosConnectorSuite2(t *testing.T) {
	settings := ConnectorSettings{ConnectorSettings: mongoconn.ConnectorSettings{ConnectionString: TestCosmosConnectionString, MaxPageSize: 2}, MaxNumNamespaces: 10}
	client, err := mongoconn.MongoClient(context.Background(), settings.ConnectorSettings)
	assert.NoError(t, err)
	col := client.Database(DBString()).Collection(ColString())
	ns := fmt.Sprintf("%s.%s", DBString(), ColString())

	tSuite := test2.NewConnectorTestSuite(ns, func() adiomv1connect.ConnectorServiceClient {
		return test2.ClientFromHandler(NewConn(settings))
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

/*
* Cosmos-specific test for deletes emulation DISABLED (default)
* Confirms that no deletes are emitted when the ForceDeletes() is triggered
*
* Scenario:
* 1) Start reading without any special configuration
* 2) Wait a bit to make sure that we entered the CDC phase
* 3) Do a delete
* 4) Call ForceDelete()
* 5) Wait a bit more to make sure that the deletes are processed
* 6) Check that no deletes were emitted
 */
func TestConnectorDeletesNotEmitted(testState *testing.T) {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID("3")
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(testConnectorID, nil)

	// create a new connector object
	connector := connectorFactoryFunc()

	// setup the connector and make sure it returns no errors
	err := connector.(iface.Connector).Setup(ctx, t)
	assert.NoError(testState, err)

	// check that the mocked methods were called
	t.AssertExpectations(testState)
	c.AssertExpectations(testState)

	// Connect the test data store
	dataStore := datastoreFactoryFunc().(*CosmosTestDataStore)
	err = dataStore.Setup()
	assert.NoError(testState, err)

	// Preset the variables
	var readPlan iface.ConnectorReadPlan
	flowComplete := make(chan struct{})
	dummyTestDBName := test.DBString()
	dummyTestColName := test.ColString()
	dataStore.InsertDummy(dummyTestDBName, dummyTestColName, bson.M{"_id": -1})

	// Do some prep
	flowID := iface.FlowID("1234")
	dataChannelID := iface.DataChannelID("4321")
	dataChannel := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{}
	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil).Run(func(args mock.Arguments) {
		flowComplete <- struct{}{}
	})
	c.On("NotifyTaskDone", flowID, testConnectorID, mock.AnythingOfType("iface.ReadPlanTaskID"), mock.Anything).Return(nil)
	deleteMessageCount := 0

	// Start a go routine to read from the data channel until it's closed
	dataReader := func(channel chan iface.DataMessage) {
		for {
			msg, ok := <-channel
			if !ok {
				break
			}

			if msg.MutationType == iface.MutationType_Delete {
				deleteMessageCount++
			}
		}
	}
	go dataReader(dataChannel)

	// Generate a read plan
	// We'll need to implement a mock for the completion function to store the read plan
	readPlanComplete := make(chan struct{})
	c.On("PostReadPlanningResult", flowID, testConnectorID, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		readPlanRes := args.Get(2).(iface.ConnectorReadPlanResult) // Perform a type assertion here
		assert.True(testState, readPlanRes.Success, "Read planning should have succeeded")
		readPlan = readPlanRes.ReadPlan
		close(readPlanComplete)
	})
	// We'll run this with a timeout to make sure it's non-blocking
	err = test.RunWithTimeout(testState, connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).RequestCreateReadPlan(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions))
	}, test.NonBlockingTimeout,
		flowID, options)
	assert.NoError(testState, err)
	// wait for the read plan to be generated
	select {
	case <-readPlanComplete:
		// Read plan is complete
	case <-time.After(test.ReadPlanningTimeout):
		// Timeout after read planning timeout
		testState.Errorf("Timed out while waiting for the read plan")
		testState.FailNow()
	}

	// Start reading all data from a source
	// We'll run this with a timeout to make sure it's non-blocking
	err = test.RunWithTimeout(testState, connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.ConnectorReadPlan), args[3].(iface.DataChannelID))
	}, test.NonBlockingTimeout,
		flowID, options, readPlan, dataChannelID)
	assert.NoError(testState, err)

	// Sleep for a bit
	testState.Log("Sleeping for 10 seconds to allow the connector to read data")
	time.Sleep(10 * time.Second)

	// Introduce a change in the dataset
	for i := 0; i < 2; i++ {
		err = dataStore.DeleteOneDoc(dummyTestDBName, dummyTestColName)
		assert.NoError(testState, err)
	}

	// Call the check for deletes function
	go connector.(LocalConnector).Impl().(*conn).ForceDelete()

	// Sleep for a bit
	testState.Log("Sleeping for 5 seconds to allow the connector to process any deletes")
	time.Sleep(5 * time.Second)

	// The connector should still be running right now and reading data - let's interrupt it
	err = test.RunWithTimeout(testState, connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
	}, test.NonBlockingTimeout, flowID)
	assert.NoError(testState, err)

	// Wait for flow to complete after the interruption
	select {
	case <-flowComplete:
		// Read plan is complete
	case <-time.After(test.FlowCompletionTimeout):
		// Timeout
		testState.Errorf("Timed out while waiting for the flow to complete - first interruption")
		testState.FailNow()
	}

	// Assert that no deletes were emitted
	assert.Equal(testState, 0, deleteMessageCount, "No deletes should have been emitted")

	connector.Teardown()
	dataStore.Teardown()
}

/*
* Cosmos-specific test for deletes emulation ENABLED
* Confirms that deletes are emitted when the CheckForDeletes() is triggered
*
* Scenario:
* 1) Start reading with a connector configuration that enables deletes
* 2) Wait a bit to make sure that we entered the CDC phase
* 3) Inject two entries into the witness that don't exist on the source
* 4) Call ForceDelete()
* 5) Wait a bit more to make sure that the deletes are processed
* 6) Check that the 2 deletes were emitted
 */
func TestConnectorDeletesEmitted(testState *testing.T) {
	ctx := context.Background()

	// start mongo server
	witnessMongoServer, err := memongo.Start("6.0.16")
	if err != nil {
		testState.Fatal(err)
	}
	defer witnessMongoServer.Stop()
	// create a data store for tampering with evidence
	witnessDataStore := NewCosmosTestDataStore(witnessMongoServer.URI())
	err = witnessDataStore.Setup()
	if err != nil {
		testState.Fatal(err)
	}

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID("3")
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(testConnectorID, nil)

	// create a new connector object
	connector := connectorDeletesEmuFactoryFunc(witnessMongoServer.URI())

	// setup the connector and make sure it returns no errors
	err = connector.(iface.Connector).Setup(ctx, t)
	assert.NoError(testState, err)

	// check that the mocked methods were called
	t.AssertExpectations(testState)
	c.AssertExpectations(testState)

	// Connect the test data store
	dataStore := datastoreFactoryFunc().(*CosmosTestDataStore)
	err = dataStore.Setup()
	assert.NoError(testState, err)

	// Preset the variables
	var readPlan iface.ConnectorReadPlan
	flowComplete := make(chan struct{})
	dummyTestDBName := test.DBString()
	dummyTestColName := test.ColString()
	dataStore.InsertDummy(dummyTestDBName, dummyTestColName, bson.M{"_id": -1})

	// Do some prep
	flowID := iface.FlowID("1234")
	dataChannelID := iface.DataChannelID("4321")
	dataChannel := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{}
	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil).Run(func(args mock.Arguments) {
		flowComplete <- struct{}{}
	})
	c.On("NotifyTaskDone", flowID, testConnectorID, mock.AnythingOfType("iface.ReadPlanTaskID"), mock.Anything).Return(nil)
	deleteMessageCount := 0

	// Start a go routine to read from the data channel until it's closed
	dataReader := func(channel chan iface.DataMessage) {
		for {
			msg, ok := <-channel
			if !ok {
				break
			}

			if msg.MutationType == iface.MutationType_Delete {
				deleteMessageCount++
			}
		}
	}
	go dataReader(dataChannel)

	// Generate a read plan
	// We'll need to implement a mock for the completion function to store the read plan
	readPlanComplete := make(chan struct{})
	c.On("PostReadPlanningResult", flowID, testConnectorID, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		readPlanRes := args.Get(2).(iface.ConnectorReadPlanResult) // Perform a type assertion here
		assert.True(testState, readPlanRes.Success, "Read planning should have succeeded")
		readPlan = readPlanRes.ReadPlan
		close(readPlanComplete)
	})
	// We'll run this with a timeout to make sure it's non-blocking
	err = test.RunWithTimeout(testState, connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).RequestCreateReadPlan(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions))
	}, test.NonBlockingTimeout,
		flowID, options)
	assert.NoError(testState, err)
	// wait for the read plan to be generated
	select {
	case <-readPlanComplete:
		// Read plan is complete
	case <-time.After(test.ReadPlanningTimeout):
		// Timeout after read planning timeout
		testState.Errorf("Timed out while waiting for the read plan")
		testState.FailNow()
	}

	// Start reading all data from a source
	// We'll run this with a timeout to make sure it's non-blocking
	err = test.RunWithTimeout(testState, connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.ConnectorReadPlan), args[3].(iface.DataChannelID))
	}, test.NonBlockingTimeout,
		flowID, options, readPlan, dataChannelID)
	assert.NoError(testState, err)

	// Sleep for a bit
	testState.Log("Sleeping for 10 seconds to allow the connector to read data")
	time.Sleep(10 * time.Second)

	// Inject fake entries into the witness
	// It's like what BMW did, but legal
	for i := 0; i < 2; i++ {
		witnessDataStore.InsertDummy(dummyTestDBName, dummyTestColName, bson.M{"_id": i})
	}

	// Call the check for deletes function
	go connector.(LocalConnector).Impl().(*conn).ForceDelete()

	// Sleep for a bit
	testState.Log("Sleeping for 5 seconds to allow the connector to process any deletes")
	time.Sleep(5 * time.Second)

	// The connector should still be running right now and reading data - let's interrupt it
	err = test.RunWithTimeout(testState, connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
	}, test.NonBlockingTimeout, flowID)
	assert.NoError(testState, err)

	// Wait for flow to complete after the interruption
	select {
	case <-flowComplete:
		// Read plan is complete
	case <-time.After(test.FlowCompletionTimeout):
		// Timeout
		testState.Errorf("Timed out while waiting for the flow to complete - first interruption")
		testState.FailNow()
	}

	// Assert that 2 deletes were emitted
	assert.Equal(testState, 2, deleteMessageCount, "2 deletes should have been emitted")

	connector.Teardown()
	dataStore.Teardown()
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

// Deletes the first document in the collection
func (c *CosmosTestDataStore) DeleteOneDoc(dbName string, colName string) error {
	db := c.client.Database(dbName)
	coll := db.Collection(colName)
	data := coll.FindOne(context.TODO(), bson.M{})
	_, err := coll.DeleteOne(context.TODO(), data)

	return err
}

func (c *CosmosTestDataStore) Teardown() error {
	c.client.Disconnect(context.TODO())
	return nil
}

func (c *CosmosTestDataStore) DeleteNamespace(dbName string, colName string) error {
	db := c.client.Database(dbName)
	coll := db.Collection(colName)
	// dropping collections on Cosmos causes issues with the change stream due to RID (collection id) caching
	_, err := coll.DeleteMany(context.TODO(), bson.M{})
	return err
}
