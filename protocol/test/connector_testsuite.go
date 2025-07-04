/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package test

import (
	"context"
	"fmt"
	"time"

	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
)

/**
* Defines a generic test suite for implementations of the connector interface
* Implementations will have their own tests that hook into this suite
 */

type ConnectorTestSuite struct {
	suite.Suite
	connectorFactoryFunc func() iface.Connector
	datastoreFactoryFunc func() TestDataStore //optional and might be nil
	SkipIntegrity        bool
}

func NewConnectorTestSuite(connectorFunc func() iface.Connector, datastoreFactoryFunc func() TestDataStore) *ConnectorTestSuite {
	test_suite := new(ConnectorTestSuite)
	test_suite.connectorFactoryFunc = connectorFunc
	test_suite.datastoreFactoryFunc = datastoreFactoryFunc
	return test_suite
}

// We are creating new connector instances for each test, so we don't need to do anything here
// We do this partially because our connectors don't support multi-flow setups and aren't thread-safe at all
func (suite *ConnectorTestSuite) SetupSuite() {
}

func (suite *ConnectorTestSuite) TearDownSuite() {
}

// Check that the connector can read
func (suite *ConnectorTestSuite) TestConnectorReadAll() {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID("1")
	var caps iface.ConnectorCapabilities
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(testConnectorID, nil).Run(func(args mock.Arguments) {
		// Store advertised connector capabilities to skip irrelevant tests
		caps = args.Get(0).(iface.ConnectorDetails).Cap // Perform a type assertion here
	})

	// create a new connector object
	connector := suite.connectorFactoryFunc()

	// setup the connector and make sure it returns no errors
	err := connector.Setup(ctx, t)
	assert.NoError(suite.T(), err)

	// check that the mocked methods were called
	t.AssertExpectations(suite.T())
	c.AssertExpectations(suite.T())

	// Check if the connector supports source capabilities
	if !caps.Source {
		// Check that the method fails first
		err := connector.StartReadToChannel(iface.FlowID("1234"), iface.ConnectorOptions{}, iface.ConnectorReadPlan{}, iface.DataChannelID("4321"))
		assert.Error(suite.T(), err, "Should fail to read data from a source if the connector does not support source capabilities")
		suite.T().Skip("Skipping test because this connector does not support source capabilities")
	}

	// Do some prep
	flowID := iface.FlowID("1234")
	dataChannelID := iface.DataChannelID("4321")
	dataChannel := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{
		Namespace: []string{NamespaceString()},
	}
	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil)
	c.On("NotifyTaskDone", flowID, testConnectorID, mock.AnythingOfType("iface.ReadPlanTaskID"), mock.Anything).Return(nil)
	messageCount := 0

	// Start a go routine to read from the data channel until it's closed
	go func() {
		for {
			_, ok := <-dataChannel
			if !ok {
				break
			}
			messageCount++
		}
	}()

	// Generate a read plan
	// We'll need to implement a mock for the completion function to store the read plan
	var readPlan iface.ConnectorReadPlan
	readPlanComplete := make(chan struct{})
	c.On("PostReadPlanningResult", flowID, testConnectorID, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		readPlanRes := args.Get(2).(iface.ConnectorReadPlanResult) // Perform a type assertion here
		assert.True(suite.T(), readPlanRes.Success, "Read planning should have succeeded")
		readPlan = readPlanRes.ReadPlan
		close(readPlanComplete)
	})
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).RequestCreateReadPlan(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions))
	}, NonBlockingTimeout,
		flowID, options)
	assert.NoError(suite.T(), err)
	// wait for the read plan to be generated
	select {
	case <-readPlanComplete:
		// Read plan is complete
	case <-time.After(ReadPlanningTimeout):
		// Timeout after read planning timeout
		suite.T().Errorf("Timed out while waiting for the read plan")
		suite.T().FailNow()
	}

	// Test reading all data from a source
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.ConnectorReadPlan), args[3].(iface.DataChannelID))
	}, NonBlockingTimeout,
		flowID, options, readPlan, dataChannelID)
	assert.NoError(suite.T(), err)

	// Sleep for a bit
	suite.T().Log("Sleeping for 10 seconds to allow the connector to read data")
	time.Sleep(10 * time.Second)

	// The connector should still be running right now and reading data - let's interrupt it
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
	}, NonBlockingTimeout, flowID)
	assert.NoError(suite.T(), err)

	// Sleep for 1 second to ensure that the interruption took effect
	time.Sleep(1 * time.Second)

	// We should have gotten some data in the channel
	assert.True(suite.T(), messageCount > 0, "Should have read some data")

	// A notification should have been sent to the coordinator that the job is done
	c.AssertCalled(suite.T(), "NotifyDone", flowID, testConnectorID)

	connector.Teardown()
}

// Check that the connector can write
func (suite *ConnectorTestSuite) TestConnectorWrite() {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID("2")
	var caps iface.ConnectorCapabilities
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(testConnectorID, nil).Run(func(args mock.Arguments) {
		// Store advertised connector capabilities to skip irrelevant tests
		caps = args.Get(0).(iface.ConnectorDetails).Cap // Perform a type assertion here
	})

	// create a new connector object
	connector := suite.connectorFactoryFunc()

	// setup the connector and make sure it returns no errors
	err := connector.Setup(ctx, t)
	assert.NoError(suite.T(), err)

	// check that the mocked methods were called
	t.AssertExpectations(suite.T())
	c.AssertExpectations(suite.T())

	// Check if the connector supports sink capabilities
	if !caps.Sink {
		suite.T().Skip("Skipping test because this connector does not support sink capabilities")
	}

	// Do some prep
	flowID := iface.FlowID("2234")
	dataChannelID := iface.DataChannelID("4321")
	dataChannel := make(chan iface.DataMessage)
	defer close(dataChannel)

	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil)
	c.On("NotifyTaskDone", flowID, testConnectorID, mock.AnythingOfType("iface.ReadPlanTaskID"), mock.Anything).Return(nil)
	messageIterCount := 100

	dbName := DBString()
	colName := ColString()
	// Connect the test data store
	if suite.datastoreFactoryFunc != nil {
		dataStore := suite.datastoreFactoryFunc()
		err = dataStore.Setup()
		assert.NoError(suite.T(), err)
		_ = dataStore.DeleteNamespace(dbName, colName)
	}

	// Start a go routine to write to the data channel
	go func() {
		loc := fmt.Sprintf("%v.%v", dbName, colName)
		lsn := int64(0)
		// write a number of messages to the channel
		for i := 0; i < messageIterCount; i++ {
			//do a simple 'pre-fix' random message sequence
			id := i
			doc := bson.M{"_id": id, "test": i}
			updatedDoc := bson.M{"_id": id, "test": i + 1}

			idType, idVal, _ := bson.MarshalValue(id)
			bsonDataRaw, _ := bson.Marshal(doc)
			bsonDataRawUpdated, _ := bson.Marshal(updatedDoc)
			aid := []*adiomv1.BsonValue{{
				Data: idVal,
				Type: uint32(idType),
			}}

			dataChannel <- iface.DataMessage{Data: &bsonDataRaw, Id: aid, MutationType: iface.MutationType_Insert, Loc: loc, SeqNum: lsn}
			lsn++
			dataChannel <- iface.DataMessage{Data: &bsonDataRawUpdated, MutationType: iface.MutationType_Update, Loc: loc, SeqNum: lsn, Id: aid}
			lsn++
			dataChannel <- iface.DataMessage{MutationType: iface.MutationType_Delete, Loc: loc, SeqNum: lsn, Id: aid}
			lsn++
		}
	}()

	// Test writing to the sink
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartWriteFromChannel(args[0].(iface.FlowID), args[1].(iface.DataChannelID))
	}, NonBlockingTimeout,
		flowID, dataChannelID)
	assert.NoError(suite.T(), err)

	// Sleep for a bit
	suite.T().Log("Sleeping for 10 seconds to allow the connector to write data")
	time.Sleep(10 * time.Second)

	// The connector should still be running right now and writing data from the channel - let's interrupt it
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
	}, NonBlockingTimeout, flowID)
	assert.NoError(suite.T(), err)

	// Sleep for 1 second to ensure that the interruption took effect
	time.Sleep(1 * time.Second)

	// We should have written some data from the channel, so the LSN should be greater than 0
	assert.True(suite.T(), connector.GetConnectorStatus(flowID).WriteLSN > 0, "Should have written some data")
	// A notification should have been sent to the coordinator that the job is done
	c.AssertCalled(suite.T(), "NotifyDone", flowID, testConnectorID)

	connector.Teardown()
}
