package test

import (
	"context"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

/**
* Defines a generic test suite for implementations of the connector interface
* Implementations will have their own tests that hook into this suite
 */

type ConnectorTestSuite struct {
	suite.Suite
	connector iface.Connector
	caps      iface.ConnectorCapabilities
	t         *mocks.Transport
	c         *mocks.Coordinator
}

func NewConnectorTestSuite(connector iface.Connector) *ConnectorTestSuite {
	// lo := logger.Options{Verbosity: "DEBUG"}
	// logger.Setup(lo)

	suite := new(ConnectorTestSuite)
	suite.connector = connector
	return suite
}

/**
* First, make sure that the connector can be setup correctly
* It should obtain the coordinator endpoint from the transport
* And register itself with the coordinator
* We will also keep reusing the same connector for the rest of the tests
**/
func (suite *ConnectorTestSuite) SetupSuite() {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	suite.t = new(mocks.Transport)
	t := suite.t
	suite.c = new(mocks.Coordinator)
	c := suite.c

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID{ID: "1234"}
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(testConnectorID, nil).Run(func(args mock.Arguments) {
		// Store advertised connector capabilities to skip irrelevant tests
		suite.caps = args.Get(0).(iface.ConnectorDetails).Cap // Perform a type assertion here
	})

	// setup the connector and make sure it returns no errors
	err := suite.connector.Setup(ctx, t)
	assert.NoError(suite.T(), err)

	// check that the mocked methods were called
	t.AssertExpectations(suite.T())
	c.AssertExpectations(suite.T())
}

func (suite *ConnectorTestSuite) TearDownSuite() {
	suite.connector.Teardown()
}

func (suite *ConnectorTestSuite) TestConnectorReadAll() {
	// Check if the connector supports source capabilities
	if !suite.caps.Source {
		suite.T().Skip("Skipping test because this connector does not support source capabilities")
	}

	// Do some prep
	testConnectorID := iface.ConnectorID{ID: "1234"}
	flowID := iface.FlowID{ID: "1234"}
	dataChannelID := iface.DataChannelID{ID: "4321"}
	dataChannel := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{}
	suite.t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	suite.c.On("NotifyDone", flowID, testConnectorID).Return(nil)
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

	// Test reading all data from a source
	// We'll run this with a timeout to make sure it's non-blocking
	err := RunWithTimeout(suite.T(), suite.connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.DataChannelID))
	}, NonBlockingTimeout,
		flowID, options, dataChannelID)
	assert.NoError(suite.T(), err)

	// Sleep for a bit
	suite.T().Log("Sleeping for 10 seconds to allow the connector to read data")
	time.Sleep(10 * time.Second)

	// The connector should still be running right now and reading data - let's interrupt it
	err = RunWithTimeout(suite.T(), suite.connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
	}, NonBlockingTimeout, flowID)
	assert.NoError(suite.T(), err)

	// Sleep for 1 second to ensure that the interruption took effect
	time.Sleep(1 * time.Second)

	// We should have gotten some data in the channel
	assert.Greater(suite.T(), messageCount, 0)
	// A notification should have been sent to the coordinator that the job is done
	suite.c.AssertCalled(suite.T(), "NotifyDone", flowID, testConnectorID)
}

func (suite *ConnectorTestSuite) TestMongoConnector() {
	// Setup the transport mock
	// Setup the coordinator mock

	// Setup the connector

	// Start reading to channel

	// Tear it down

	// Test setup
	//ctx := context.Background()
	// t := &mockTransport{}
	// err := suite.connector.Setup(ctx, t)
	// assert.NoError(suite.T(), err)

	// // Test teardown
	// suite.connector.Teardown()

	// // Test connector capabilities
	// capabilities := iface.ConnectorCapabilities{
	// 	Source: true,
	// 	Sink:   true,
	// }
	// suite.connector.SetParameters(capabilities)

	// // Test read from channel
	// flowID := "test-flow"
	// options := iface.ConnectorOptions{
	// 	Namespace: []string{"test-namespace"},
	// }
	// dataChannel := "test-channel"
	// err = suite.connector.StartReadToChannel(flowID, options, dataChannel)
	// assert.NoError(suite.T(), err)

	// // Test write from channel
	// err = suite.connector.StartWriteFromChannel(flowID, dataChannel)
	// assert.NoError(suite.T(), err)

	// // Test data integrity check
	// err = suite.connector.RequestDataIntegrityCheck(flowID, options)
	// assert.NoError(suite.T(), err)

	// // Test connector status
	status := suite.connector.GetConnectorStatus(iface.FlowID{})
	assert.NotNil(suite.T(), status)
}
