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
	connectorFactoryFunc func() iface.Connector
}

func NewConnectorTestSuite(connectorFunc func() iface.Connector) *ConnectorTestSuite {
	// lo := logger.Options{Verbosity: "DEBUG"}
	// logger.Setup(lo)

	suite := new(ConnectorTestSuite)
	suite.connectorFactoryFunc = connectorFunc
	return suite
}

func (suite *ConnectorTestSuite) SetupSuite() {
}

func (suite *ConnectorTestSuite) TearDownSuite() {
}

func (suite *ConnectorTestSuite) TestConnectorReadAll() {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID{ID: "1234"}
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
		suite.T().Skip("Skipping test because this connector does not support source capabilities")
	}

	// Do some prep
	flowID := iface.FlowID{ID: "1234"}
	dataChannelID := iface.DataChannelID{ID: "4321"}
	dataChannel := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{}
	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil)
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
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.DataChannelID))
	}, NonBlockingTimeout,
		flowID, options, dataChannelID)
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
	assert.Greater(suite.T(), messageCount, 0)
	// A notification should have been sent to the coordinator that the job is done
	c.AssertCalled(suite.T(), "NotifyDone", flowID, testConnectorID)

	connector.Teardown()
}

func (suite *ConnectorTestSuite) TestConnectorWrite() {

}
