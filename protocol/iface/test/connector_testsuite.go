package test

import (
	"context"

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
}

func NewConnectorTestSuite(connector iface.Connector) *ConnectorTestSuite {
	suite := new(ConnectorTestSuite)
	suite.connector = connector
	return suite
}

func (suite *ConnectorTestSuite) SetupTest() {
	//suite.connector = connector.NewMongoConnector("", connector.MongoConnectorSettings{})
}

/**
* Make sure that the connector can be setup correctly
* It should obtain the coordinator endpoint from the transport
* And register itself with the coordinator
**/
func (suite *ConnectorTestSuite) TestConnectorSetup() {
	ctx := context.Background()
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	testConnectorID := "1234"
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(iface.ConnectorID{ID: testConnectorID}, nil)
	err := suite.connector.Setup(ctx, t)
	assert.NoError(suite.T(), err)

	t.AssertExpectations(suite.T())
	c.AssertExpectations(suite.T())
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
