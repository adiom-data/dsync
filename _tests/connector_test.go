package test

import (
	"testing"

	"github.com/adiom-data/dsync/connector"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

/**
Defines a generic test suite for implementations of the connector interface
Implementations will have their own tests that hook into this suite
*/

type ConnectorTestSuite struct {
	suite.Suite
	connector iface.Connector
}

func (suite *ConnectorTestSuite) SetupTest() {
	suite.connector = connector.NewMongoConnector("", connector.MongoConnectorSettings{})
}

func (suite *ConnectorTestSuite) TestMongoConnector() {
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

func TestConnectorSuite(t *testing.T) {
	suite.Run(t, new(ConnectorTestSuite))
}
