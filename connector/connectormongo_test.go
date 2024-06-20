package connector

import (
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/test"
	"github.com/stretchr/testify/suite"
)

const (
	// TestMongoConnectionString is the connection string for the test MongoDB
	TestMongoConnectionString = "mongodb://localhost:27017"
)

// Standard test suite for the connector interface
func TestMongoConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(func() iface.Connector {
		return NewMongoConnector("test", MongoConnectorSettings{ConnectionString: TestMongoConnectionString})
	})
	suite.Run(t, tSuite)
}