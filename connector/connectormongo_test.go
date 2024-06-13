package connector

import (
	"testing"

	"github.com/adiom-data/dsync/test"
	"github.com/stretchr/testify/suite"
)

// Standaard test suite for the connector interface
func TestMongoConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(NewMongoConnector("", MongoConnectorSettings{}))
	suite.Run(t, tSuite)
}
