package connectorRandom

import (
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/test"
	"github.com/stretchr/testify/suite"
)

// Standard test suite for the connector interface
func TestRandomConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(func() iface.Connector {
		return NewRandomReadConnector("test", RandomConnectorSettings{})
	})
	suite.Run(t, tSuite)
}
