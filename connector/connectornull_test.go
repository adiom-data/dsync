package connector

import (
	"testing"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/test"
	"github.com/stretchr/testify/suite"
)

// Standard test suite for the connector interface
func TestNullConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(func() iface.Connector {
		return NewNullConnector("test")
	})
	suite.Run(t, tSuite)
}
