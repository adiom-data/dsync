// Copyright (c) 2024. Adiom, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package random

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/test"
)

// Standard test suite for the connector interface
func TestRandomConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(func() iface.Connector {
		return NewRandomReadConnector("test", RandomConnectorSettings{})
	}, nil)
	suite.Run(t, tSuite)
}
