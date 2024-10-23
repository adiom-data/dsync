/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package null

import (
	"testing"

	"github.com/adiom-data/dsync/connectors/common"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/test"
	"github.com/stretchr/testify/suite"
)

// Standard test suite for the connector interface
func TestNullConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(func() iface.Connector {
		return common.NewLocalConnector("test", NewConn(), common.ConnectorSettings{})
	}, nil)
	suite.Run(t, tSuite)
}
