/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package random

import (
	"testing"
	"time"

	"github.com/adiom-data/dsync/connectors/common"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	test2 "github.com/adiom-data/dsync/pkg/test"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/test"
	"github.com/stretchr/testify/suite"
)

// Standard test suite for the connector interface
func TestRandomConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite(func() iface.Connector {
		return common.NewLocalConnector("test", NewConn(ConnectorSettings{}), common.ConnectorSettings{ResumeTokenUpdateInterval: 5 * time.Second})
	}, nil)
	tSuite.SkipIntegrity = true
	suite.Run(t, tSuite)
}

func TestRandomConnectorSuite2(t *testing.T) {
	tSuite := test2.NewConnectorTestSuite("test", func() adiomv1connect.ConnectorServiceClient {
		return test2.ClientFromHandler(NewConn(ConnectorSettings{}))
	}, nil, nil, 1, 500)
	tSuite.SkipDuplicateTest = true
	suite.Run(t, tSuite)
}
