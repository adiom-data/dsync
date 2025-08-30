/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package random

import (
	"testing"
	"time"

	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/pkg/test"
	"github.com/stretchr/testify/suite"
)

func TestRandomConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite("test", func() adiomv1connect.ConnectorServiceClient {
		return test.ClientFromHandler(NewConn(ConnectorSettings{}))
	}, nil, nil, 1, 500)
	tSuite.SkipDuplicateTest = true
	suite.Run(t, tSuite)
}

func TestRandomV2ConnectorSuite(t *testing.T) {
	tSuite := test.NewConnectorTestSuite("ns0", func() adiomv1connect.ConnectorServiceClient {
		return test.ClientFromHandler(NewConnV2(ConnV2Input{
			NamespacePrefix:                 "ns",
			NumNamespaces:                   1,
			NumPartitionsPerNamespace:       1,
			NumUpdatePartitionsPerNamespace: 0,
			BatchSize:                       10,
			UpdateBatchSize:                 5,
			MaxUpdatesPerTick:               20,
			NumDocsPerPartition:             500,
			UpdateDuration:                  time.Millisecond,
			StreamTick:                      time.Millisecond,
		}))
	}, nil, nil, 50, 500)
	suite.Run(t, tSuite)
}
