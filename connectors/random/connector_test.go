/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package random

import (
	"testing"

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
