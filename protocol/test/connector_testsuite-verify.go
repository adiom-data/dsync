/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package test

import (
	"context"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// check that the connector can perform a data integrity check
func (suite *ConnectorTestSuite) TestConnectorDataIntegrityCheck() {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID("3")
	var caps iface.ConnectorCapabilities
	c.On("RegisterConnector", mock.Anything, mock.Anything).Return(testConnectorID, nil).Run(func(args mock.Arguments) {
		// Store advertised connector capabilities to skip irrelevant tests
		caps = args.Get(0).(iface.ConnectorDetails).Cap // Perform a type assertion here
	})

	// create a new connector object
	connector := suite.connectorFactoryFunc()

	// setup the connector and make sure it returns no errors
	err := connector.Setup(ctx, t)
	assert.NoError(suite.T(), err)

	// check that the mocked methods were called
	t.AssertExpectations(suite.T())
	c.AssertExpectations(suite.T())

	// Check if the connector supports integrity check capabilities
	if !caps.IntegrityCheck {
		// Check that the method fails first
		err := connector.RequestDataIntegrityCheck(iface.FlowID("3234"), iface.ConnectorReadPlan{}, iface.ConnectorOptions{})
		assert.Error(suite.T(), err, "Should fail to perform a data integrity check if the connector does not support integrity check capabilities")
		suite.T().Skip("Skipping test because this connector does not support integrity check capabilities")
	}

	// Do some prep
	flowID := iface.FlowID("3234")
	options := iface.ConnectorOptions{}
	c.On("PostDataIntegrityCheckResult", flowID, testConnectorID, mock.AnythingOfType("iface.ConnectorDataIntegrityCheckResult")).Return(nil)

	// Test performing a data integrity check
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).RequestDataIntegrityCheck(args[0].(iface.FlowID), iface.ConnectorReadPlan{}, args[1].(iface.ConnectorOptions))
	}, NonBlockingTimeout,
		flowID, options)
	assert.NoError(suite.T(), err)

	// A notification should have been sent to the coordinator that the check is done
	c.AssertCalled(suite.T(), "PostDataIntegrityCheckResult", flowID, testConnectorID, mock.AnythingOfType("iface.ConnectorDataIntegrityCheckResult"))

	connector.Teardown()
}
