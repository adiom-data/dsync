/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package test

import (
	"context"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

/*
* Check that the connector can perform a data integrity check and posts result to the coordinator
 */
func (suite *ConnectorTestSuite) TestConnectorDataIntegrityCheckPostResult() {
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
	checkComplete := make(chan struct{})
	c.On("PostDataIntegrityCheckResult", flowID, testConnectorID, mock.AnythingOfType("iface.ConnectorDataIntegrityCheckResult")).Return(nil).Run(func(args mock.Arguments) {
		// Notify that the check is complete
		checkComplete <- struct{}{}
	})

	// Test performing a data integrity check
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).RequestDataIntegrityCheck(args[0].(iface.FlowID), iface.ConnectorReadPlan{}, args[1].(iface.ConnectorOptions))
	}, NonBlockingTimeout,
		flowID, options)
	assert.NoError(suite.T(), err)

	// wait for the check to be complete
	select {
	case <-checkComplete:
		// check is complete
	case <-time.After(DataIntegrityCheckTimeout):
		// Timeout after data integrity check
		suite.T().Errorf("Timed out while waiting for the integrity check to complete")
		suite.T().FailNow()
	}

	// A notification should have been sent to the coordinator that the check is done
	c.AssertCalled(suite.T(), "PostDataIntegrityCheckResult", flowID, testConnectorID, mock.AnythingOfType("iface.ConnectorDataIntegrityCheckResult"))

	connector.Teardown()
}

/*
* Check that the connector can returns the same result for the same dataset assuming no changes in between
 */
func (suite *ConnectorTestSuite) TestConnectorDataIntegrityCheckResultConsistency() {
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
	phase := 0
	checkComplete := make(chan struct{})

	var checkResult iface.ConnectorDataIntegrityCheckResult
	c.On("PostDataIntegrityCheckResult", flowID, testConnectorID, mock.AnythingOfType("iface.ConnectorDataIntegrityCheckResult")).Return(nil).Run(func(args mock.Arguments) {
		// Check that the result is consistent
		result := args.Get(2).(iface.ConnectorDataIntegrityCheckResult)
		if phase == 0 {
			// First phase, store the result
			phase = 1
			checkResult = result
		} else {
			// Second phase, check that the result is the same
			assert.Equal(suite.T(), checkResult, result, "Data integrity check result should be consistent for the same dataset")
		}
		checkComplete <- struct{}{}
	})

	// Test performing a data integrity check
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).RequestDataIntegrityCheck(args[0].(iface.FlowID), iface.ConnectorReadPlan{}, args[1].(iface.ConnectorOptions))
	}, NonBlockingTimeout,
		flowID, options)
	assert.NoError(suite.T(), err)

	// wait for the check to be complete
	select {
	case <-checkComplete:
		// check is complete
	case <-time.After(DataIntegrityCheckTimeout):
		// Timeout after data integrity check
		suite.T().Errorf("Timed out while waiting for the first integrity check to complete")
		suite.T().FailNow()
	}

	// call the method again to check that the result is consistent
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).RequestDataIntegrityCheck(args[0].(iface.FlowID), iface.ConnectorReadPlan{}, args[1].(iface.ConnectorOptions))
	}, NonBlockingTimeout,
		flowID, options)
	assert.NoError(suite.T(), err)

	// wait for the check to be complete
	select {
	case <-checkComplete:
		// check is complete
	case <-time.After(DataIntegrityCheckTimeout):
		// Timeout after data integrity check
		suite.T().Errorf("Timed out while waiting for the second integrity check to complete")
		suite.T().FailNow()
	}

	// A notification should have been sent to the coordinator that the check is done
	c.AssertCalled(suite.T(), "PostDataIntegrityCheckResult", flowID, testConnectorID, mock.AnythingOfType("iface.ConnectorDataIntegrityCheckResult"))
	// The result callback should've been called twice
	c.AssertNumberOfCalls(suite.T(), "PostDataIntegrityCheckResult", 2)

	connector.Teardown()
}
