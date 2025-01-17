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
* Contains tests related to reversibility
 */

/*
* Check that a cdc-only flow works
* And the the read plan for cdc-only flows is generated correctly
* It should have the resume token and no tasks
*
* Scenario:
* 1) Start a flow in cdc-only mode
* 2) Check the read plan
* 3) Interrupt after the fist message on the data channel (that would be the first CDC barrier or the first write)
 */
func (suite *ConnectorTestSuite) TestConnectorCDCOnly() {
	ctx := context.Background()

	// create mocks for the transport and coordinator
	t := new(mocks.Transport)
	c := new(mocks.Coordinator)

	// transport should return the mock coordinator endpoint
	t.On("GetCoordinatorEndpoint", mock.Anything).Return(c, nil)
	// coordinator should return a connector ID on registration
	testConnectorID := iface.ConnectorID("1")
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

	// Check if the connector supports source capabilities
	if !caps.Source {
		// Check that the method fails first
		err := connector.StartReadToChannel(iface.FlowID("1234"), iface.ConnectorOptions{}, iface.ConnectorReadPlan{}, iface.DataChannelID("4321"))
		assert.Error(suite.T(), err, "Should fail to read data from a source if the connector does not support source capabilities")
		suite.T().Skip("Skipping test because this connector does not support source capabilities")
	}

	// Preset the phase variables
	var readPlan iface.ConnectorReadPlan
	flowComplete := make(chan struct{})

	// Do some prep
	flowID := iface.FlowID("1234")
	dataChannelID := iface.DataChannelID("4321")
	dataChannelID2 := iface.DataChannelID("43210")
	dataChannel := make(chan iface.DataMessage)
	dataChannel2 := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{Mode: iface.SyncModeCDC, Namespace: []string{NamespaceString()}} //CDC-only mode
	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	t.On("GetDataChannelEndpoint", dataChannelID2).Return(dataChannel2, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil).Run(func(args mock.Arguments) {
		flowComplete <- struct{}{}
	})

	// Start a go routine to read from the data channel until it's closed
	dataReader := func(channel chan iface.DataMessage) {
		for {
			_, ok := <-channel
			if !ok {
				break
			}

			err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
				return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
			}, NonBlockingTimeout, flowID)
			assert.NoError(suite.T(), err)
		}
	}
	go dataReader(dataChannel)

	// Generate a read plan
	// We'll need to implement a mock for the completion function to store the read plan
	readPlanComplete := make(chan struct{})
	c.On("PostReadPlanningResult", flowID, testConnectorID, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		readPlanRes := args.Get(2).(iface.ConnectorReadPlanResult) // Perform a type assertion here
		assert.True(suite.T(), readPlanRes.Success, "Read planning should have succeeded")
		// the tasks should be empty
		assert.Empty(suite.T(), readPlanRes.ReadPlan.Tasks, "Read plan should have no tasks")
		// the resume token should be set
		assert.NotEmpty(suite.T(), readPlanRes.ReadPlan.CdcResumeToken, "Read plan should have a resume token")
		readPlan = readPlanRes.ReadPlan
		close(readPlanComplete)
	})
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).RequestCreateReadPlan(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions))
	}, NonBlockingTimeout,
		flowID, options)
	assert.NoError(suite.T(), err)
	// wait for the read plan to be generated
	select {
	case <-readPlanComplete:
		// Read plan is complete
	case <-time.After(ReadPlanningTimeout):
		// Timeout after read planning timeout
		suite.T().Errorf("Timed out while waiting for the read plan")
		suite.T().FailNow()
	}

	// Test reading all data from a source
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.ConnectorReadPlan), args[3].(iface.DataChannelID))
	}, NonBlockingTimeout,
		flowID, options, readPlan, dataChannelID)
	assert.NoError(suite.T(), err)

	// Wait for flow to complete after the interruption (the first CDC barrier)
	select {
	case <-flowComplete:
		// Read plan is complete
	case <-time.After(FlowCompletionTimeout):
		// Timeout
		suite.T().Errorf("Timed out while waiting for the flow to complete")
		suite.T().FailNow()
	}

	connector.Teardown()
}
