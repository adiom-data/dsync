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
* Check that a resumable reader correctly resumes plan execution after an interruption
*
* Scenario:
* 1) Interrupt after a certain number of completed tasks or barriers
* 2) Resume
* 3a) Ensure that none of the already executed task ids were executed again
* 3b) Ensure that all the unexecuted task IDs were executed
 */
func (suite *ConnectorTestSuite) TestConnectorReadResumeInitialCopy() {
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

	// Check if the connector supports resume capabilities
	if !caps.Resumability {
		//XXX: should we check that setting the capabilities fails?
		suite.T().Skip("Skipping test because this connector does not support resume capabilities")
	}

	// Tell the connector to turn it on
	reqCaps := caps
	reqCaps.Resumability = true
	connector.SetParameters(iface.FlowID("1234"), reqCaps)

	// Preset the phase variables
	var readPlan iface.ConnectorReadPlan
	phase := 0
	flowComplete := make(chan struct{})
	allTasksComplete := make(chan struct{})
	completedTasks := make(map[iface.ReadPlanTaskID]bool) //for easier tracking of completed tasks

	// Do some prep
	flowID := iface.FlowID("1234")
	dataChannelID := iface.DataChannelID("4321")
	dataChannelID2 := iface.DataChannelID("43210")
	dataChannel := make(chan iface.DataMessage)
	dataChannel2 := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{}
	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	t.On("GetDataChannelEndpoint", dataChannelID2).Return(dataChannel2, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil).Run(func(args mock.Arguments) {
		flowComplete <- struct{}{}
	})
	c.On("NotifyTaskDone", flowID, testConnectorID, mock.AnythingOfType("iface.ReadPlanTaskID")).Return(nil).Run(func(args mock.Arguments) {
		taskID := args.Get(2).(iface.ReadPlanTaskID)
		// assert that the task was not already completed before
		assert.False(suite.T(), completedTasks[taskID], "Task should not have been completed before")

		completedTasks[taskID] = true

		// Set the task as completed in the readplan
		for i, task := range readPlan.Tasks {
			if task.Id == taskID {
				readPlan.Tasks[i].Status = iface.ReadPlanTaskStatus_Completed
				break
			}
		}

		// Also catch if all the tasks have been completed
		if len(completedTasks) == len(readPlan.Tasks) {
			close(allTasksComplete)
		}
	})
	messageCount := 0

	// Start a go routine to read from the data channel until it's closed
	dataReader := func(channel chan iface.DataMessage) {
		for {
			msg, ok := <-channel
			if !ok {
				break
			}

			if msg.MutationType != iface.MutationType_Barrier {
				// This is a data message
				messageCount++
				if phase == 0 && messageCount == 24 {
					// let's interrupt the flow
					err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
						return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
					}, NonBlockingTimeout, flowID)
					assert.NoError(suite.T(), err)
				}
			}
		}
	}
	go dataReader(dataChannel)

	// Generate a read plan
	// We'll need to implement a mock for the completion function to store the read plan
	readPlanComplete := make(chan struct{})
	c.On("PostReadPlanningResult", flowID, testConnectorID, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		readPlanRes := args.Get(2).(iface.ConnectorReadPlanResult) // Perform a type assertion here
		assert.True(suite.T(), readPlanRes.Success, "Read planning should have succeeded")
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

	// Wait for flow to complete after the first interruption
	select {
	case <-flowComplete:
		// Read plan is complete
	case <-time.After(FlowCompletionTimeout):
		// Timeout
		suite.T().Errorf("Timed out while waiting for the flow to complete after interruption")
		suite.T().FailNow()
	}

	// Set the phase to 1
	phase = 1
	// Start a go routine to read from the data channel until it's closed
	go dataReader(dataChannel2)

	// Continue the flow
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.ConnectorReadPlan), args[3].(iface.DataChannelID))
	}, NonBlockingTimeout,
		flowID, options, readPlan, dataChannelID2)
	assert.NoError(suite.T(), err)

	// Wait for all the tasks to complete
	select {
	case <-allTasksComplete:
		// Read plan is complete
	case <-time.After(FlowCompletionTimeout):
		// Timeout
		suite.T().Errorf("Timed out while waiting for all the tasks to complete after the resume")
		suite.T().FailNow()
	}

	// Let's interrupt it
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
	}, NonBlockingTimeout, flowID)
	assert.NoError(suite.T(), err)

	// Wait for the flow to complete after the second interruption
	select {
	case <-flowComplete:
		// Read plan is complete
	case <-time.After(FlowCompletionTimeout):
		// Timeout
		suite.T().Errorf("Timed out while waiting for the flow to complete after the second interruption")
		suite.T().FailNow()
	}

	// We should have gotten some data in the channel
	assert.True(suite.T(), messageCount > 0, "Should have read some data")

	// A notification should have been sent to the coordinator that the job is done
	c.AssertCalled(suite.T(), "NotifyDone", flowID, testConnectorID)

	connector.Teardown()
}

/*
* Check that a resumable reader sends CDC barriers with the correct (same) resume token after an interruption when there were no writes in between
*
* Scenario:
* 1) Interrupt after a first CDC barrier
* 2) Resume
* 3) Check that the next CDC barrier with the resume token equivalent to the one in plan (since there were no writes in between)
 */
func (suite *ConnectorTestSuite) TestConnectorReadResumeCDC() {
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

	// Check if the connector supports resume capabilities
	if !caps.Resumability {
		//XXX: should we check that setting the capabilities fails?
		suite.T().Skip("Skipping test because this connector does not support resume capabilities")
	}

	// Tell the connector to turn it on
	reqCaps := caps
	reqCaps.Resumability = true
	connector.SetParameters(iface.FlowID("1234"), reqCaps)

	// Preset the phase variables
	var readPlan iface.ConnectorReadPlan
	phase := 0
	flowComplete := make(chan struct{})

	// Do some prep
	flowID := iface.FlowID("1234")
	dataChannelID := iface.DataChannelID("4321")
	dataChannelID2 := iface.DataChannelID("43210")
	dataChannel := make(chan iface.DataMessage)
	dataChannel2 := make(chan iface.DataMessage)
	options := iface.ConnectorOptions{}
	t.On("GetDataChannelEndpoint", dataChannelID).Return(dataChannel, nil)
	t.On("GetDataChannelEndpoint", dataChannelID2).Return(dataChannel2, nil)
	c.On("NotifyDone", flowID, testConnectorID).Return(nil).Run(func(args mock.Arguments) {
		flowComplete <- struct{}{}
	})
	c.On("NotifyTaskDone", flowID, testConnectorID, mock.AnythingOfType("iface.ReadPlanTaskID")).Return(nil)
	messageCount := 0
	secondBarrierReceived := false

	// Start a go routine to read from the data channel until it's closed
	dataReader := func(channel chan iface.DataMessage) {
		for {
			msg, ok := <-channel
			if !ok {
				break
			}

			if msg.MutationType != iface.MutationType_Barrier {
				// This is a data message
				messageCount++
			} else {
				// it's a barrier
				if msg.BarrierType == iface.BarrierType_CdcResumeTokenUpdate {
					// let's interrupt the flow
					err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
						return receiver.(iface.ConnectorICoordinatorSignal).Interrupt(args[0].(iface.FlowID))
					}, NonBlockingTimeout, flowID)
					assert.NoError(suite.T(), err)
					if phase == 1 {
						secondBarrierReceived = true
						// Check that the CDC resume token is the same as it is in the plan
						assert.Equal(suite.T(), msg.BarrierCdcResumeToken, readPlan.CdcResumeToken, "CDC resume token should be the same as in the plan")
					}
				}
			}
		}
	}
	go dataReader(dataChannel)

	// Generate a read plan
	// We'll need to implement a mock for the completion function to store the read plan
	readPlanComplete := make(chan struct{})
	c.On("PostReadPlanningResult", flowID, testConnectorID, mock.Anything).Return(nil).Run(func(args mock.Arguments) {
		readPlanRes := args.Get(2).(iface.ConnectorReadPlanResult) // Perform a type assertion here
		assert.True(suite.T(), readPlanRes.Success, "Read planning should have succeeded")
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

	// Wait for flow to complete after the first interruption (the first CDC barrier)
	select {
	case <-flowComplete:
		// Read plan is complete
	case <-time.After(FlowCompletionTimeout):
		// Timeout
		suite.T().Errorf("Timed out while waiting for the flow to complete - first interruption")
		suite.T().FailNow()
	}

	// Set the phase to 1
	phase = 1
	// Start a go routine to read from the data channel until it's closed
	go dataReader(dataChannel2)

	// Continue the flow
	// We'll run this with a timeout to make sure it's non-blocking
	err = RunWithTimeout(suite.T(), connector, func(receiver interface{}, args ...interface{}) error {
		return receiver.(iface.Connector).StartReadToChannel(args[0].(iface.FlowID), args[1].(iface.ConnectorOptions), args[2].(iface.ConnectorReadPlan), args[3].(iface.DataChannelID))
	}, NonBlockingTimeout,
		flowID, options, readPlan, dataChannelID2)
	assert.NoError(suite.T(), err)

	// Wait for flow to complete after the second interruption (the first CDC barrier)
	select {
	case <-flowComplete:
		// Read plan is complete
	case <-time.After(FlowCompletionTimeout):
		// Timeout
		suite.T().Errorf("Timed out while waiting for the flow to complete after the second interruption")
		suite.T().FailNow()
	}

	// We should have gotten some data in the channel
	assert.True(suite.T(), messageCount > 0, "Should have read some data")

	// Assert that we saw the second barrier
	assert.True(suite.T(), secondBarrierReceived, "Never received the second CDC barrier")

	connector.Teardown()
}
