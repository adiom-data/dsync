/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package iface

import (
	"context"
)

type ConnectorID string

// General coordinator interface
type Coordinator interface {
	// General
	Setup(ctx context.Context, t Transport, s Statestore)
	Teardown()

	// User
	GetConnectors() []ConnectorDetails

	FlowGetOrCreate(FlowOptions) (FlowID, error)                                                                    // Get or create a flow if it doesn't exist
	FlowStart(FlowID) error                                                                                         // Start the flow or resume it
	FlowStop(FlowID)                                                                                                // Stop the flow
	FlowDestroy(FlowID)                                                                                             // Destroy the flow and the associated metadata (also cleans up persisted state)
	WaitForFlowDone(FlowID) error                                                                                   // Wait for the flow to be done
	PerformFlowIntegrityCheck(context.Context, FlowID, IntegrityCheckOptions) (FlowDataIntegrityCheckResult, error) // Perform an integrity check on the flow (synchronous)
	GetFlowStatus(FlowID) (FlowStatus, error)                                                                       // Get the status of the flow
	GetFlowIntegrityStatus(FlowID) ([]FlowIntegrityStatus, error)                                                   // Status for integrity check

	CoordinatorIConnectorSignal
}

type FlowIntegrityStatus struct {
	Namespace      string
	TasksTotal     int
	TasksCompleted int
}

type IntegrityCheckOptions struct {
	QuickCount bool
}

type ConnectorDetails struct {
	Id   ConnectorID
	Desc string
	Type ConnectorType
	Cap  ConnectorCapabilities
}

// Abstraction for the read plan
type ConnectorReadPlan struct {
	Tasks          []ReadPlanTask
	CdcResumeToken []byte // for cdc - we could generalize it as a task and the whole sequence as a DAG or something similar
	CreatedAtEpoch int64  // real plan at this epoch time - used as a fallback for change stream creation
}

type ReadPlanTask struct {
	Id     ReadPlanTaskID //should always start with 1 to avoid confusion with an uninitialized value
	Status uint

	//XXX: this should be interface{} - a connector-specific task definition (implementation-specific) but making simple for now
	Def struct {
		Db  string
		Col string

		// all are optional and define the range of documents to copy for the task
		PartitionKey string      // partition key for the task
		Low          interface{} // lower bound for the task (inclusive)
		High         interface{} // upper bound for the task (exclusive)
	}

	//some metrics for reporting
	EstimatedDocCount int64 // estimated number of documents in the task
	DocsCopied        int64 // number of documents copied
}

type TaskDoneMeta struct {
	DocsCopied int64
}

type ReadPlanTaskID uint

const (
	ReadPlanTaskStatus_New = iota
	ReadPlanTaskStatus_Completed
)

// Signalling coordinator interface for use by connectors
type CoordinatorIConnectorSignal interface {
	// Register a connector with type, capabilities, and endpoint for its signalling interface
	RegisterConnector(details ConnectorDetails, cep ConnectorICoordinatorSignal) (ConnectorID, error)
	DelistConnector(ConnectorID)

	// Done event for a flow (for a connector to announce that they finished the flow)
	NotifyDone(flowId FlowID, conn ConnectorID) error

	// Done event for a task (for a connector to announce that they finished a task)
	// Accepts the optional taskData parameter which is a connector-specific task data to be persisted along the task
	NotifyTaskDone(flowId FlowID, conn ConnectorID, taskId ReadPlanTaskID, taskData *TaskDoneMeta) error

	// Planning completion event (for a connector to share the read plan)
	PostReadPlanningResult(flowId FlowID, conn ConnectorID, res ConnectorReadPlanResult) error

	// Update the status of the connector
	UpdateConnectorStatus(flowId FlowID, conn ConnectorID, status ConnectorStatus) error

	// Post new CDC resume token for a flow
	UpdateCDCResumeToken(flowId FlowID, conn ConnectorID, resumeToken []byte) error
}
