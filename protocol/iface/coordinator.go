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

	FlowGetOrCreate(o FlowOptions) (FlowID, error)                              // Get or create a flow if it doesn't exist
	FlowStart(fid FlowID) error                                                 // Start the flow or resume it
	FlowStop(fid FlowID)                                                        // Stop the flow
	FlowDestroy(fid FlowID)                                                     // Destroy the flow and the associated metadata (also cleans up persisted state)
	WaitForFlowDone(flowId FlowID) error                                        // Wait for the flow to be done
	PerformFlowIntegrityCheck(fid FlowID) (FlowDataIntegrityCheckResult, error) // Perform an integrity check on the flow (synchronous)
	GetFlowStatus(fid FlowID) (FlowStatus, error)                               // Get the status of the flow

	CoordinatorIConnectorSignal
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
}

type ReadPlanTask struct {
	Id     ReadPlanTaskID //should always start with 1 to avoid confusion with an uninitialized value
	Status uint

	//XXX: this should be interface{} - a connector-specific task definition (implementation-specific) but making simple for now
	Def struct {
		Db  string
		Col string
	}
}

type ReadPlanTaskID uint

const (
	ReadPlanTaskStatus_New = iota
	ReadPlanTaskStatus_Completed
)

// Singalling coordinator interface for use by connectors
type CoordinatorIConnectorSignal interface {
	// Register a connector with type, capabilities, and endpoint for its signalling interface
	RegisterConnector(details ConnectorDetails, cep ConnectorICoordinatorSignal) (ConnectorID, error)
	DelistConnector(ConnectorID)

	// Done event for a flow (for a connector to announce that they finished the flow)
	NotifyDone(flowId FlowID, conn ConnectorID) error

	// Done event for a task (for a connector to announce that they finished a task)
	NotifyTaskDone(flowId FlowID, conn ConnectorID, taskId ReadPlanTaskID) error

	// Planning completion event (for a connector to share the read plan)
	PostReadPlanningResult(flowId FlowID, conn ConnectorID, res ConnectorReadPlanResult) error

	// Data integrity check completion event (for a connector to share results that they finished the integrity check)
	PostDataIntegrityCheckResult(flowId FlowID, conn ConnectorID, res ConnectorDataIntegrityCheckResult) error

	// Update the status of the connector
	UpdateConnectorStatus(flowId FlowID, conn ConnectorID, status ConnectorStatus) error

	// Post new CDC resume token for a flow
	UpdateCDCResumeToken(flowId FlowID, conn ConnectorID, resumeToken []byte) error
}
