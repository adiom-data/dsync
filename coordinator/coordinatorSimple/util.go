/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package coordinatorSimple

import (
	"fmt"
	"strconv"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/google/uuid"
	"github.com/mitchellh/hashstructure"
)

type ConnectorDetailsWithEp struct {
	Details  iface.ConnectorDetails
	Endpoint iface.ConnectorICoordinatorSignal
}

func generateConnectorID() iface.ConnectorID {
	id := uuid.New()
	return iface.ConnectorID(id.String())
}

// name for the flow state store in metadata
const FLOW_STATE_METADATA_STORE = "flow_state"

type FlowDetails struct {
	FlowID     iface.FlowID
	Options    iface.FlowOptions
	flowStatus iface.FlowStatus

	dataChannels []iface.DataChannelID

	doneNotificationChannels   []chan struct{}                                //for connectors to let us know they're done with the flow
	integrityCheckDoneChannels []chan iface.ConnectorDataIntegrityCheckResult //for connectors to post the results of the integrity check (this can be a continious stream in the future, hence a channel)

	flowDone chan struct{} //for everyone else to know the flow is done

	ReadPlan         iface.ConnectorReadPlan //read plan for the flow
	readPlanningDone chan struct{}           //for source connector to let us know they're done with read planning
}

// Generates static flow ID based on the flow options which should be unique across the board
// XXX: is this the right place for this?
func generateFlowID(o iface.FlowOptions) iface.FlowID {
	id, err := hashstructure.Hash(o, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.FlowID(strconv.FormatUint(id, 16))
}

func updateFlowTaskStatus(flowDet *FlowDetails, taskId iface.ReadPlanTaskID, taskStatus uint) error {
	for i, task := range flowDet.ReadPlan.Tasks {
		if task.Id == taskId {
			flowDet.ReadPlan.Tasks[i].Status = taskStatus
			return nil
		}
	}
	return fmt.Errorf("task with ID %d not found", taskId)
}
