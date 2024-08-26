/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package coordinatorSimple

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/mitchellh/hashstructure"

	"github.com/adiom-data/dsync/protocol/iface"
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
const flowStateMetadataStore = "flow_state"

// hash base
const hashBase = 16

type FlowDetails struct {
	FlowID     iface.FlowID
	Options    iface.FlowOptions
	flowStatus iface.FlowStatus

	dataChannels []iface.DataChannelID

	doneNotificationChannels   []chan struct{}                                // for connectors to let us know they're done with the flow
	integrityCheckDoneChannels []chan iface.ConnectorDataIntegrityCheckResult // for connectors to post the results of the integrity check (this can be a continious stream in the future, hence a channel)

	flowDone chan struct{} // for everyone else to know the flow is done

	ReadPlan         iface.ConnectorReadPlan // read plan for the flow
	readPlanningDone chan struct{}           // for source connector to let us know they're done with read planning

	Resumable bool
}

// Generates static flow ID based on the flow options which should be unique across the board
// XXX: is this the right place for this?
func generateFlowID(options iface.FlowOptions) iface.FlowID {
	id, err := hashstructure.Hash(options, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the flow options: %v", err))
	}
	return iface.FlowID(strconv.FormatUint(id, hashBase))
}

func updateFlowTaskStatus(flowDetails *FlowDetails, taskId iface.ReadPlanTaskID, taskStatus uint) error {
	for i, task := range flowDetails.ReadPlan.Tasks {
		if task.Id == taskId {
			flowDetails.ReadPlan.Tasks[i].Status = taskStatus
			return nil
		}
	}
	return fmt.Errorf("task with ID %d not found", taskId)
}

func updateFlowTaskData(flowDetails *FlowDetails, taskId iface.ReadPlanTaskID, taskData *iface.TaskDoneMeta) error {
	if taskData == nil {
		return nil // nothing to update
	}

	for i, task := range flowDetails.ReadPlan.Tasks {
		if task.Id == taskId {
			flowDetails.ReadPlan.Tasks[i].DocsCopied = taskData.DocsCopied
			return nil
		}
	}
	return fmt.Errorf("task with ID %d not found", taskId)
}
