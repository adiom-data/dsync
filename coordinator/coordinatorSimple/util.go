/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package coordinatorSimple

import (
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/google/uuid"
)

type ConnectorDetailsWithEp struct {
	Details  iface.ConnectorDetails
	Endpoint iface.ConnectorICoordinatorSignal
}

func generateConnectorID() iface.ConnectorID {
	id := uuid.New()
	return iface.ConnectorID{ID: id.String()}
}

type FlowDetails struct {
	FlowID     iface.FlowID
	Options    iface.FlowOptions
	flowStatus iface.FlowStatus

	DataChannels []iface.DataChannelID

	DoneNotificationChannels   []chan struct{}                                //for connectors to let us know they're done with the flow
	IntegrityCheckDoneChannels []chan iface.ConnectorDataIntegrityCheckResult //for connectors to post the results of the integrity check (this can be a continious stream in the future, hence a channel)

	flowDone chan struct{} //for everyone else to know the flow is done

	ReadPlan         iface.ConnectorReadPlan //read plan for the flow
	readPlanningDone chan struct{}           //for source connector to let us know they're done with read planning
}

func generateFlowID() iface.FlowID {
	id := uuid.New()
	return iface.FlowID{ID: id.String()}
}
