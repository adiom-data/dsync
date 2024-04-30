package coordinator

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

	DoneNotificationChannels   []chan struct{}                                  //for connectors to let us know they're done with the flow
	IntegrityCheckDoneChannels []chan iface.ConnectorDataIntegrityCheckResponse //for connectors to post the results of the integrity check

	flowDone chan struct{} //for everyone else to know the flow is done
}

func generateFlowID() iface.FlowID {
	id := uuid.New()
	return iface.FlowID{ID: id.String()}
}
