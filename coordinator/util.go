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
	FlowID      iface.FlowID
	Options     iface.FlowOptions
	DataChannel iface.DataChannel
}

func generateFlowID() iface.FlowID {
	id := uuid.New()
	return iface.FlowID{ID: id.String()}
}
