package transport

import (
	"github.com/adiom-data/dsync/protocol/iface"
)

type TransportLocal struct {
	coordEP iface.CoordinatorIConnectorSignal
}

func (t *TransportLocal) NewTransportLocal(coordEP iface.CoordinatorIConnectorSignal) *TransportLocal {
	t.coordEP = coordEP
	return t
}

func (t *TransportLocal) GetCoordinatorEndpoint(location string) iface.CoordinatorIConnectorSignal {
	// Implement the logic to get the coordinator endpoint
	// For example:
	return t.coordEP
}
