package transport

import (
	"errors"

	"github.com/adiom-data/dsync/protocol/iface"
)

type TransportLocal struct {
	coordEP iface.CoordinatorIConnectorSignal
}

func NewTransportLocal(coordEP iface.CoordinatorIConnectorSignal) *TransportLocal {
	return &TransportLocal{coordEP: coordEP}
}

func (t *TransportLocal) GetCoordinatorEndpoint(location string) (iface.CoordinatorIConnectorSignal, error) {
	if location != "local" {
		return nil, errors.New("local transport only supports the 'local' location")
	}
	return t.coordEP, nil
}

func (t *TransportLocal) CreateDataChannel() (iface.DataChannel, error) {
	return iface.DataChannel{}, nil
}

func (t *TransportLocal) CloseDataChannel(id string) {
}
