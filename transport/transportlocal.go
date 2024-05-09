package transport

import (
	"errors"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/google/uuid"
)

type TransportLocal struct {
	coordEP iface.CoordinatorIConnectorSignal

	dataChannels map[iface.DataChannelID]chan iface.DataMessage
	mu_dc        sync.RWMutex // to make the map thread-safe
}

func generateDataChannelID() iface.DataChannelID {
	id := uuid.New()
	return iface.DataChannelID{ID: id.String()}
}

func NewTransportLocal(coordEP iface.CoordinatorIConnectorSignal) *TransportLocal {
	dataChannels := make(map[iface.DataChannelID]chan iface.DataMessage)
	return &TransportLocal{coordEP: coordEP, dataChannels: dataChannels}
}

func (t *TransportLocal) GetCoordinatorEndpoint(location string) (iface.CoordinatorIConnectorSignal, error) {
	if location != "local" {
		return nil, errors.New("local transport only supports the 'local' location")
	}
	return t.coordEP, nil
}

func (t *TransportLocal) CreateDataChannel() (iface.DataChannelID, error) {
	t.mu_dc.Lock()
	defer t.mu_dc.Unlock()

	var cid iface.DataChannelID
	for {
		cid = generateDataChannelID()
		if _, ok := t.dataChannels[cid]; !ok {
			break
		}
	}

	channel := make(chan iface.DataMessage)
	t.dataChannels[cid] = channel
	return cid, nil
}

func (t *TransportLocal) CloseDataChannel(dcid iface.DataChannelID) {
	t.mu_dc.Lock()
	defer t.mu_dc.Unlock()

	if _, ok := t.dataChannels[dcid]; !ok {
		return
	}
	//close(t.dataChannels[dcid]) //unneccesary since we have a different signalling mechanism
	delete(t.dataChannels, dcid)
}

func (t *TransportLocal) GetDataChannelEndpoint(dcid iface.DataChannelID) (chan iface.DataMessage, error) {
	t.mu_dc.RLock()
	defer t.mu_dc.RUnlock()

	if channel, ok := t.dataChannels[dcid]; ok {
		return channel, nil
	}
	return nil, errors.New("data channel not found for ID: " + dcid.ID)
}
