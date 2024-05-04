package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
)

type SimpleCoordinator struct {
	// Implement the necessary fields here
	ctx context.Context

	connectors    map[iface.ConnectorID]ConnectorDetailsWithEp
	mu_connectors sync.RWMutex // to make the map thread-safe
}

func NewSimpleCoordinator() *SimpleCoordinator {
	// Implement the NewSimpleCoordinator function
	return &SimpleCoordinator{connectors: make(map[iface.ConnectorID]ConnectorDetailsWithEp)}
}

// *****
// Thread-safe methods to work with items in the connectors map
// *****

// deletes a connector from the map
func (c *SimpleCoordinator) delConnector(cid iface.ConnectorID) {
	c.mu_connectors.Lock()
	defer c.mu_connectors.Unlock()
	delete(c.connectors, cid)
}

// adds a connector with a unqiue ID and returns the ID
func (c *SimpleCoordinator) addConnector(connector ConnectorDetailsWithEp) iface.ConnectorID {
	c.mu_connectors.Lock()
	defer c.mu_connectors.Unlock()

	var cid iface.ConnectorID

	for {
		cid = generateConnectorID()
		if _, ok := c.connectors[cid]; !ok {
			break
		}
	}

	connector.Details.Id = cid // set the ID in the details for easier operations later
	c.connectors[cid] = connector

	return cid
}

// return a list of ConnectorDetails for all known connectors
func (c *SimpleCoordinator) GetConnectors() []iface.ConnectorDetails {
	c.mu_connectors.RLock()
	defer c.mu_connectors.RUnlock()
	connectors := make([]iface.ConnectorDetails, 0, len(c.connectors))
	for _, details := range c.connectors {
		connectors = append(connectors, details.Details)
	}
	return connectors
}

// *****
// Implement the Coordinator interface methods
// *****

func (c *SimpleCoordinator) Setup(ctx context.Context, t iface.Transport, s iface.Statestore) {
	// Implement the Setup method
	c.ctx = ctx
}

func (c *SimpleCoordinator) Run() error {
	slog.Info("SimpleCoordinator is running...")
	sleep, cancel := context.WithTimeout(c.ctx, time.Second*10)
	defer cancel()
	<-sleep.Done()
	return nil
}

func (c *SimpleCoordinator) Teardown() {
	// Implement the Teardown method
}

func (c *SimpleCoordinator) RegisterConnector(details iface.ConnectorDetails, cep iface.ConnectorICoordinatorSignal) (iface.ConnectorID, error) {
	slog.Info("Registering connector with details: " + fmt.Sprintf("%v", details))

	// Check that the details.Id is empty, otherwise error
	if details.Id.ID != "" {
		return iface.ConnectorID{}, fmt.Errorf("we don't support re-registering connectors yet")
	}

	// Add the connector to the list
	cid := c.addConnector(ConnectorDetailsWithEp{Details: details, Endpoint: cep})
	slog.Debug("assigned connector ID: " + cid.ID)

	// Implement the RegisterConnector method
	return cid, nil
}

func (c *SimpleCoordinator) DelistConnector(cid iface.ConnectorID) {
	slog.Info("Deregistering connector with ID: " + fmt.Sprintf("%v", cid))

	// Implement the DelistConnector method
	c.delConnector(cid)
}

func (c *SimpleCoordinator) FlowCreate(src iface.ConnectorID, dst iface.ConnectorID, o iface.FlowOptions) iface.FlowID {
	// Implement the FlowCreate method
	return iface.FlowID{}
}

func (c *SimpleCoordinator) FlowStart(fid iface.FlowID) {
	// Implement the FlowStart method
}

func (c *SimpleCoordinator) FlowStop(fid iface.FlowID) {
	// Implement the FlowStop method
}
