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
	t   iface.Transport
	s   iface.Statestore

	connectors    map[iface.ConnectorID]ConnectorDetailsWithEp
	mu_connectors sync.RWMutex // to make the map thread-safe

	flows    map[iface.FlowID]FlowDetails
	mu_flows sync.RWMutex // to make the map thread-safe
}

func NewSimpleCoordinator() *SimpleCoordinator {
	// Implement the NewSimpleCoordinator function
	return &SimpleCoordinator{connectors: make(map[iface.ConnectorID]ConnectorDetailsWithEp)}
}

// *****
// Thread-safe methods to work with items in the connectors map
// *****

// gets a connector by id
func (c *SimpleCoordinator) getConnector(cid iface.ConnectorID) (ConnectorDetailsWithEp, bool) {
	c.mu_connectors.RLock()
	defer c.mu_connectors.RUnlock()
	connector, ok := c.connectors[cid]
	return connector, ok
}

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
// Thread-safe methods to work with items in the flows map
// *****

// gets a flow by id
func (c *SimpleCoordinator) getFlow(fid iface.FlowID) (FlowDetails, bool) {
	c.mu_flows.RLock()
	defer c.mu_flows.RUnlock()
	flow, ok := c.flows[fid]
	return flow, ok
}

// adds a flow and returns the ID
func (c *SimpleCoordinator) addFlow(details FlowDetails) iface.FlowID {
	c.mu_flows.Lock()
	defer c.mu_flows.Unlock()

	var fid iface.FlowID

	for {
		fid = generateFlowID()
		if _, ok := c.flows[fid]; !ok {
			break
		}
	}

	details.FlowID = fid // set the ID in the details for easier operations later
	c.flows[fid] = details

	return fid
}

// removes a flow from the map
func (c *SimpleCoordinator) delFlow(fid iface.FlowID) {
	c.mu_flows.Lock()
	defer c.mu_flows.Unlock()
	delete(c.flows, fid)
}

// *****
// Implement the Coordinator interface methods
// *****

func (c *SimpleCoordinator) Setup(ctx context.Context, t iface.Transport, s iface.Statestore) {
	// Implement the Setup method
	c.ctx = ctx
	c.t = t
	c.s = s
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

func (c *SimpleCoordinator) FlowCreate(o iface.FlowOptions) (iface.FlowID, error) {
	// Check flow type and error out if not unidirectional
	if o.Type != iface.UnidirectionalFlowType {
		return iface.FlowID{}, fmt.Errorf("only unidirectional flows are supported")
	}

	dc, err := c.t.CreateDataChannel()
	if err != nil {
		return iface.FlowID{}, fmt.Errorf("failed to create data channel: %v", err)
	}

	fdet := FlowDetails{
		Options:     o,
		DataChannel: dc,
	}
	fid := c.addFlow(fdet)

	return fid, nil
}

func (c *SimpleCoordinator) FlowStart(fid iface.FlowID) {
	// Implement the FlowStart method
}

func (c *SimpleCoordinator) FlowStop(fid iface.FlowID) {
	// Implement the FlowStop method
}

func (c *SimpleCoordinator) FlowDestroy(fid iface.FlowID) {
	// Get the flow details
	flowDet, err := c.getFlow(fid)
	if !err {
		slog.Error("Flow not found", fid)
	}
	// close the data channel
	c.t.CloseDataChannel(flowDet.DataChannel.ID)
	// remove the flow from the map
	c.delFlow(fid)
}
