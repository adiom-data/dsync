package iface

import "context"

type ConnectorID struct {
	ID string
}

// General coordinator interface
type Coordinator interface {
	// General
	Setup(ctx context.Context, t Transport, s Statestore)
	Run() error
	Teardown()

	// User
	GetConnectors() []ConnectorDetails

	FlowCreate(o FlowOptions) (FlowID, error)
	FlowStart(fid FlowID)
	FlowStop(fid FlowID)
	FlowDestroy(fid FlowID)

	CoordinatorIConnectorSignal
}

type ConnectorDetails struct {
	Id   ConnectorID
	Desc string
	Type ConnectorType
	Cap  ConnectorCapabilities
}

// Singalling coordinator interface for use by connectors
type CoordinatorIConnectorSignal interface {
	// Register a connector with type, capabilities, and endpoint for its signalling interface
	RegisterConnector(details ConnectorDetails, cep ConnectorICoordinatorSignal) (ConnectorID, error)
	DelistConnector(ConnectorID)

	//TODO: Done event? (for a specific job)
	//TODO should this have a flow id?
	NotifyDone(flowId FlowID, conn ConnectorID) error
}
