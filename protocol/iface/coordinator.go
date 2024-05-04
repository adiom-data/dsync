package iface

import "context"

type ConnectorID struct {
	ID string
}

type FlowID struct {
	ID string
}

type FlowOptions struct {
}

// General coordinator interface
type Coordinator interface {
	// General
	Setup(ctx context.Context, t Transport, s Statestore)
	Run() error
	Teardown()

	// User
	GetConnectors() []ConnectorDetails

	FlowCreate(src ConnectorID, dst ConnectorID, o FlowOptions) FlowID
	FlowStart(fid FlowID)
	FlowStop(fid FlowID)

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
}
