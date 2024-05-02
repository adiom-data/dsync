package iface

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
	Setup(t Transport, s Statestore)
	Teardown()

	// User
	FlowCreate(src ConnectorID, dst ConnectorID, o FlowOptions) FlowID
	FlowStart(fid FlowID)
	FlowStop(fid FlowID)

	CoordinatorIConnectorSignal
}

// Singalling coordinator interface for use by connectors
type CoordinatorIConnectorSignal interface {
	// Register a connector with type, capabilities, and endpoint for its signalling interface
	RegisterConnector(ctype ConnectorType, ccap ConnectorCapabilities, cep ConnectorICoordinatorSignal) ConnectorID
	DelistConnector(ConnectorID)
}
