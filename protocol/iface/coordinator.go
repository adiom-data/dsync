package iface

type ConnectorID struct {
	ID string
}

type FlowID struct {
	ID string
}

type FlowOptions struct {
}

type Coordinator interface {
	// General
	Setup(t Transport, s Statestore)
	Teardown()

	// Connector
	RegisterConnector(ctype ConnectorType, ccap ConnectorCapabilities) ConnectorID
	DelistConnector(ConnectorID)

	// User
	FlowCreate(src ConnectorID, dst ConnectorID, o FlowOptions) FlowID
	FlowStart(fid FlowID)
	FlowStop(fid FlowID)
}
