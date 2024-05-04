package iface

import "context"

type ConnectorType struct {
	DbType  string
	Version string
}

type ConnectorCapabilities struct {
	Source bool
	Sink   bool
}

// General Connector Interface
type Connector interface {
	Setup(ctx context.Context, t Transport) error
	Run() error
	Teardown()

	ConnectorICoordinatorSignal
}

// Singalling Connector Interface for use by Coordinator
type ConnectorICoordinatorSignal interface {
	SetParameters(reqCap ConnectorCapabilities)
}
