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

type Connector interface {
	// General
	Setup(ctx context.Context, t Transport) error
	Teardown()

	// Coordinator
	SetParameters(reqCap ConnectorCapabilities)
}
