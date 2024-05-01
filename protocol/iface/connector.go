package iface

type ConnectorType struct {
	DbType string
}

type ConnectorCapabilities struct {
	Source bool
	Sink   bool
}

type Connector interface {
	// General
	Setup(t Transport)
	Teardown()

	// Coordinator
	SetParameters(reqCap ConnectorCapabilities)
}
