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
	Setup()
	Teardown()

	// Coordinator
	SetParameters(capActual ConnectorCapabilities)
}
