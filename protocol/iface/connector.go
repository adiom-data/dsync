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

// Signalling Connector Interface for use by Coordinator
type ConnectorICoordinatorSignal interface {
	SetParameters(reqCap ConnectorCapabilities) // Set the capabilities requested by the Coordinator

	//TODO: these should get the actual channel pointers from the transport layer
	StartReadToChannel(dataChannel chan<- DataMessage)    // Read data into the provided channel (async)
	StartWriteFromChannel(dataChannel chan<- DataMessage) // Write data from the provided channel (async)
}
