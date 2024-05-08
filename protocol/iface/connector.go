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
	//TODO: shoud these have flow id?
	StartReadToChannel(flowId FlowID, dataChannel chan<- DataMessage)    // Read data into the provided channel (async)
	StartWriteFromChannel(flowId FlowID, dataChannel chan<- DataMessage) // Write data from the provided channel (async)
}
