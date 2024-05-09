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
	Teardown()

	ConnectorICoordinatorSignal
}

// Signalling Connector Interface for use by Coordinator
type ConnectorICoordinatorSignal interface {
	SetParameters(reqCap ConnectorCapabilities) // Set the capabilities requested by the Coordinator

	StartReadToChannel(flowId FlowID, dataChannel DataChannelID) error    // Read data into the provided channel (async)
	StartWriteFromChannel(flowId FlowID, dataChannel DataChannelID) error // Write data from the provided channel (async)
}
