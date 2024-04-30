package iface

import "context"

type ConnectorType struct {
	DbType  string
	Version string
	Spec    string
}

type ConnectorCapabilities struct {
	Source bool
	Sink   bool
}

// XXX: not sure if it logically belongs here or to another iface file
type ConnectorDataIntegrityCheckResponse struct {
	Checksum string
	Count    int64

	Success bool
}

type ConnectorStatus struct {
	// last sequence number for writes
	WriteLSN  int64 //XXX: For the source, it's the last write sequence number read from the change stream. For the destination - last one that was written
	CDCActive bool  //XXX: For the source, it's whether the change stream is active
}

// Pass options to use to the connector
type ConnectorOptions struct {
	Namespace []string
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

	StartReadToChannel(flowId FlowID, options ConnectorOptions, dataChannel DataChannelID) error // Read data into the provided channel (async)
	StartWriteFromChannel(flowId FlowID, dataChannel DataChannelID) error                        // Write data from the provided channel (async)

	RequestDataIntegrityCheck(flowId FlowID, options ConnectorOptions) error // Request a data integrity check (async)

	GetConnectorStatus(flowId FlowID) ConnectorStatus // Immediate and non-blocking
}
