/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package iface

import (
	"context"
	"sync/atomic"
)

type ConnectorType struct {
	DbType  string
	Version string
	Spec    string
}

// Supported mode of operation
// If something is announced as true, it can be later turned off through the setParameters call
// But if something is announced as not supported (false), it can't be turned on
type ConnectorCapabilities struct {
	Source         bool
	Sink           bool
	IntegrityCheck bool
	Resumability   bool
}

// XXX (AK, 6/2024): not sure if it logically belongs here or to another iface file
type ConnectorDataIntegrityCheckResult struct {
	Digest string
	Count  int64

	Success bool
}

type ConnectorReadPlanResult struct {
	ReadPlan ConnectorReadPlan

	Success bool
}

type ConnectorStatus struct {
	// last sequence number for writes
	/**
	For the source, it's the last write sequence number read from the change stream
	For the destination, indicates last one that was written
	*/
	WriteLSN int64
	// For the source, indicates whether the change stream is active
	CDCActive bool

	SyncState string // "InitialSync", "ChangeStream", "ReadPlan", "Cleanup", "Verification"

	//progress reporting attributes
	NamespaceProgress map[string]*NameSpaceStatus //map key is namespace: "db.col"
	Namespaces        []Namespace

	EstimatedTotalDocCount int64

	ProgressMetrics ProgressMetrics
}

type ProgressMetrics struct {
	NumNamespaces       int64
	NumNamespacesSynced int64 //change name

	NumDocsSynced      int64
	ChangeStreamEvents int64
	DeletesCaught      uint64

	TasksTotal     int64
	TasksStarted   int64
	TasksCompleted int64
}
type Namespace struct {
	Db  string
	Col string
}

type NameSpaceStatus struct {
	EstimatedDocCount int64
	Throughput        float64
	Tasks             []ReadPlanTask //all the tasks for the namespace
	TasksCompleted    atomic.Int64
	TasksStarted      atomic.Int64
	DocsCopied        atomic.Int64
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
	SetParameters(flowId FlowID, reqCap ConnectorCapabilities) // Set parameters for the flow //XXX: should this be allowed to return an error?

	RequestCreateReadPlan(flowId FlowID, options ConnectorOptions) error                                                     // Request planning (async) //XXX: we could not do it explicitly and just post to coordinator lazily whenever we create the plan
	StartReadToChannel(flowId FlowID, options ConnectorOptions, readPlan ConnectorReadPlan, dataChannel DataChannelID) error // Read data into the provided channel (async)
	StartWriteFromChannel(flowId FlowID, dataChannel DataChannelID) error                                                    // Write data from the provided channel (async)
	Interrupt(flowId FlowID) error                                                                                           // Interrupt the flow (async)

	RequestDataIntegrityCheck(flowId FlowID, options ConnectorOptions) error // Request a data integrity check based on a read plan (async)

	GetConnectorStatus(flowId FlowID) ConnectorStatus // Immediate and non-blocking
}
