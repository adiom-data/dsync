/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package iface

import (
	"context"
	"time"
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

type IntegrityCheckQuery struct {
	Namespace string
	CountOnly bool

	PartitionKey string
	Low          interface{}
	High         interface{}
}

// XXX (AK, 6/2024): not sure if it logically belongs here or to another iface file
type ConnectorDataIntegrityCheckResult struct {
	XXHash uint64
	Count  int64
}

type ConnectorReadPlanResult struct {
	ReadPlan ConnectorReadPlan

	Success bool
	Error   error
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

	SyncState      string // "InitialSync", "ChangeStream", "ReadPlan", "Cleanup", "Verification"
	AdditionalInfo string // connector-specific additional info on the connector status

	ProgressMetrics ProgressMetrics // Progress Details for progress reporting interface, not required for all connectors
}

// XXX: we should separate the connector and the flow state instead of mixing them together
const (
	SetupSyncState        = "Setup"
	VerifySyncState       = "Verify"
	CleanupSyncState      = "Cleanup"
	ReadPlanningSyncState = "ReadPlanning"

	ChangeStreamSyncState = "ChangeStream"
	InitialSyncSyncState  = "InitialSync"
)

const (
	SyncModeFull = "Full"
	SyncModeCDC  = "CDC"
)

type ProgressMetrics struct {
	NumNamespaces          int64
	NumNamespacesCompleted int64 //change name

	NumDocsSynced      int64
	ChangeStreamEvents int64
	DeletesCaught      uint64

	TasksTotal     int64
	TasksStarted   int64
	TasksCompleted int64

	//progress reporting attributes
	NamespaceProgress map[Namespace]*NamespaceStatus //map key is namespace: "db.col"
	Namespaces        []Namespace

	LastChangeStreamTime time.Time
}

type Namespace struct {
	Db  string
	Col string
}

type NamespaceStatus struct {
	EstimatedDocCount   int64                   // Estimated number of documents in the namespace
	Throughput          float64                 // Throughput in operations per second
	Tasks               []ReadPlanTask          //all the tasks for the namespace
	TasksCompleted      int64                   //number of tasks completed
	TasksStarted        int64                   //number of tasks started
	DocsCopied          int64                   //number of documents copied
	EstimatedDocsCopied int64                   //this is for calculating percentage complete based on approximate number of docs per task, not actual number of docs copied
	ActiveTasksList     map[ReadPlanTaskID]bool //map of active task ids
}

// Pass options to use to the connector
type ConnectorOptions struct {
	Namespace []string
	Mode      string
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

	IntegrityCheck(ctx context.Context, task IntegrityCheckQuery) (ConnectorDataIntegrityCheckResult, error) // Request a data integrity check based on a read plan task

	GetConnectorStatus(flowId FlowID) ConnectorStatus // Immediate and non-blocking
}
