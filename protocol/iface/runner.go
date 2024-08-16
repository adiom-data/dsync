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

type Runner interface {
	// General
	Setup(ctx context.Context) error
	Run()
	Teardown()
	// Progress reporting
	GetProgress(flowId FlowID)
}

type RunnerSyncStatus struct {
	StartTime time.Time
	CurrTime  time.Time
	SyncState string

	TotalNamespaces        int64
	NumNamespacesCompleted int64

	NumDocsSynced int64

	ChangeStreamEvents int64
	DeletesCaught      uint64

	Throughput    float64
	NsProgressMap map[Namespace]*NameSpaceStatus
	Namespaces    []Namespace //use map and get the keys so print order is consistent

	TasksTotal     int64
	TasksStarted   int64
	TasksCompleted int64
}
