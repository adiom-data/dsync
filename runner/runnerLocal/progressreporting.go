/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package runnerLocal

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
)

type RunnerSyncProgress struct {
	StartTime time.Time
	CurrTime  time.Time
	SyncState string

	TotalNamespaces        int64
	NumNamespacesCompleted int64

	NumDocsSynced int64

	ChangeStreamEvents int64
	DeletesCaught      uint64

	Throughput    float64
	NsProgressMap map[iface.Namespace]*iface.NamespaceStatus
	Namespaces    []iface.Namespace //use map and get the keys so print order is consistent

	TasksTotal     int64
	TasksStarted   int64
	TasksCompleted int64

	Lag int64

	VerificationResult string

	SrcAdditionalStateInfo string
}

// Update the runner progress struct with the latest progress metrics from the flow status
func (r *RunnerLocal) UpdateRunnerProgress() {
	if r.activeFlowID == iface.FlowID("") { //no active flow - probably not active yet
		return
	}

	flowStatus, err := r.coord.GetFlowStatus(r.activeFlowID)
	if err != nil {
		slog.Error("Failed to get flow status", err)
		return
	}
	srcStatus := flowStatus.SrcStatus

	if r.runnerProgress.SyncState != iface.VerifySyncState && r.runnerProgress.SyncState != iface.CleanupSyncState { //XXX: if we're cleaning up or verifying, we don't want to overwrite the state
		r.runnerProgress.SyncState = srcStatus.SyncState
	}
	r.runnerProgress.SrcAdditionalStateInfo = srcStatus.AdditionalInfo

	r.runnerProgress.CurrTime = time.Now()
	r.runnerProgress.NumNamespacesCompleted = srcStatus.ProgressMetrics.NumNamespacesCompleted
	r.runnerProgress.TotalNamespaces = srcStatus.ProgressMetrics.NumNamespaces
	r.runnerProgress.NumDocsSynced = srcStatus.ProgressMetrics.NumDocsSynced
	r.runnerProgress.NsProgressMap = srcStatus.ProgressMetrics.NamespaceProgress

	r.runnerProgress.Namespaces = srcStatus.ProgressMetrics.Namespaces
	r.runnerProgress.TasksTotal = srcStatus.ProgressMetrics.TasksTotal
	r.runnerProgress.TasksStarted = srcStatus.ProgressMetrics.TasksStarted
	r.runnerProgress.TasksCompleted = srcStatus.ProgressMetrics.TasksCompleted
	r.runnerProgress.ChangeStreamEvents = srcStatus.ProgressMetrics.ChangeStreamEvents
	r.runnerProgress.DeletesCaught = srcStatus.ProgressMetrics.DeletesCaught

	// update replication lag
	if flowStatus.SrcStatus.CDCActive {
		eventsDiff := flowStatus.SrcStatus.WriteLSN - flowStatus.DstStatus.WriteLSN
		if eventsDiff < 0 {
			eventsDiff = 0
		}
		r.runnerProgress.Lag = eventsDiff
	}
}

// Get the latest runner progress struct
func (r *RunnerLocal) GetRunnerProgress() RunnerSyncProgress {
	return r.runnerProgress
}

// Loops to update the status and throughput metrics for the runner progress struct
func (r *RunnerLocal) updateRunnerSyncThroughputRoutine(throughputUpdateInterval time.Duration) {
	r.UpdateRunnerProgress()
	ticker := time.NewTicker(throughputUpdateInterval)
	currTime := time.Now()
	totaloperations := 0 + r.runnerProgress.NumDocsSynced + r.runnerProgress.ChangeStreamEvents + int64(r.runnerProgress.DeletesCaught)
	nsProgress := make(map[iface.Namespace]int64)
	for ns, nsStatus := range r.runnerProgress.NsProgressMap {
		if nsStatus != nil {
			nsProgress[ns] = atomic.LoadInt64(&nsStatus.DocsCopied)
		}
	}

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.UpdateRunnerProgress()
			elapsed := time.Since(currTime).Seconds()
			operationsNew := r.runnerProgress.NumDocsSynced + r.runnerProgress.ChangeStreamEvents + int64(r.runnerProgress.DeletesCaught)

			total_operations_delta := operationsNew - totaloperations

			r.runnerProgress.Throughput = float64(total_operations_delta) / elapsed

			for ns, nsStatus := range r.runnerProgress.NsProgressMap {
				operationsNew := atomic.LoadInt64(&nsStatus.DocsCopied)
				operationsDelta := operationsNew - nsProgress[ns]
				nsStatus.Throughput = float64(operationsDelta) / elapsed
				nsProgress[ns] = operationsNew
			}
			currTime = time.Now()
			totaloperations = operationsNew
		}
	}
}
