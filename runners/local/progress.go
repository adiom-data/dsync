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
	SourceDescription      string
	DestinationDescription string

	StartTime time.Time // start of the sync process
	CurrTime  time.Time // current time
	SyncState string    // current state of the sync process

	TotalNamespaces        int64 // total number of namespaces to sync
	NumNamespacesCompleted int64 // number of namespaces completed

	NumDocsSynced int64 // number of documents synced

	ChangeStreamEvents int64  // number of change stream events processed
	DeletesCaught      uint64 // number of deletes caught

	Throughput    float64                                    // throughput in operations per second
	NsProgressMap map[iface.Namespace]*iface.NamespaceStatus `json:"-"` // map key is namespace: "db.col"
	Namespaces    []iface.Namespace                          //use map and get the keys so print order is consistent

	TasksTotal     int64 // total number of tasks
	TasksStarted   int64 // number of tasks started
	TasksCompleted int64 // number of tasks completed

	Lag int64 // replication lag as number of events

	VerificationResult string // verification result (if running in the verify mode)

	AdditionalStateInfo string // additional state info

	NamespaceVerifyProgresses []iface.FlowIntegrityStatus
}

// Update the runner progress struct with the latest progress metrics from the flow status
func (r *RunnerLocal) UpdateRunnerProgress() {
	r.rpMutex.Lock()
	defer r.rpMutex.Unlock()
	if r.activeFlowID == iface.FlowID("") { //no active flow - probably not active yet
		return
	}

	flowStatus, err := r.coord.GetFlowStatus(r.activeFlowID)
	if err != nil {
		slog.Error("Failed to get flow status", "err", err)
		return
	}
	srcStatus := flowStatus.SrcStatus
	stateInfo := ""
	allTasksCompleted := flowStatus.Status.TasksDone == flowStatus.Status.TasksTotal

	switch {
	case r.runnerProgress.SyncState == iface.VerifySyncState || r.runnerProgress.SyncState == iface.CleanupSyncState:
		// Do nothing, keep the current state
	case srcStatus.SyncState == iface.ChangeStreamSyncState && !allTasksCompleted:
		// Source is already in the change stream mode but not all tasks were fully completed
		r.runnerProgress.SyncState = iface.InitialSyncSyncState
		stateInfo += "Finalizing initial sync... "
	default:
		// Just use the source state
		r.runnerProgress.SyncState = srcStatus.SyncState
	}

	stateInfo += srcStatus.AdditionalInfo

	r.runnerProgress.AdditionalStateInfo = stateInfo

	r.runnerProgress.CurrTime = time.Now()
	r.runnerProgress.TotalNamespaces = srcStatus.ProgressMetrics.NumNamespaces
	r.runnerProgress.NumDocsSynced = srcStatus.ProgressMetrics.NumDocsSynced

	// HACK: currently we just copy any old throughput over to the new copy
	// Find a better way to do this later
	for k, v := range srcStatus.ProgressMetrics.NamespaceProgress {
		if old, ok := r.runnerProgress.NsProgressMap[k]; ok {
			v.Throughput = old.Throughput
		}
	}
	r.runnerProgress.NsProgressMap = srcStatus.ProgressMetrics.NamespaceProgress

	r.runnerProgress.NumNamespacesCompleted = 0
	for ns, m := range flowStatus.StatusByNamespace {
		progress := r.runnerProgress.NsProgressMap[ns]
		if progress == nil {
			slog.Warn("progress map does not exist- skipping", "ns", ns)
			continue
		}
		progress.TasksCompleted = m.TasksDone
		if m.TasksDone == m.TasksTotal {
			r.runnerProgress.NumNamespacesCompleted += 1
		}
	}

	r.runnerProgress.Namespaces = srcStatus.ProgressMetrics.Namespaces
	r.runnerProgress.TasksTotal = flowStatus.Status.TasksTotal
	r.runnerProgress.TasksStarted = srcStatus.ProgressMetrics.TasksStarted
	r.runnerProgress.TasksCompleted = flowStatus.Status.TasksDone
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

	if r.runnerProgress.SyncState == iface.VerifySyncState {
		statuses, err := r.coord.GetFlowIntegrityStatus(r.activeFlowID)
		if err != nil {
			slog.Error("Failed to get flow integrity status", "err", err)
			return
		}
		r.runnerProgress.NamespaceVerifyProgresses = statuses
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
