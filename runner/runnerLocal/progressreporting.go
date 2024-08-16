/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package runnerLocal

import (
	"log/slog"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
)

// Update the runner progress struct with the latest progress metrics from the flow status
func (r *RunnerLocal) UpdateRunnerProgress(flowId iface.FlowID) {
	flowStatus, err := r.coord.GetFlowStatus(flowId)
	if err != nil {
		slog.Error("Failed to get flow status", err)
		return
	}
	srcStatus := flowStatus.SrcStatus

	r.runnerProgress.SyncState = srcStatus.SyncState
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
}
