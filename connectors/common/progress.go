package common

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
)

// Tracks progress of the tasks
type ProgressTracker struct {
	muProgressMetrics sync.Mutex
	status            *iface.ConnectorStatus
	ctx               context.Context
}

// Initializes and returns a new ProgressTracker
func NewProgressTracker(status *iface.ConnectorStatus, ctx context.Context) *ProgressTracker {
	status.ProgressMetrics = iface.ProgressMetrics{
		NumDocsSynced:          0,
		TasksTotal:             0,
		TasksStarted:           0,
		TasksCompleted:         0,
		NumNamespaces:          0,
		NumNamespacesCompleted: 0,
		ChangeStreamEvents:     0,

		NamespaceProgress: make(map[iface.Namespace]*iface.NamespaceStatus),
		Namespaces:        make([]iface.Namespace, 0),
	}

	return &ProgressTracker{
		status: status,
		ctx:    ctx,
	}
}

func (pt *ProgressTracker) CDCActive() {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	pt.status.CDCActive = true
}

func (pt *ProgressTracker) UpdateSyncState(newState string) {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	pt.status.SyncState = newState
}

// restoreProgressDetails restores the progress metrics from the persisted tasks and progress
func (pt *ProgressTracker) RestoreProgressDetails(tasks []iface.ReadPlanTask) {
	slog.Debug("Restoring progress metrics from tasks")
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	pt.status.ProgressMetrics.TasksTotal = int64(len(tasks))
	for _, task := range tasks {
		ns := iface.Namespace{Db: task.Def.Db, Col: task.Def.Col}
		nsStatus := pt.status.ProgressMetrics.NamespaceProgress[ns]
		//check if the namespace status exists, if not create it
		if nsStatus == nil {
			nsStatus = &iface.NamespaceStatus{
				EstimatedDocCount:   0,
				Throughput:          0,
				Tasks:               []iface.ReadPlanTask{},
				TasksCompleted:      0,
				TasksStarted:        0,
				DocsCopied:          0,
				EstimatedDocsCopied: 0,
				ActiveTasksList:     make(map[iface.ReadPlanTaskID]bool),
			}
			pt.status.ProgressMetrics.NamespaceProgress[ns] = nsStatus
		}
		nsStatus.Tasks = append(nsStatus.Tasks, task)
		nsStatus.EstimatedDocCount += task.EstimatedDocCount
		//if the task is completed, update the document counters
		if task.Status == iface.ReadPlanTaskStatus_Completed {
			pt.status.ProgressMetrics.TasksCompleted++
			pt.status.ProgressMetrics.NumDocsSynced += task.DocsCopied

			nsStatus.TasksCompleted++
			nsStatus.DocsCopied += task.DocsCopied

			nsStatus.EstimatedDocsCopied += task.EstimatedDocCount
		}
	}
	pt.status.ProgressMetrics.NumNamespaces = int64(len(pt.status.ProgressMetrics.NamespaceProgress))

	for ns, nsStatus := range pt.status.ProgressMetrics.NamespaceProgress {
		if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
			pt.status.ProgressMetrics.NumNamespacesCompleted++
		}
		pt.status.ProgressMetrics.Namespaces = append(pt.status.ProgressMetrics.Namespaces, ns)
	}

	slog.Debug(fmt.Sprintf("Restored progress metrics: %+v", pt.status.ProgressMetrics))
}

// update estimated namespace doc counts from the actual database
func (pt *ProgressTracker) ResetNsProgressEstimatedDocCounts(counter func(context.Context, iface.Namespace) (int64, error)) error {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	for ns, nsStatus := range pt.status.ProgressMetrics.NamespaceProgress {
		count, err := counter(pt.ctx, ns)
		if err != nil {
			return fmt.Errorf("failed to count documents: %v", err)
		}
		nsStatus.EstimatedDocCount = count
	}
	return nil
}

// Updates the progress metrics once a task has been started
func (pt *ProgressTracker) TaskStartedProgressUpdate(ns iface.Namespace, taskId iface.ReadPlanTaskID) {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	nsStatus := pt.status.ProgressMetrics.NamespaceProgress[ns]
	nsStatus.ActiveTasksList[taskId] = true
	pt.status.ProgressMetrics.TasksStarted++
	nsStatus.TasksStarted++
}

// Updates the progress metrics once a task has been started
func (pt *ProgressTracker) TaskInProgressUpdate(ns iface.Namespace, inc int64) {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	nsStatus := pt.status.ProgressMetrics.NamespaceProgress[ns]
	nsStatus.DocsCopied += inc
	nsStatus.EstimatedDocsCopied += inc
	pt.status.ProgressMetrics.NumDocsSynced += inc
}

// Updates the progress metrics once a task has been completed
func (pt *ProgressTracker) TaskDoneProgressUpdate(ns iface.Namespace, taskId iface.ReadPlanTaskID) {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	nsStatus := pt.status.ProgressMetrics.NamespaceProgress[ns]
	// update progress counters: num tasks completed
	pt.status.ProgressMetrics.TasksCompleted++
	nsStatus.TasksCompleted++

	// go through all the tasks
	// - mark ours as completed
	// - calculate the approximate number of docs copied based on per-task estimates
	approxDocsCopied := int64(0)
	for i, task := range nsStatus.Tasks {
		if task.Id == taskId {
			nsStatus.Tasks[i].Status = iface.ReadPlanTaskStatus_Completed
		}
		if task.Status == iface.ReadPlanTaskStatus_Completed {
			approxDocsCopied += task.EstimatedDocCount
		}
	}
	// update the estimated docs copied count for the namespace to keep percentage proportional
	nsStatus.EstimatedDocsCopied = int64(math.Max(float64(nsStatus.EstimatedDocsCopied), float64(approxDocsCopied)))
	// check if namespace has been completed
	if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
		pt.status.ProgressMetrics.NumNamespacesCompleted++
	}
	// decrement the tasks started counter
	pt.status.ProgressMetrics.TasksStarted--
	nsStatus.TasksStarted--
	// remove the task from the active tasks list
	delete(nsStatus.ActiveTasksList, taskId)
}

// update changeStreamEvents progress counters atomically
func (pt *ProgressTracker) UpdateChangeStreamProgressTracking() {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	pt.status.ProgressMetrics.ChangeStreamEvents++
}

// update deletes caught progress counters atomically
func (pt *ProgressTracker) UpdateChangeStreamProgressTrackingDeletes() {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	pt.status.ProgressMetrics.DeletesCaught++
}

func (pt *ProgressTracker) UpdateWriteLSN(lsn int64) {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	pt.status.WriteLSN = max(pt.status.WriteLSN, lsn)
}

func (pt *ProgressTracker) CopyStatus() iface.ConnectorStatus {
	pt.muProgressMetrics.Lock()
	defer pt.muProgressMetrics.Unlock()
	statusCopy := *pt.status
	statusCopy.ProgressMetrics.NamespaceProgress = map[iface.Namespace]*iface.NamespaceStatus{}
	for k, v := range pt.status.ProgressMetrics.NamespaceProgress {
		progressCopy := *v
		progressCopy.ActiveTasksList = map[iface.ReadPlanTaskID]bool{}
		for k2, v2 := range v.ActiveTasksList {
			progressCopy.ActiveTasksList[k2] = v2
		}
		statusCopy.ProgressMetrics.NamespaceProgress[k] = &progressCopy
	}
	return statusCopy
}
