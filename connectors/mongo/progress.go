package mongo

import (
	"fmt"
	"log/slog"
	"math"
	"sync"

	"github.com/adiom-data/dsync/protocol/iface"
)

// Tracks progress of the tasks
type ProgressTracker struct {
	muProgressMetrics sync.Mutex
	Status            iface.ConnectorStatus
}

// Initializes and returns a new ProgressTracker
func NewProgressTracker() *ProgressTracker {
	progressMetrics := iface.ProgressMetrics{
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
		Status: iface.ConnectorStatus{
			WriteLSN: 0,
			ProgressMetrics: progressMetrics,
		},
	}
}

// restoreProgressDetails restores the progress metrics from the persisted tasks and progress
func (pt *ProgressTracker) RestoreProgressDetails(tasks []iface.ReadPlanTask) { 
	slog.Debug("Restoring progress metrics from tasks")
	pt.Status.ProgressMetrics.TasksTotal = int64(len(tasks))
	for _, task := range tasks {
		ns := iface.Namespace{Db: task.Def.Db, Col: task.Def.Col}
		nsStatus := pt.Status.ProgressMetrics.NamespaceProgress[ns]
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
			pt.Status.ProgressMetrics.NamespaceProgress[ns] = nsStatus
		}
		nsStatus.Tasks = append(nsStatus.Tasks, task)
		nsStatus.EstimatedDocCount += task.EstimatedDocCount
		//if the task is completed, update the document counters
		if task.Status == iface.ReadPlanTaskStatus_Completed {
			pt.Status.ProgressMetrics.TasksCompleted++
			pt.Status.ProgressMetrics.NumDocsSynced += task.DocsCopied

			nsStatus.TasksCompleted++
			nsStatus.DocsCopied += task.DocsCopied

			nsStatus.EstimatedDocsCopied += task.EstimatedDocCount
		}
	}
	pt.Status.ProgressMetrics.NumNamespaces = int64(len(pt.Status.ProgressMetrics.NamespaceProgress))

	for ns, nsStatus := range pt.Status.ProgressMetrics.NamespaceProgress {
		if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
			pt.Status.ProgressMetrics.NumNamespacesCompleted++
		}
		pt.Status.ProgressMetrics.Namespaces = append(pt.Status.ProgressMetrics.Namespaces, ns)
	}

	slog.Debug(fmt.Sprintf("Restored progress metrics: %+v", pt.Status.ProgressMetrics))
}

// update estimated namespace doc counts from the actual database
func (pt *ProgressTracker) ResetNsProgressEstimatedDocCounts(mc *BaseMongoConnector) error {
	for ns, nsStatus := range pt.Status.ProgressMetrics.NamespaceProgress {
		collection := mc.Client.Database(ns.Db).Collection(ns.Col)
		count, err := collection.EstimatedDocumentCount(mc.Ctx)
		if err != nil {
			return fmt.Errorf("failed to count documents: %v", err)
		}
		nsStatus.EstimatedDocCount = int64(count)
	}
	return nil
}

// Updates the progress metrics once a task has been started
func (pt *ProgressTracker) TaskStartedProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	pt.muProgressMetrics.Lock()
	nsStatus.ActiveTasksList[taskId] = true
	pt.Status.ProgressMetrics.TasksStarted++
	nsStatus.TasksStarted++
	pt.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been started
func (pt *ProgressTracker) TaskInProgressUpdate(nsStatus *iface.NamespaceStatus) {
	pt.muProgressMetrics.Lock()
	nsStatus.DocsCopied++
	nsStatus.EstimatedDocsCopied++
	pt.Status.ProgressMetrics.NumDocsSynced++
	pt.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been completed
func (pt *ProgressTracker) TaskDoneProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	pt.muProgressMetrics.Lock()
	// update progress counters: num tasks completed
	pt.Status.ProgressMetrics.TasksCompleted++
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
		pt.Status.ProgressMetrics.NumNamespacesCompleted++
	}
	// decrement the tasks started counter
	pt.Status.ProgressMetrics.TasksStarted--
	nsStatus.TasksStarted--
	// remove the task from the active tasks list
	delete(nsStatus.ActiveTasksList, taskId)
	pt.muProgressMetrics.Unlock()
}