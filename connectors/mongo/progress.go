package mongo

import (
	"log/slog"
	"math"
	"fmt"
	"github.com/adiom-data/dsync/protocol/iface"
)

// restoreProgressDetails restores the progress metrics from the persisted tasks and progress
func RestoreProgressDetails(tasks []iface.ReadPlanTask, status *iface.ConnectorStatus) { 
	slog.Debug("Restoring progress metrics from tasks")
	status.ProgressMetrics.TasksTotal = int64(len(tasks))
	for _, task := range tasks {
		ns := iface.Namespace{Db: task.Def.Db, Col: task.Def.Col}
		nsStatus := status.ProgressMetrics.NamespaceProgress[ns]
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
			status.ProgressMetrics.NamespaceProgress[ns] = nsStatus
		}
		nsStatus.Tasks = append(nsStatus.Tasks, task)
		nsStatus.EstimatedDocCount += task.EstimatedDocCount
		//if the task is completed, update the document counters
		if task.Status == iface.ReadPlanTaskStatus_Completed {
			status.ProgressMetrics.TasksCompleted++
			status.ProgressMetrics.NumDocsSynced += task.DocsCopied

			nsStatus.TasksCompleted++
			nsStatus.DocsCopied += task.DocsCopied

			nsStatus.EstimatedDocsCopied += task.EstimatedDocCount
		}
	}
	status.ProgressMetrics.NumNamespaces = int64(len(status.ProgressMetrics.NamespaceProgress))

	for ns, nsStatus := range status.ProgressMetrics.NamespaceProgress {
		if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
			status.ProgressMetrics.NumNamespacesCompleted++
		}
		status.ProgressMetrics.Namespaces = append(status.ProgressMetrics.Namespaces, ns)
	}

	slog.Debug(fmt.Sprintf("Restored progress metrics: %+v", status.ProgressMetrics))
}

// update estimated namespace doc counts from the actual database
func ResetNsProgressEstimatedDocCounts(mc *Connector) error {
	for ns, nsStatus := range mc.status.ProgressMetrics.NamespaceProgress {
		collection := mc.client.Database(ns.Db).Collection(ns.Col)
		count, err := collection.EstimatedDocumentCount(mc.ctx)
		if err != nil {
			return fmt.Errorf("failed to count documents: %v", err)
		}
		nsStatus.EstimatedDocCount = int64(count)
	}
	return nil
}

// Updates the progress metrics once a task has been started
func (mc *Connector) taskStartedProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	mc.muProgressMetrics.Lock()
	nsStatus.ActiveTasksList[taskId] = true
	mc.status.ProgressMetrics.TasksStarted++
	nsStatus.TasksStarted++
	mc.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been started
func (mc *Connector) taskInProgressUpdate(nsStatus *iface.NamespaceStatus) {
	mc.muProgressMetrics.Lock()
	nsStatus.DocsCopied++
	nsStatus.EstimatedDocsCopied++
	mc.status.ProgressMetrics.NumDocsSynced++
	mc.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been completed
func (mc *Connector) taskDoneProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	mc.muProgressMetrics.Lock()
	// update progress counters: num tasks completed
	mc.status.ProgressMetrics.TasksCompleted++
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
		mc.status.ProgressMetrics.NumNamespacesCompleted++
	}
	// decrement the tasks started counter
	mc.status.ProgressMetrics.TasksStarted--
	nsStatus.TasksStarted--
	// remove the task from the active tasks list
	delete(nsStatus.ActiveTasksList, taskId)
	mc.muProgressMetrics.Unlock()
}