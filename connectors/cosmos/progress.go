package cosmos

import (
	"fmt"
	"math"

	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/connectors/mongo"
)


// restoreProgressDetails restores the progress metrics from the persisted tasks and progress
func (cc *Connector) restoreProgressDetails(tasks []iface.ReadPlanTask) { 
	mongo.RestoreProgressDetails(tasks, &cc.status)
}

// Updates the progress metrics once a task has been started
func (cc *Connector) taskStartedProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	cc.muProgressMetrics.Lock()
	nsStatus.ActiveTasksList[taskId] = true
	cc.status.ProgressMetrics.TasksStarted++
	nsStatus.TasksStarted++
	cc.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been completed
func (cc *Connector) taskDoneProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	cc.muProgressMetrics.Lock()
	//update progress counters: num tasks completed
	cc.status.ProgressMetrics.TasksCompleted++
	nsStatus.TasksCompleted++

	//go through all the tasks
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
	//update the estimated docs copied count for the namespace to keep percentage proportional
	nsStatus.EstimatedDocsCopied = int64(math.Max(float64(nsStatus.EstimatedDocsCopied), float64(approxDocsCopied)))
	//check if namespace has been completed
	if nsStatus.TasksCompleted == int64(len(nsStatus.Tasks)) {
		cc.status.ProgressMetrics.NumNamespacesCompleted++
	}
	//decrement the tasks started counter
	cc.status.ProgressMetrics.TasksStarted--
	nsStatus.TasksStarted--
	//remove the task from the active tasks list
	delete(nsStatus.ActiveTasksList, taskId)
	cc.muProgressMetrics.Unlock()
}

// Updates the progress metrics once a task has been started
func (cc *Connector) taskInProgressUpdate(nsStatus *iface.NamespaceStatus) {
	cc.muProgressMetrics.Lock()
	nsStatus.DocsCopied++
	nsStatus.EstimatedDocsCopied++
	cc.status.ProgressMetrics.NumDocsSynced++
	cc.muProgressMetrics.Unlock()
}

// update changeStreamEvents progress counters atomically
func (cc *Connector) updateChangeStreamProgressTracking(reader *ReaderProgress) {
	cc.muProgressMetrics.Lock()
	defer cc.muProgressMetrics.Unlock()
	reader.changeStreamEvents++
	cc.status.ProgressMetrics.ChangeStreamEvents++
	return
}

// update estimated namespace doc counts from the actual database
func (cc *Connector) resetNsProgressEstimatedDocCounts() error {
	for ns, nsStatus := range cc.status.ProgressMetrics.NamespaceProgress {
		collection := cc.client.Database(ns.Db).Collection(ns.Col)
		count, err := collection.EstimatedDocumentCount(cc.ctx)
		if err != nil {
			return fmt.Errorf("failed to count documents: %v", err)
		}
		nsStatus.EstimatedDocCount = int64(count)
	}
	return nil
}