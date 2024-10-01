package cosmos

import (
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/connectors/mongo"
)


// restoreProgressDetails restores the progress metrics from the persisted tasks and progress
func (cc *Connector) restoreProgressDetails(tasks []iface.ReadPlanTask) { 
	mongo.RestoreProgressDetails(tasks)
}

// update estimated namespace doc counts from the actual database
func (cc *Connector) resetNsProgressEstimatedDocCounts() error {
	return mongo.ResetNsProgressEstimatedDocCounts(&cc.BaseMongoConnector)
}

// Updates the progress metrics once a task has been started
func (cc *Connector) taskStartedProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	mongo.TaskStartedProgressUpdate(&cc.BaseMongoConnector, nsStatus, taskId)
}

// Updates the progress metrics once a task has been started
func (cc *Connector) taskInProgressUpdate(nsStatus *iface.NamespaceStatus) {
	mongo.TaskInProgressUpdate(&cc.BaseMongoConnector, nsStatus)
}

// Updates the progress metrics once a task has been completed
func (cc *Connector) taskDoneProgressUpdate(nsStatus *iface.NamespaceStatus, taskId iface.ReadPlanTaskID) {
	mongo.TaskDoneProgressUpdate(&cc.BaseMongoConnector, nsStatus, taskId)
}

// update changeStreamEvents progress counters atomically
func (cc *Connector) updateChangeStreamProgressTracking(reader *ReaderProgress) {
	cc.muProgressMetrics.Lock()
	defer cc.muProgressMetrics.Unlock()
	reader.changeStreamEvents++
	cc.Status.ProgressMetrics.ChangeStreamEvents++
	return
}

