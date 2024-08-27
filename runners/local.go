// Copyright (c) 2024. Adiom, Inc.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runners

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/statestores/statestoreTemp"

	"github.com/adiom-data/dsync/connectors/connectorNull"
	"github.com/adiom-data/dsync/connectors/cosmos"
	"github.com/adiom-data/dsync/connectors/random"
	"github.com/adiom-data/dsync/coordinator/coordinatorSimple"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/runners/runnerLocal"
	"github.com/adiom-data/dsync/transport"
)

// Implements the protocol.iface.Runner interface
// Supports two connectors (source and destination)
// Sets up all the components to run locally in a single binary

type RunnerLocal struct {
	settings RunnerLocalSettings

	trans      iface.Transport
	statestore iface.Statestore
	coord      iface.Coordinator
	src, dst   iface.Connector

	runnerProgress runnerLocal.RunnerSyncProgress // internal structure to keep track of the sync progress. Updated ad-hoc on UpdateRunnerProgress()

	ctx context.Context

	activeFlowID iface.FlowID
}

type RunnerLocalSettings struct {
	SrcConnString        string
	DstConnString        string
	SrcType              string
	StateStoreConnString string

	NsFromString []string

	VerifyRequestedFlag  bool
	CleanupRequestedFlag bool

	FlowStatusReportingInterval time.Duration

	CosmosDeletesEmuRequestedFlag bool

	AdvancedProgressRecalcInterval time.Duration // 0 means disabled

	LoadLevel string
}

const (
	sourceName      = "Source"
	destinationName = "Destination"
)

func NewRunnerLocal(settings RunnerLocalSettings) *RunnerLocal {
	r := &RunnerLocal{}
	r.runnerProgress = runnerLocal.RunnerSyncProgress{
		StartTime:     time.Now(),
		CurrTime:      time.Now(),
		SyncState:     iface.SetupSyncState,
		NsProgressMap: make(map[iface.Namespace]*iface.NamespaceStatus),
		Namespaces:    make([]iface.Namespace, 0),
	}
	nullRead := settings.SrcConnString == "/dev/random"
	if nullRead {
		r.src = random.NewRandomReadConnector(sourceName, random.RandomConnectorSettings{})
	} else if settings.SrcType == "CosmosDB" {
		cosmosSettings := cosmos.CosmosConnectorSettings{ConnectionString: settings.SrcConnString}
		if settings.CosmosDeletesEmuRequestedFlag {
			cosmosSettings.EmulateDeletes = true
			// the destination is a MongoDB database otherwise the Options check would have failed
			cosmosSettings.WitnessMongoConnString = settings.DstConnString
		}
		if settings.LoadLevel != "" {
			btc := getBaseThreadCount(settings.LoadLevel)
			cosmosSettings.InitialSyncNumParallelCopiers = btc
			cosmosSettings.NumParallelWriters = btc / 2
		}
		r.src = cosmos.NewCosmosConnector(sourceName, cosmosSettings)
	} else if settings.SrcType == "MongoDB" {
		r.src = connectorMongo.NewMongoConnector(sourceName, connectorMongo.MongoConnectorSettings{ConnectionString: settings.SrcConnString})
	}
	// null write?
	nullWrite := settings.DstConnString == "/dev/null"
	if nullWrite {
		r.dst = connectorNull.NewNullConnector(destinationName)
	} else {
		connSettings := connectorMongo.MongoConnectorSettings{ConnectionString: settings.DstConnString}
		if settings.LoadLevel != "" {
			btc := getBaseThreadCount(settings.LoadLevel)
			connSettings.NumParallelWriters = btc / 2
		}
		r.dst = connectorMongo.NewMongoConnector(destinationName, connSettings)
	}

	if settings.StateStoreConnString != "" { // if the statestores is explicitly set, use it
		r.statestore = statestoreMongo.NewMongoStateStore(statestoreMongo.MongoStateStoreSettings{ConnectionString: settings.StateStoreConnString})
	} else if !nullWrite { // if the destination is stateful, we can use it as statestores
		r.statestore = statestoreMongo.NewMongoStateStore(statestoreMongo.MongoStateStoreSettings{ConnectionString: settings.DstConnString})
	} else {
		// otherwise, use a stub statestores because no statestores is needed
		r.statestore = statestoreTemp.NewMongoStateStore()
	}

	r.coord = coordinatorSimple.NewSimpleCoordinator()
	r.trans = transport.NewLocalTransport(r.coord)
	r.settings = settings

	return r
}

func (r *RunnerLocal) Setup(ctx context.Context) error {
	slog.Debug("RunnerLocal Setup")

	r.ctx = ctx

	// Initialize in sequence
	err := r.statestore.Setup(r.ctx)
	if err != nil {
		slog.Error("RunnerLocal Setup statestores", err)
		return err
	}

	r.coord.Setup(r.ctx, r.trans, r.statestore)

	err = r.src.Setup(r.ctx, r.trans)
	if err != nil {
		slog.Error("RunnerLocal Setup src", err)
		return err
	}

	err = r.dst.Setup(r.ctx, r.trans)
	if err != nil {
		slog.Error("RunnerLocal Setup dst", err)
		return err
	}

	return nil
}

func (r *RunnerLocal) Run() error {
	slog.Debug("RunnerLocal Run")

	// get available connectors from the coordinator
	connectors := r.coord.GetConnectors()
	// print the available connectors
	slog.Debug(fmt.Sprintf("Connectors: %v", connectors))

	// find connector ids based on their descriptions
	var srcId, dstId iface.ConnectorID
	for _, connectorDetails := range connectors {
		if connectorDetails.Desc == sourceName {
			srcId = connectorDetails.Id
		} else if connectorDetails.Desc == destinationName {
			dstId = connectorDetails.Id
		}
	}

	if srcId == iface.ConnectorID("") {
		slog.Error("Source connector not found")
		return fmt.Errorf("source connector not found")
	}
	if dstId == iface.ConnectorID("") {
		slog.Error("Destination connector not found")
		return fmt.Errorf("destination connector not found")
	}

	// create a flow
	flowOptions := iface.FlowOptions{
		SrcId:               srcId,
		DstId:               dstId,
		Type:                iface.UnidirectionalFlowType,
		SrcConnectorOptions: iface.ConnectorOptions{Namespace: r.settings.NsFromString},
	}
	flowID, err := r.coord.FlowGetOrCreate(flowOptions)
	if err != nil {
		slog.Error("Failed to create flow", err)
		return err
	}
	r.activeFlowID = flowID

	// don't start the flow if the verify flag is set
	if r.settings.VerifyRequestedFlag {
		r.runnerProgress.SyncState = "Verify"
		integrityCheckRes, err := r.coord.PerformFlowIntegrityCheck(flowID)
		if err != nil {
			slog.Error("Failed to perform flow integrity check", err)
		} else {
			if integrityCheckRes.Passed {
				r.runnerProgress.VerificationResult = "OK"
				slog.Info("Data integrity check: OK")
			} else {
				r.runnerProgress.VerificationResult = "FAIL"
				slog.Error("Data integrity check: FAIL")
			}
		}
		return err
	}

	// destroy the flow if the cleanup flag is set
	if r.settings.CleanupRequestedFlag {
		r.runnerProgress.SyncState = "Cleanup"
		slog.Info("Cleaning up metadata for the flow")
		r.coord.FlowDestroy(flowID)
		return nil
	}

	// start the flow
	err = r.coord.FlowStart(flowID)
	if err != nil {
		slog.Error("Failed to start flow", err)
		return err
	}

	if r.settings.AdvancedProgressRecalcInterval > 0 {
		// periodically update the throughout
		go r.updateRunnerSyncThroughputRoutine(r.settings.AdvancedProgressRecalcInterval)
	}

	// periodically print the flow status for visibility
	// XXX (AK, 6/2024): not sure if this is the best way to do it
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
				flowStatus, err := r.coord.GetFlowStatus(flowID)
				if err != nil {
					slog.Error("Failed to get flow status", err)
					break
				}
				slog.Debug(fmt.Sprintf("Flow status: %v", flowStatus))
				if flowStatus.SrcStatus.CDCActive {
					eventsDiff := flowStatus.SrcStatus.WriteLSN - flowStatus.DstStatus.WriteLSN
					if eventsDiff < 0 {
						eventsDiff = 0
					}
					slog.Info(fmt.Sprintf("Number of events to fully catch up: %d", eventsDiff))
				}
				time.Sleep(r.settings.FlowStatusReportingInterval * time.Second)
			}
		}
	}()

	// wait for the flow to finish
	if err := r.coord.WaitForFlowDone(flowID); err != nil {
		return err
	}
	return nil
}

func (r *RunnerLocal) Teardown() {
	slog.Debug("RunnerLocal Teardown")

	r.coord.Teardown()
	r.src.Teardown()
	r.dst.Teardown()
	r.statestore.Teardown()
}

// Determines the base thread count for the connector based on the provided load level
func getBaseThreadCount(loadLevel string) int {
	switch loadLevel {
	case "Low":
		return 4
	case "Medium":
		return 8
	case "High":
		return 16
	case "Beast":
		return 32
	}

	return 0
}

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
	Namespaces    []iface.Namespace // use map and get the keys so print order is consistent

	TasksTotal     int64
	TasksStarted   int64
	TasksCompleted int64

	Lag int64

	VerificationResult string

	SrcAdditionalStateInfo string
}

// Update the runners progress struct with the latest progress metrics from the flow status
func (r *runner.RunnerLocal) UpdateRunnerProgress() {
	if r.activeFlowID == iface.FlowID("") { // no active flow - probably not active yet
		return
	}

	flowStatus, err := r.coord.GetFlowStatus(r.activeFlowID)
	if err != nil {
		slog.Error("Failed to get flow status", err)
		return
	}
	srcStatus := flowStatus.SrcStatus

	if r.runnerProgress.SyncState != iface.VerifySyncState && r.runnerProgress.SyncState != iface.CleanupSyncState { // XXX: if we're cleaning up or verifying, we don't want to overwrite the state
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

// Get the latest runners progress struct
func (r *runner.RunnerLocal) GetRunnerProgress() RunnerSyncProgress {
	return r.runnerProgress
}

// Loops to update the status and throughput metrics for the runners progress struct
func (r *runner.RunnerLocal) updateRunnerSyncThroughputRoutine(throughputUpdateInterval time.Duration) {
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
