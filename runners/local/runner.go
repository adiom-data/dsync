/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package runnerLocal

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/adiom-data/dsync/connectors/common"
	coordinatorSimple "github.com/adiom-data/dsync/coordinators/simple"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/internal/app/options"
	"github.com/adiom-data/dsync/protocol/iface"
	statestoreMongo "github.com/adiom-data/dsync/statestores/mongo"
	statestoreTemp "github.com/adiom-data/dsync/statestores/temp"
	transportLocal "github.com/adiom-data/dsync/transports/local"
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

	runnerProgress RunnerSyncProgress //internal structure to keep track of the sync progress. Updated ad-hoc on UpdateRunnerProgress()
	rpMutex        sync.Mutex

	ctx context.Context

	// Separate context for integrity as this is for gracefully interrupting it
	integrityCtx       context.Context
	cancelIntegrityCtx context.CancelFunc

	activeFlowID iface.FlowID
}

type RunnerLocalSettings struct {
	SrcDescription       string
	DstDescription       string
	SrcDataType          adiomv1.DataType
	DstDataType          adiomv1.DataType
	TransformClient      adiomv1connect.TransformServiceClient
	Src                  options.ConfiguredConnector
	Dst                  options.ConfiguredConnector
	StateStoreConnString string

	NsFromString []string

	VerifyRequestedFlag  bool
	VerifyQuickCountFlag bool
	VerifyMaxTasks       int
	CleanupRequestedFlag bool
	ReverseRequestedFlag bool

	FlowStatusReportingInterval time.Duration

	AdvancedProgressRecalcInterval time.Duration // 0 means disabled

	LoadLevel                      string
	InitialSyncNumParallelCopiers  int
	NumParallelWriters             int
	NumParallelIntegrityCheckTasks int
	CdcResumeTokenUpdateInterval   time.Duration
	WriterMaxBatchSize             int
	SyncMode                       string
	MultinamespaceBatcher          bool

	NamespaceStreamWriter []string
}

const (
	sourceName      = "Source"
	destinationName = "Destination"
)

func NewRunnerLocal(settings RunnerLocalSettings) *RunnerLocal {
	// apply the reversal to the settings if needed
	applyReverseIfNeeded(&settings)

	r := &RunnerLocal{}
	r.runnerProgress = RunnerSyncProgress{
		SourceDescription:      settings.SrcDescription,
		DestinationDescription: settings.DstDescription,
		StartTime:              time.Now(),
		CurrTime:               time.Now(),
		SyncState:              iface.SetupSyncState,
		NsProgressMap:          make(map[iface.Namespace]*iface.NamespaceStatus),
		Namespaces:             make([]iface.Namespace, 0),
	}
	connectorSettings := common.ConnectorSettings{
		NumParallelCopiers:        settings.InitialSyncNumParallelCopiers,
		NumParallelWriters:        settings.NumParallelWriters,
		MaxWriterBatchSize:        settings.WriterMaxBatchSize,
		MultinamespaceBatcher:     settings.MultinamespaceBatcher,
		ResumeTokenUpdateInterval: settings.CdcResumeTokenUpdateInterval,
		TransformClient:           settings.TransformClient,
		SourceDataType:            settings.SrcDataType,
		DestinationDataType:       settings.DstDataType,
		NamespaceStreamWriter:     settings.NamespaceStreamWriter,
	}
	if settings.LoadLevel != "" {
		btc := GetBaseThreadCount(settings.LoadLevel)
		if connectorSettings.NumParallelCopiers == 0 {
			connectorSettings.NumParallelCopiers = btc
		}
		if connectorSettings.NumParallelWriters == 0 { // default for writer and partition workers are the same
			connectorSettings.NumParallelWriters = btc / 2
		}
	}

	if settings.Src.Local != nil {
		r.src = common.NewLocalConnector(sourceName, settings.Src.Local, connectorSettings)
	} else {
		r.src = common.NewRemoteConnector(sourceName, settings.Src.Remote, connectorSettings)
	}
	if settings.Dst.Local != nil {
		r.dst = common.NewLocalConnector(destinationName, settings.Dst.Local, connectorSettings)
	} else {
		r.dst = common.NewRemoteConnector(destinationName, settings.Dst.Remote, connectorSettings)
	}

	//XXX: implement and use a function isStateful() from connectors to determine if a connector is stateful (although we may not care too much about it for SaaS deployments)
	if settings.StateStoreConnString != "" { //if the statestore is explicitly set, use it
		r.statestore = statestoreMongo.NewMongoStateStore(statestoreMongo.StateStoreSettings{ConnectionString: settings.StateStoreConnString})
	} else {
		// otherwise, use a stub statestore because no statestore is needed
		r.statestore = statestoreTemp.NewTempStateStore()
	}

	numParallelIntegrityCheckTasks := 4 // default
	if settings.NumParallelIntegrityCheckTasks != 0 {
		numParallelIntegrityCheckTasks = settings.NumParallelIntegrityCheckTasks
	} else if settings.LoadLevel != "" {
		btc := GetBaseThreadCount(settings.LoadLevel)
		numParallelIntegrityCheckTasks = btc // default to the base thread count to be consistent with the number of parallel copiers
	}
	r.coord = coordinatorSimple.NewSimpleCoordinator(numParallelIntegrityCheckTasks)
	r.trans = transportLocal.NewTransportLocal(r.coord)
	r.settings = settings

	return r
}

func (r *RunnerLocal) Setup(ctx context.Context) error {
	slog.Debug("RunnerLocal Setup")

	r.ctx = ctx
	r.integrityCtx, r.cancelIntegrityCtx = context.WithCancel(ctx)

	//Initialize in sequence
	err := r.statestore.Setup(r.ctx)
	if err != nil {
		slog.Error("RunnerLocal Setup statestore", "err", err)
		return err
	}

	r.coord.Setup(r.ctx, r.trans, r.statestore)

	err = r.src.Setup(r.ctx, r.trans)
	if err != nil {
		slog.Error("RunnerLocal Setup src", "err", err)
		return err
	}

	err = r.dst.Setup(r.ctx, r.trans)
	if err != nil {
		slog.Error("RunnerLocal Setup dst", "err", err)
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
		SrcConnectorOptions: iface.ConnectorOptions{Namespace: r.settings.NsFromString, Mode: r.settings.SyncMode},
	}
	flowID, err := r.coord.FlowGetOrCreate(flowOptions)
	if err != nil {
		slog.Error("Failed to create flow", "err", err)
		return err
	}
	r.activeFlowID = flowID

	//don't start the flow if the verify flag is set
	if r.settings.VerifyRequestedFlag {
		r.runnerProgress.SyncState = "Verify"
		integrityCheckRes, err := r.coord.PerformFlowIntegrityCheck(r.integrityCtx, flowID, iface.IntegrityCheckOptions{QuickCount: r.settings.VerifyQuickCountFlag, MaxTasks: r.settings.VerifyMaxTasks})
		if err != nil {
			r.runnerProgress.VerificationResult = "ERROR"
			slog.Error("Data integrity check: ERROR")
			slog.Error("Failed to perform flow integrity check", "err", err)
		} else {
			if integrityCheckRes.Passed {
				r.runnerProgress.VerificationResult = "OK"
				if r.integrityCtx.Err() != nil && errors.Is(r.integrityCtx.Err(), context.Canceled) {
					slog.Info("Data integrity check canceled, but so far: OK")
				} else {
					slog.Info("Data integrity check: OK")
				}
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
		slog.Error("Failed to start flow", "err", err)
		return err
	}

	if r.settings.AdvancedProgressRecalcInterval > 0 {
		// periodically update the throughout
		go r.updateRunnerSyncThroughputRoutine(r.settings.AdvancedProgressRecalcInterval)
	}

	// periodically print the flow status for visibility
	//XXX (AK, 6/2024): not sure if this is the best way to do it
	go func() {
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
				flowStatus, err := r.coord.GetFlowStatus(flowID)
				if err != nil {
					slog.Error("Failed to get flow status", "err", err)
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

func (r *RunnerLocal) GracefulShutdown() {
	slog.Debug("RunnerLocal GracefulShutdown")
	_ = r.src.Interrupt(r.activeFlowID)
	if r.cancelIntegrityCtx != nil {
		r.cancelIntegrityCtx()
	}
}

func (r *RunnerLocal) Teardown() {
	slog.Debug("RunnerLocal Teardown")

	r.coord.Teardown()
	r.src.Teardown()
	r.dst.Teardown()
	r.statestore.Teardown()
}

// Determines the base thread count for the connector based on the provided load level
func GetBaseThreadCount(loadLevel string) int {
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

func applyReverseIfNeeded(settings *RunnerLocalSettings) {
	if settings.ReverseRequestedFlag {
		slog.Info("Reversing the flow")
		settings.Src, settings.Dst = settings.Dst, settings.Src
		settings.SyncMode = iface.SyncModeCDC
	}
}
