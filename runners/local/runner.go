/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package runnerLocal

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	connectorCosmos "github.com/adiom-data/dsync/connectors/cosmos"
	connectorMongo "github.com/adiom-data/dsync/connectors/mongo"
	connectorNull "github.com/adiom-data/dsync/connectors/null"
	connectorRandom "github.com/adiom-data/dsync/connectors/random"
	coordinatorSimple "github.com/adiom-data/dsync/coordinators/simple"
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

	AdvancedProgressRecalcInterval time.Duration //0 means disabled

	LoadLevel string

	MaxNumNamespaces int
}

const (
	sourceName      = "Source"
	destinationName = "Destination"
)

func NewRunnerLocal(settings RunnerLocalSettings) *RunnerLocal {
	r := &RunnerLocal{}
	r.runnerProgress = RunnerSyncProgress{
		StartTime:     time.Now(),
		CurrTime:      time.Now(),
		SyncState:     iface.SetupSyncState,
		NsProgressMap: make(map[iface.Namespace]*iface.NamespaceStatus),
		Namespaces:    make([]iface.Namespace, 0),
	}
	nullRead := settings.SrcConnString == "/dev/random"
	if nullRead {
		r.src = connectorRandom.NewRandomReadConnector(sourceName, connectorRandom.ConnectorSettings{})
	} else if settings.SrcType == "CosmosDB" {
		cosmosSettings := connectorCosmos.ConnectorSettings{ConnectionString: settings.SrcConnString}
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
		if settings.MaxNumNamespaces != 8 {
			cosmosSettings.MaxNumNamespaces = settings.MaxNumNamespaces
		}
		r.src = connectorCosmos.NewCosmosConnector(sourceName, cosmosSettings)
	} else if settings.SrcType == "MongoDB" {
		r.src = connectorMongo.NewMongoConnector(sourceName, connectorMongo.ConnectorSettings{ConnectionString: settings.SrcConnString})
	}
	//null write?
	nullWrite := settings.DstConnString == "/dev/null"
	if nullWrite {
		r.dst = connectorNull.NewNullConnector(destinationName)
	} else {
		connSettings := connectorMongo.ConnectorSettings{ConnectionString: settings.DstConnString}
		if settings.LoadLevel != "" {
			btc := getBaseThreadCount(settings.LoadLevel)
			connSettings.NumParallelWriters = btc / 2
		}
		r.dst = connectorMongo.NewMongoConnector(destinationName, connSettings)
	}

	if settings.StateStoreConnString != "" { //if the statestore is explicitly set, use it
		r.statestore = statestoreMongo.NewMongoStateStore(statestoreMongo.StateStoreSettings{ConnectionString: settings.StateStoreConnString})
	} else if !nullWrite { //if the destination is stateful, we can use it as statestore
		r.statestore = statestoreMongo.NewMongoStateStore(statestoreMongo.StateStoreSettings{ConnectionString: settings.DstConnString})
	} else {
		// otherwise, use a stub statestore because no statestore is needed
		r.statestore = statestoreTemp.NewTempStateStore()
	}

	r.coord = coordinatorSimple.NewSimpleCoordinator()
	r.trans = transportLocal.NewTransportLocal(r.coord)
	r.settings = settings

	return r
}

func (r *RunnerLocal) Setup(ctx context.Context) error {
	slog.Debug("RunnerLocal Setup")

	r.ctx = ctx

	//Initialize in sequence
	err := r.statestore.Setup(r.ctx)
	if err != nil {
		slog.Error("RunnerLocal Setup statestore", err)
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

	//don't start the flow if the verify flag is set
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
	//XXX (AK, 6/2024): not sure if this is the best way to do it
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
