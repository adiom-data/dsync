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
	"strings"
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
	"go.mongodb.org/mongo-driver/x/mongo/driver/connstring"
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
	DstType 			 string
	StateStoreConnString string

	NsFromString []string

	VerifyRequestedFlag  bool
	CleanupRequestedFlag bool

	FlowStatusReportingInterval time.Duration

	CosmosDeletesEmuRequestedFlag bool
	
	AdvancedProgressRecalcInterval time.Duration // 0 means disabled

	LoadLevel string
	CosmosInitialSyncNumParallelCopiers int
	CosmosNumParallelWriters int
	CosmosNumParallelIntegrityCheckTasks int
	CosmosNumParallelPartitionWorkers int
	CosmosMaxNumNamespaces int
	CosmosServerConnectTimeout time.Duration
	CosmosPingTimeout time.Duration
	CosmosCdcResumeTokenUpdateInterval time.Duration
	CosmosWriterMaxBatchSize int
	CosmosTargetDocCountPerPartition int64
	CosmosDeletesCheckInterval time.Duration
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
		r.runnerProgress.SourceDescription = "/dev/null"
	} else if settings.SrcType == "CosmosDB" {
		cosmosSettings := connectorCosmos.ConnectorSettings{ConnectionString: settings.SrcConnString}
		if settings.CosmosDeletesEmuRequestedFlag {
			cosmosSettings.EmulateDeletes = true
			// the destination is a MongoDB database otherwise the Options check would have failed
			cosmosSettings.WitnessMongoConnString = settings.DstConnString
		}
		// check and adjust for configurated settings
		if settings.LoadLevel != "" {
			btc := getBaseThreadCount(settings.LoadLevel)
			if settings.CosmosInitialSyncNumParallelCopiers != 0 { // override the loadlevel settings if user specifies
				cosmosSettings.InitialSyncNumParallelCopiers = settings.CosmosInitialSyncNumParallelCopiers
			} else {
				cosmosSettings.InitialSyncNumParallelCopiers = btc
			}
			if settings.CosmosNumParallelWriters != 0 {
				cosmosSettings.NumParallelWriters = settings.CosmosNumParallelWriters
			} else { // default for writer, integrity check, and partition workers are the same 
				cosmosSettings.NumParallelWriters = btc / 2
			} 
			if settings.CosmosNumParallelIntegrityCheckTasks != 0 {
				cosmosSettings.NumParallelIntegrityCheckTasks = settings.CosmosNumParallelIntegrityCheckTasks
			} else {
				cosmosSettings.NumParallelIntegrityCheckTasks = btc / 2
			}
			if settings.CosmosNumParallelPartitionWorkers != 0 {
				cosmosSettings.NumParallelPartitionWorkers = settings.CosmosNumParallelPartitionWorkers
			} else {
				cosmosSettings.NumParallelPartitionWorkers = btc / 2
			}
		} else {
			cosmosSettings.InitialSyncNumParallelCopiers = settings.CosmosInitialSyncNumParallelCopiers
			cosmosSettings.NumParallelWriters = settings.CosmosNumParallelWriters
			cosmosSettings.NumParallelIntegrityCheckTasks = settings.CosmosNumParallelIntegrityCheckTasks
			cosmosSettings.NumParallelPartitionWorkers = settings.CosmosNumParallelPartitionWorkers
		}
		cosmosSettings.MaxNumNamespaces = settings.CosmosMaxNumNamespaces
		cosmosSettings.ServerConnectTimeout = settings.CosmosServerConnectTimeout
		cosmosSettings.PingTimeout = settings.CosmosPingTimeout
		cosmosSettings.CdcResumeTokenUpdateInterval = settings.CosmosCdcResumeTokenUpdateInterval
		cosmosSettings.WriterMaxBatchSize = settings.CosmosWriterMaxBatchSize
		cosmosSettings.TargetDocCountPerPartition = settings.CosmosTargetDocCountPerPartition
		cosmosSettings.DeletesCheckInterval = settings.CosmosDeletesCheckInterval
	
		// set all other settings to default
		r.src = connectorCosmos.NewCosmosConnector(sourceName, cosmosSettings)
		r.runnerProgress.SourceDescription = "[Cosmos DB] " + redactMongoConnString(settings.SrcConnString)
	} else if settings.SrcType == "MongoDB" {
		r.src = connectorMongo.NewMongoConnector(sourceName, connectorMongo.ConnectorSettings{ConnectionString: settings.SrcConnString})
		r.runnerProgress.SourceDescription = "[MongoDB] " + redactMongoConnString(settings.SrcConnString)
	}
	//null write?
	nullWrite := settings.DstConnString == "/dev/null"
	if nullWrite {
		r.dst = connectorNull.NewNullConnector(destinationName)
		r.runnerProgress.DestinationDescription = "/dev/null"
	} else if settings.DstType == "CosmosDB" {
		connSettings := connectorCosmos.ConnectorSettings{ConnectionString: settings.DstConnString}
		if settings.LoadLevel != "" {
			btc := getBaseThreadCount(settings.LoadLevel)
			connSettings.NumParallelWriters = btc * 2 // double the base thread count to have more writers than readers (accounting for latency)
		}
		r.dst = connectorCosmos.NewCosmosConnector(destinationName, connSettings)
		// writer max batch size, parallel writers, timeout
	} else {
		connSettings := connectorMongo.ConnectorSettings{ConnectionString: settings.DstConnString}
		if settings.LoadLevel != "" {
			btc := getBaseThreadCount(settings.LoadLevel)
			connSettings.NumParallelWriters = btc * 2 // double the base thread count to have more writers than readers (accounting for latency)
		}
		r.dst = connectorMongo.NewMongoConnector(destinationName, connSettings)
		r.runnerProgress.DestinationDescription = "[MongoDB] " + redactMongoConnString(settings.DstConnString)
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

// redacts mongodb connection string and keeps only hostname:port
// XXX: is this the right place for it or connectors should expose the redacted string themselves?
func redactMongoConnString(url string) string {
	var hosts string

	connString, err := connstring.Parse(url)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to parse connection string: %v", err))
		hosts = url //assume it's a hostname
	} else {
		hosts = strings.Join(connString.Hosts, ",")
	}

	return hosts
}
