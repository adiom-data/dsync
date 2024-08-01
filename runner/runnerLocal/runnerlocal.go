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
	"math"
	"strings"
	"time"

	"github.com/adiom-data/dsync/connector/connectorCosmos"
	"github.com/adiom-data/dsync/connector/connectorMongo"
	"github.com/adiom-data/dsync/connector/connectorNull"
	"github.com/adiom-data/dsync/connector/connectorRandom"
	"github.com/adiom-data/dsync/coordinator/coordinatorSimple"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/statestore/statestoreMongo"
	"github.com/adiom-data/dsync/transport/transportLocal"
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

	runnerProgress runnerSyncProgress

	ctx context.Context
}

type RunnerLocalSettings struct {
	SrcConnString        string
	DstConnString        string
	SrcType              string
	StateStoreConnString string

	NsFromString []string

	VerifyRequestedFlag  bool
	CleanupRequestedFlag bool

	FlowStatusReportingIntervalSecs time.Duration

	CosmosDeletesEmuRequestedFlag bool
}

const (
	sourceName      = "Source"
	destinationName = "Destination"
)

func NewRunnerLocal(settings RunnerLocalSettings) *RunnerLocal {
	r := &RunnerLocal{}
	r.runnerProgress = runnerSyncProgress{
		startTime:           time.Now(),
		syncState:           "InitialSync",
		totalNamespaces:     5,
		numNamespacesSynced: 0,
		totalDocs:           1000,
		numDocsSynced:       0,
		throughput:          0,
		nsProgressMap: map[iface.Location]namespaceProgress{
			{Database: "db1", Collection: "col1"}: {startTime: time.Now(), totalDocs: 10, numDocsSynced: 5, throughput: 2},
			{Database: "db1", Collection: "col2"}: {startTime: time.Now(), totalDocs: 20, numDocsSynced: 10, throughput: 9},
			{Database: "db2", Collection: "col1"}: {startTime: time.Now(), totalDocs: 30, numDocsSynced: 15, throughput: 10},
			{Database: "db2", Collection: "col2"}: {startTime: time.Now(), totalDocs: 40, numDocsSynced: 25, throughput: 20},
			{Database: "db2", Collection: "col3"}: {startTime: time.Now(), totalDocs: 50, numDocsSynced: 35, throughput: 25},
		},
		namespaces: []iface.Location{{Database: "db1", Collection: "col1"}, {Database: "db1", Collection: "col2"}, {Database: "db2", Collection: "col1"}, {Database: "db2", Collection: "col2"}, {Database: "db2", Collection: "col3"}},
	}
	nullRead := settings.SrcConnString == "/dev/random"
	if nullRead {
		r.src = connectorRandom.NewRandomReadConnector(sourceName, connectorRandom.RandomConnectorSettings{})
	} else if settings.SrcType == "CosmosDB" {
		cosmosSettings := connectorCosmos.CosmosConnectorSettings{ConnectionString: settings.SrcConnString}
		if settings.CosmosDeletesEmuRequestedFlag {
			cosmosSettings.EmulateDeletes = true
			// the destination is a MongoDB database otherwise the Options check would have failed
			cosmosSettings.WitnessMongoConnString = settings.DstConnString
		}
		r.src = connectorCosmos.NewCosmosConnector(sourceName, cosmosSettings)
	} else if settings.SrcType == "MongoDB" {
		r.src = connectorMongo.NewMongoConnector(sourceName, connectorMongo.MongoConnectorSettings{ConnectionString: settings.SrcConnString})
	}
	//null write?
	nullWrite := settings.DstConnString == "/dev/null"
	if nullWrite {
		r.dst = connectorNull.NewNullConnector(destinationName)
	} else {
		r.dst = connectorMongo.NewMongoConnector(destinationName, connectorMongo.MongoConnectorSettings{ConnectionString: settings.DstConnString})
	}
	r.statestore = statestoreMongo.NewMongoStateStore(statestoreMongo.MongoStateStoreSettings{ConnectionString: settings.StateStoreConnString})
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

	//don't start the flow if the verify flag is set
	if r.settings.VerifyRequestedFlag {
		integrityCheckRes, err := r.coord.PerformFlowIntegrityCheck(flowID)
		if err != nil {
			slog.Error("Failed to perform flow integrity check", err)
		} else {
			if integrityCheckRes.Passed {
				slog.Info("Data integrity check: OK")
			} else {
				slog.Error("Data integrity check: FAIL")
			}
		}
		return err
	}

	// destroy the flow if the cleanup flag is set
	if r.settings.CleanupRequestedFlag {
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
	// periodically print the flow status for visibility
	//XXX (AK, 6/2024): not sure if this is the best way to do it
	go func() {
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
					time.Sleep(r.settings.FlowStatusReportingIntervalSecs * time.Second)
				}
			}
		}()
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

type runnerSyncProgress struct {
	startTime           time.Time
	syncState           string                               //get from coordinator
	totalNamespaces     int                                  //get from reader
	numNamespacesSynced int                                  //get from writer
	totalDocs           int                                  //get from reader
	numDocsSynced       int                                  //get from writer
	throughput          int                                  //get from coord
	nsProgressMap       map[iface.Location]namespaceProgress //get from coord? or writer?
	namespaces          []iface.Location                     //use map and get the keys so print order is consistent
}

type namespaceProgress struct {
	startTime     time.Time //get from reader
	totalDocs     int       //get from reader
	numDocsSynced int       //get from writer
	throughput    int       //writer?
}

func (r *RunnerLocal) GetStatusReport() {
	totalTimeElapsed := time.Since(r.runnerProgress.startTime).Seconds()
	fmt.Printf("\n\033[2K\rDsync Progress Report : %v\nTime Elapsed: %.2fs\n\n", r.runnerProgress.syncState, totalTimeElapsed)

	for _, key := range r.runnerProgress.namespaces {
		ns := r.runnerProgress.nsProgressMap[key]
		percentComplete := math.Floor(float64(ns.numDocsSynced) / float64(ns.totalDocs) * 100)
		percentCompleteStr := fmt.Sprintf("%.0f%% complete", percentComplete)
		timeElapsed := time.Since(ns.startTime).Seconds()
		timeElapsedStr := fmt.Sprintf("Time Elapsed: %.2fs", timeElapsed)
		throughputStr := fmt.Sprintf("Throughput: %v docs/s", ns.throughput)
		namespace := "Namespace: " + key.Database + "." + key.Collection
		fmt.Printf("\033[2K\r%-30s %-30s %-25s %-25s\n", namespace, timeElapsedStr, percentCompleteStr, throughputStr)
	}
	totalPercentComplete := float64(r.runnerProgress.numDocsSynced) / float64(r.runnerProgress.totalDocs) * 100
	progressBarWidth := 50
	progress := int(totalPercentComplete / 100 * float64(progressBarWidth))
	progressBar := fmt.Sprintf("[%s%s] %.2f%%", strings.Repeat(string('#'), progress), strings.Repeat(" ", progressBarWidth-progress), totalPercentComplete)
	fmt.Printf("\n\033[2K\r%s\n", progressBar)

	for i := 0; i < r.runnerProgress.totalNamespaces+6; i++ {
		fmt.Print("\033[F")
	}
	if totalPercentComplete != 100 {
		r.runnerProgress.numDocsSynced += 50
	}
}
