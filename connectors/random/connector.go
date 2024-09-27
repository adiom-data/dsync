/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package random

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
)

type Connector struct {
	desc string

	settings ConnectorSettings
	ctx      context.Context

	t           iface.Transport
	id          iface.ConnectorID
	docMap      map[iface.Location]*IndexMap //map of locations to map of document IDs
	docMapMutex sync.RWMutex

	connectorType         iface.ConnectorType
	connectorCapabilities iface.ConnectorCapabilities

	coord iface.CoordinatorIConnectorSignal

	//TODO (AK, 6/2024): this should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	status         iface.ConnectorStatus
	flowctx        context.Context
	flowCancelFunc context.CancelFunc
}

type ConnectorSettings struct {
	numParallelGenerators int //number of parallel data generators

	numDatabases                     int  //must be at least 1
	numCollectionsPerDatabase        int  //must be at least 1
	numInitialDocumentsPerCollection int  //must be at least 1
	numFields                        int  //number of fields in each document, must be at least 1
	docSize                          uint //size of field values in number of chars/bytes, must be at least 1
	maxDocsPerCollection             int  //maximum number of documents per collection, cap the change stream
	changeStreamDuration             int  //duration of change stream in seconds
	//list of size 4, representing probabilities of change stream operations in order: insert, insertBatch, update, delete
	//sum of probabilities must add to 1.0
	probabilities []float64
}

func NewRandomReadConnector(desc string, settings ConnectorSettings) *Connector {
	// Set default values

	settings.numParallelGenerators = 4

	settings.numDatabases = 10
	settings.numCollectionsPerDatabase = 2
	settings.numInitialDocumentsPerCollection = 500
	settings.numFields = 10
	settings.docSize = 15 //
	settings.maxDocsPerCollection = 1000
	settings.changeStreamDuration = 60

	settings.probabilities = []float64{0.25, 0.25, 0.25, 0.25}

	return &Connector{desc: desc, settings: settings}
}

func (rc *Connector) Setup(ctx context.Context, t iface.Transport) error {
	//setup the connector
	rc.ctx = ctx
	rc.t = t

	// Instantiate ConnectorType
	rc.connectorType = iface.ConnectorType{DbType: "/dev/random"}
	// Instantiate ConnectorCapabilities
	rc.connectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: false}
	// Instantiate ConnectorStatus
	rc.status = iface.ConnectorStatus{WriteLSN: 0}
	// Instantiate docMap
	rc.docMap = make(map[iface.Location]*IndexMap)

	// Get the coordinator endpoint
	coord, err := rc.t.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	rc.coord = coord

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: rc.desc, Type: rc.connectorType, Cap: rc.connectorCapabilities}
	// Register the connector
	rc.id, err = coord.RegisterConnector(connectorDetails, rc)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	slog.Info("RandomReadConnector has been configured with ID " + (string)(rc.id))

	return nil
}

func (rc *Connector) Teardown() {
	//does nothing, no client to disconnect
}

func (rc *Connector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	//not necessary always source
}

func (rc *Connector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelId iface.DataChannelID) error {
	rc.flowctx, rc.flowCancelFunc = context.WithCancel(rc.ctx)

	tasks := readPlan.Tasks
	if len(tasks) == 0 && options.Mode != iface.SyncModeCDC {
		return errors.New("no tasks to copy")
	}
	slog.Debug(fmt.Sprintf("StartReadToChannel Tasks: %v", tasks))

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := rc.t.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	// Declare two channels to wait for the change stream reader and the initial sync to finish
	//changeStreamGenerationDone := make(chan struct{}) , change stream not implemented yet
	initialGenerationDone := make(chan struct{})
	changeStreamGenerationDone := make(chan struct{})

	readerProgress := ReaderProgress{ //XXX (AK, 6/2024): should we handle overflow? Also, should we use atomic types?
		changeStreamEvents: 0,
		tasksTotal:         uint64(len(tasks)),
	}

	readerProgress.initialSyncDocs.Store(0)
	readerProgress.tasksCompleted.Store(0)

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		startTime := time.Now()
		operations := uint64(0)
		for {
			select {
			case <-rc.flowctx.Done():
				return
			case <-ticker.C:
				elapsedTime := time.Since(startTime).Seconds()
				operations_delta := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents - operations
				opsPerSec := math.Floor(float64(operations_delta) / elapsedTime)
				// Print reader progress
				slog.Info(fmt.Sprintf("RandomReaderConnector Progress: Initial Docs Generation - %d (%d/%d tasks completed), Change Stream Events - %d, Operations per Second - %.2f",
					readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted.Load(), readerProgress.tasksTotal, readerProgress.changeStreamEvents, opsPerSec))

				startTime = time.Now()
				operations = readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
			}
		}
	}()

	// Start the initial generation
	go func() {
		defer close(initialGenerationDone)

		// if we have no tasks (e.g. we are in CDC mode), we skip the initial sync
		if len(tasks) == 0 {
			slog.Info(fmt.Sprintf("Null Read Connector %s is skipping initial sync for flow %s", rc.id, flowId))
			return
		}

		slog.Info(fmt.Sprintf("Null Read Connector %s is starting initial data generation for flow %s", rc.id, flowId))
		//create a channel to distribute tasks to copiers
		taskChannel := make(chan iface.ReadPlanTask)
		//create a wait group to wait for all copiers to finish
		var wg sync.WaitGroup
		wg.Add(rc.settings.numParallelGenerators)

		for i := 0; i < rc.settings.numParallelGenerators; i++ {
			go func() {
				defer wg.Done()
				for task := range taskChannel {
					slog.Debug(fmt.Sprintf("Generating task: %v", task))
					rc.ProcessDataGenerationTaskBatch(task, dataChannel, &readerProgress)
				}
			}()
		}
		//iterate over all the tasks and distribute them to copiers
		for _, task := range tasks {
			taskChannel <- task
		}
		//close the task channel to signal copiers that there are no more tasks
		close(taskChannel)

		//wait for all copiers to finish
		wg.Wait()
	}()

	// Start the change stream generation
	go func() {
		<-initialGenerationDone
		//timeout for changestream generation
		ctx, cancel := context.WithTimeout(rc.flowctx, time.Duration(rc.settings.changeStreamDuration)*time.Second)
		defer cancel()
		defer close(changeStreamGenerationDone)

		var lsn int64 = 0
		slog.Info(fmt.Sprintf("Null Read Connector %s is starting change stream generation for flow %s", rc.id, flowId))
		//SK: MongoConnector uses namespace filtering when copying over data during the initial sync and change stream, there is no data to filter, namespace filtering is not neccessary in the RandomReadConnecto
		rc.status.CDCActive = true
		//continuos change stream generator simulates a random change operation every second,
		//either a single insert, batch insert, single update, or single delete.
		//XXX: change stream currently uses one thread, should we parallelize it with multiple threads to generate more changes?
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				//generate random operation
				operation, err := rc.generateOperation()
				if err != nil {
					slog.Error(fmt.Sprintf("Failed to generate operation: %v", err))
					continue
				}
				dataMsg, err := rc.generateChangeStreamEvent(operation, &readerProgress, &lsn) //XXX: ID logic will not work with insertBatch, will need to change
				if err != nil {
					slog.Error(fmt.Sprintf("Failed to generate change stream event: %v", err))
					continue
				}
				//XXX Should we do atomic add here as well, shared variable multiple threads
				dataMsg.SeqNum = lsn
				dataChannel <- dataMsg
			}
		}

	}()

	// Wait for initial generation and change stream generation to finish
	go func() {
		<-initialGenerationDone
		<-changeStreamGenerationDone

		close(dataChannel) //send a signal downstream that we are done sending data //TODO (AK, 6/2024): is this the right way to do it?

		slog.Info(fmt.Sprintf("RandomReadConnector %s is done generating data for flow %s", rc.id, flowId))
		err := rc.coord.NotifyDone(flowId, rc.id) //TODO (AK, 6/2024): Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", rc.id, flowId, err))
		}
	}()
	return nil
}

func (rc *Connector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	//never writes to destination, errors
	return errors.New("RandomReadConnector does not write to destination")
}

func (rc *Connector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions, full bool) error {
	//no client, errors
	return errors.New("RandomReadConnector does not have a client to request data integrity check")
}

func (rc *Connector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	//get connector status
	return rc.status
}

func (rc *Connector) Interrupt(flowId iface.FlowID) error {
	//TODO: implement for testing
	rc.flowCancelFunc()
	return nil
}

func (rc *Connector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go func() {
		var tasks []iface.ReadPlanTask
		if options.Mode != iface.SyncModeCDC {
			tasks = rc.CreateInitialGenerationTasks()
		} else {
			tasks = nil
		}
		plan := iface.ConnectorReadPlan{Tasks: tasks, CdcResumeToken: []byte{1}} //we supply a faux resume token to follow the spec
		err := rc.coord.PostReadPlanningResult(flowId, rc.id, iface.ConnectorReadPlanResult{ReadPlan: plan, Success: true})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed notifying coordinator about read planning done: %v", err))
		}
	}()
	return nil
}
