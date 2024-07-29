/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorCosmos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

const (
	connectorDBType string = "CosmosDB"               // We're a CosmosDB-compatible connector
	connectorSpec   string = "MongoDB Provisioned RU" // Only compatible with MongoDB API and provisioned deployments
)

type CosmosConnector struct {
	desc string

	settings CosmosConnectorSettings
	client   *mongo.Client
	ctx      context.Context

	t  iface.Transport
	id iface.ConnectorID

	connectorType         iface.ConnectorType
	connectorCapabilities iface.ConnectorCapabilities

	coord iface.CoordinatorIConnectorSignal

	mutex sync.Mutex

	//TODO (AK, 6/2024): these should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	status                iface.ConnectorStatus
	flowCtx               context.Context
	flowCancelFunc        context.CancelFunc
	flowId                iface.FlowID
	flowConnCapabilities  iface.ConnectorCapabilities
	flowCDCResumeTokenMap *TokenMap //stores the resume token for each namespace
}

type CosmosConnectorSettings struct {
	ConnectionString string

	serverConnectTimeout           time.Duration
	pingTimeout                    time.Duration
	initialSyncNumParallelCopiers  int
	writerMaxBatchSize             int //0 means no limit (in # of documents)
	numParallelWriters             int
	CdcResumeTokenUpdateInterval   time.Duration
	numParallelIntegrityCheckTasks int
}

func NewCosmosConnector(desc string, settings CosmosConnectorSettings) *CosmosConnector {
	// Set default values
	settings.serverConnectTimeout = 10 * time.Second
	settings.pingTimeout = 2 * time.Second
	settings.initialSyncNumParallelCopiers = 4
	settings.writerMaxBatchSize = 0
	settings.numParallelWriters = 4
	settings.numParallelIntegrityCheckTasks = 4
	if settings.CdcResumeTokenUpdateInterval == 0 { //if not set, default to 60 seconds
		settings.CdcResumeTokenUpdateInterval = 60 * time.Second
	}

	return &CosmosConnector{desc: desc, settings: settings}
}

func (cc *CosmosConnector) Setup(ctx context.Context, t iface.Transport) error {
	cc.ctx = ctx
	cc.t = t

	// Connect to the MongoDB instance
	ctxConnect, cancel := context.WithTimeout(cc.ctx, cc.settings.serverConnectTimeout)
	defer cancel()
	clientOptions := moptions.Client().ApplyURI(cc.settings.ConnectionString)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	cc.client = client

	// Check the connection
	ctxPing, cancel := context.WithTimeout(cc.ctx, cc.settings.pingTimeout)
	defer cancel()
	err = cc.client.Ping(ctxPing, nil)
	if err != nil {
		return err
	}

	// Get version of the MongoDB server
	var commandResult bson.M
	err = cc.client.Database("admin").RunCommand(cc.ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&commandResult)
	if err != nil {
		return err
	}
	version := commandResult["version"]

	// Instantiate ConnectorType
	cc.connectorType = iface.ConnectorType{DbType: connectorDBType, Version: version.(string), Spec: connectorSpec}
	// Instantiate ConnectorCapabilities, current capabilities are source only
	cc.connectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: false, IntegrityCheck: true, Resumability: true}
	// Instantiate ConnectorStatus
	cc.status = iface.ConnectorStatus{WriteLSN: 0}

	// Get the coordinator endpoint
	coord, err := cc.t.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	cc.coord = coord

	// Generate connector ID for resumability purposes
	id := generateConnectorID(cc.settings.ConnectionString)

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: cc.desc, Type: cc.connectorType, Cap: cc.connectorCapabilities, Id: id}
	// Register the connector
	cc.id, err = coord.RegisterConnector(connectorDetails, cc)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	slog.Info("Cosmos Connector has been configured with ID " + (string)(cc.id))

	return nil
}

func (cc *CosmosConnector) Teardown() {
	if cc.client != nil {
		cc.client.Disconnect(cc.ctx)
	}
}

func (cc *CosmosConnector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	// this is what came for the flow
	cc.flowConnCapabilities = reqCap
	slog.Debug(fmt.Sprintf("Connector %s set capabilities for flow %s: %+v", cc.id, flowId, reqCap))
}

// TODO (AK, 6/2024): this should be split to a separate class and/or functions
func (cc *CosmosConnector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	cc.flowCtx, cc.flowCancelFunc = context.WithCancel(cc.ctx)
	cc.flowId = flowId

	tasks := readPlan.Tasks

	if len(tasks) == 0 {
		return errors.New("no tasks to copy")
	}

	slog.Debug(fmt.Sprintf("StartReadToChannel Tasks: %+v", tasks))

	cc.flowCDCResumeTokenMap = NewTokenMap()

	flowCDCResumeToken := readPlan.CdcResumeToken //Get the endoded token

	err := cc.flowCDCResumeTokenMap.decodeMap(flowCDCResumeToken) //Decode the token to get the map
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to deserialize the resume token map: %v", err))
	}

	slog.Debug(fmt.Sprintf("Initial Deserialized resume token map: %v", cc.flowCDCResumeTokenMap.Map))

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := cc.t.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	// Declare two channels to wait for the change stream reader and the initial sync to finish
	changeStreamDone := make(chan struct{})
	initialSyncDone := make(chan struct{})

	readerProgress := ReaderProgress{ //initialSyncDocs is atomic counters
		tasksTotal:         uint64(len(tasks)),
		tasksCompleted:     0,
		changeStreamEvents: 0,
	}

	readerProgress.initialSyncDocs.Store(0)

	// start printing progress
	go cc.printProgress(&readerProgress)

	// kick off the change stream reader
	go func() {
		//wait for the initial sync to finish
		<-initialSyncDone
		defer close(changeStreamDone)

		// start sending periodic barrier messages with cdc resume token updates
		go func() {
			ticker := time.NewTicker(cc.settings.CdcResumeTokenUpdateInterval)
			defer ticker.Stop()
			for {
				select {
				case <-cc.flowCtx.Done():
					return
				case <-changeStreamDone:
					return
				case <-ticker.C:
					// send a barrier message with the updated resume token
					flowCDCResumeToken, err := cc.flowCDCResumeTokenMap.encodeMap()
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to serialize the resume token map: %v", err))
					}
					dataChannel <- iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_CdcResumeTokenUpdate, BarrierCdcResumeToken: flowCDCResumeToken}
				}
			}
		}()
		// start the concurrent change streams

		cc.StartConcurrentChangeStreams(tasks, &readerProgress, dataChannel)

	}()

	// kick off the initial sync
	go func() {
		defer close(initialSyncDone)

		slog.Info(fmt.Sprintf("Connector %s is starting initial sync for flow %s", cc.id, flowId))

		//create a channel to distribute tasks to copiers
		taskChannel := make(chan iface.ReadPlanTask)
		//create a wait group to wait for all copiers to finish
		var wg sync.WaitGroup
		wg.Add(cc.settings.initialSyncNumParallelCopiers)

		//start 4 copiers
		for i := 0; i < cc.settings.initialSyncNumParallelCopiers; i++ {
			go func() {
				defer wg.Done()
				for task := range taskChannel {
					slog.Debug(fmt.Sprintf("Processing task: %v", task))
					db := task.Def.Db
					col := task.Def.Col
					collection := cc.client.Database(db).Collection(col)
					cursor, err := collection.Find(cc.flowCtx, bson.D{})
					if err != nil {
						if cc.flowCtx.Err() == context.Canceled {
							slog.Debug(fmt.Sprintf("Find error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Failed to find documents in collection: %v", err))
						}
						continue
					}
					loc := iface.Location{Database: db, Collection: col}
					var dataBatch [][]byte
					var batch_idx int
					for cursor.Next(cc.flowCtx) {
						if dataBatch == nil {
							dataBatch = make([][]byte, cursor.RemainingBatchLength()+1) //preallocate the batch
							batch_idx = 0
						}
						rawData := cursor.Current
						data := []byte(rawData)
						readerProgress.initialSyncDocs.Add(1)

						dataBatch[batch_idx] = data
						batch_idx++

						if cursor.RemainingBatchLength() == 0 { //no more left in the batch
							dataChannel <- iface.DataMessage{DataBatch: dataBatch, MutationType: iface.MutationType_InsertBatch, Loc: loc}
							//TODO (AK, 6/2024): is it ok that this blocks until the app is terminated if no one reads? (e.g. reader crashes)
							dataBatch = nil
						}
					}
					if err := cursor.Err(); err != nil {
						if cc.flowCtx.Err() == context.Canceled {
							slog.Debug(fmt.Sprintf("Cursor error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Cursor error: %v", err))
						}
					} else {
						cursor.Close(cc.flowCtx)
						readerProgress.tasksCompleted++ //XXX Should we do atomic add here as well, shared variable multiple threads
						slog.Debug(fmt.Sprintf("Done processing task: %v", task))
						//notify the coordinator that the task is done from our side
						cc.coord.NotifyTaskDone(cc.flowId, cc.id, task.Id)
						//send a barrier message to signal the end of the task
						if cc.flowConnCapabilities.Resumability { //send only if the flow supports resumability otherwise who knows what will happen on the recieving side
							dataChannel <- iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_TaskComplete, BarrierTaskId: (uint)(task.Id)}
						}
					}
				}
			}()
		}

		//iterate over all the tasks and distribute them to copiers
		for _, task := range tasks {
			if task.Status == iface.ReadPlanTaskStatus_Completed {
				// the task is already completed, so we can just skip it
				readerProgress.tasksCompleted++ //XXX Should we do atomic add here as well, shared variable multiple threads
			} else {
				taskChannel <- task
			}
		}
		//close the task channel to signal copiers that there are no more tasks
		close(taskChannel)

		//wait for all copiers to finish
		wg.Wait()
	}()

	// wait for both the change stream reader and the initial sync to finish
	go func() {
		<-initialSyncDone
		<-changeStreamDone

		close(dataChannel) //send a signal downstream that we are done sending data //TODO (AK, 6/2024): is this the right way to do it?

		slog.Info(fmt.Sprintf("Connector %s is done reading for flow %s", cc.id, flowId))
		err := cc.coord.NotifyDone(flowId, cc.id) //TODO (AK, 6/2024): Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", cc.id, flowId, err))
		}
	}()

	return nil
}

func (cc *CosmosConnector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	return errors.New("CosmosConnector does not write to destination yet")
}

func (cc *CosmosConnector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go cc.doIntegrityCheck_sync(flowId, options)
	return nil
}

func (cc *CosmosConnector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return cc.status
}

func (cc *CosmosConnector) Interrupt(flowId iface.FlowID) error {
	cc.flowCancelFunc()
	return nil
}

func (cc *CosmosConnector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go func() {
		// Retrieve the latest resume token before we start reading anything
		// We will use the resume token to start the change stream

		tasks, err := cc.createInitialCopyTasks(options.Namespace)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to create initial copy tasks: %v", err))
			return
		}
		slog.Debug(fmt.Sprintf("created tasks: %v", tasks))

		tokenMap := NewTokenMap()

		//create resume token for each task
		wg := sync.WaitGroup{}
		for _, task := range tasks {
			wg.Add(1)
			go func(task iface.ReadPlanTask) {
				defer wg.Done()
				loc := iface.Location{Database: task.Def.Db, Collection: task.Def.Col}
				resumeToken, err := cc.getLatestResumeToken(loc)
				if err != nil {
					slog.Error(fmt.Sprintf("Failed to get latest resume token for task %v: %v", task.Id, err))
					return
				}
				tokenMap.AddToken(loc, resumeToken)
			}(task)
		}
		wg.Wait()

		slog.Debug(fmt.Sprintf("Read Plan Resume token map: %v", tokenMap.Map))
		//serialize the resume token map
		flowCDCResumeToken, err := tokenMap.encodeMap()
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to serialize the resume token map: %v", err))
		}

		plan := iface.ConnectorReadPlan{Tasks: tasks, CdcResumeToken: flowCDCResumeToken}

		err = cc.coord.PostReadPlanningResult(flowId, cc.id, iface.ConnectorReadPlanResult{ReadPlan: plan, Success: true})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed notifying coordinator about read planning done: %v", err))
		}
	}()
	return nil
}
