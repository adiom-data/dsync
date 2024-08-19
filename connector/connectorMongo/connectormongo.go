/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package connectorMongo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

type MongoConnector struct {
	desc string

	settings MongoConnectorSettings
	client   *mongo.Client
	ctx      context.Context

	t  iface.Transport
	id iface.ConnectorID

	connectorType         iface.ConnectorType
	connectorCapabilities iface.ConnectorCapabilities

	coord iface.CoordinatorIConnectorSignal

	//TODO (AK, 6/2024): these should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	status               iface.ConnectorStatus
	flowCtx              context.Context
	flowCancelFunc       context.CancelFunc
	flowId               iface.FlowID
	flowConnCapabilities iface.ConnectorCapabilities
	flowCDCResumeToken   bson.Raw
}

type MongoConnectorSettings struct {
	ConnectionString string

	serverConnectTimeout           time.Duration
	pingTimeout                    time.Duration
	initialSyncNumParallelCopiers  int
	writerMaxBatchSize             int // applies to batch inserts only; 0 means no limit
	numParallelWriters             int
	CdcResumeTokenUpdateInterval   time.Duration
	numParallelIntegrityCheckTasks int
}

func NewMongoConnector(desc string, settings MongoConnectorSettings) *MongoConnector {
	// Set default values
	settings.serverConnectTimeout = 10 * time.Second
	settings.pingTimeout = 2 * time.Second
	settings.initialSyncNumParallelCopiers = 4
	settings.writerMaxBatchSize = 0
	settings.numParallelWriters = 4
	if settings.CdcResumeTokenUpdateInterval == 0 { //if not set, default to 60 seconds
		settings.CdcResumeTokenUpdateInterval = 60 * time.Second
	}
	settings.numParallelIntegrityCheckTasks = 4

	return &MongoConnector{desc: desc, settings: settings}
}

func (mc *MongoConnector) Setup(ctx context.Context, t iface.Transport) error {
	mc.ctx = ctx
	mc.t = t

	// Connect to the MongoDB instance
	ctxConnect, cancel := context.WithTimeout(mc.ctx, mc.settings.serverConnectTimeout)
	defer cancel()
	clientOptions := options.Client().ApplyURI(mc.settings.ConnectionString).SetConnectTimeout(mc.settings.serverConnectTimeout)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	mc.client = client

	// Check the connection
	ctxPing, cancel := context.WithTimeout(mc.ctx, mc.settings.pingTimeout)
	defer cancel()
	err = mc.client.Ping(ctxPing, nil)
	if err != nil {
		return err
	}

	// Get version of the MongoDB server
	var commandResult bson.M
	err = mc.client.Database("admin").RunCommand(mc.ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&commandResult)
	if err != nil {
		return err
	}
	version := commandResult["version"]

	// Instantiate ConnectorType
	mc.connectorType = iface.ConnectorType{DbType: connectorDBType, Version: version.(string), Spec: connectorSpec}
	// Instantiate ConnectorCapabilities
	mc.connectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: true, IntegrityCheck: true, Resumability: true}
	// Instantiate ConnectorStatus
	mc.status = iface.ConnectorStatus{WriteLSN: 0}

	// Get the coordinator endpoint
	coord, err := mc.t.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	mc.coord = coord

	// Generate connector ID for resumability purposes
	id := generateConnectorID(mc.settings.ConnectionString)

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: mc.desc, Type: mc.connectorType, Cap: mc.connectorCapabilities, Id: id}
	// Register the connector
	mc.id, err = coord.RegisterConnector(connectorDetails, mc)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	slog.Info("MongoConnector has been configured with ID " + (string)(mc.id))

	return nil
}

func (mc *MongoConnector) Teardown() {
	if mc.client != nil {
		if err := mc.client.Disconnect(mc.ctx); err != nil {
			slog.Warn(fmt.Sprintf("Failed to disconnect from MongoDB: %v", err))
		}
	}
}

func (mc *MongoConnector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	// this is what came for the flow
	mc.flowConnCapabilities = reqCap
	slog.Debug(fmt.Sprintf("Connector %s set capabilities for flow %s: %+v", mc.id, flowId, reqCap))
}

// TODO (AK, 6/2024): this should be split to a separate class and/or functions
func (mc *MongoConnector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	mc.flowCtx, mc.flowCancelFunc = context.WithCancel(mc.ctx)
	mc.flowId = flowId

	tasks := readPlan.Tasks
	if len(tasks) == 0 {
		return errors.New("no tasks to copy")
	}
	mc.flowCDCResumeToken = readPlan.CdcResumeToken

	slog.Debug(fmt.Sprintf("StartReadToChannel Tasks: %+v", tasks))

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := mc.t.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	// Declare two channels to wait for the change stream reader and the initial sync to finish
	changeStreamDone := make(chan struct{})
	initialSyncDone := make(chan struct{})

	type ReaderProgress struct {
		initialSyncDocs    atomic.Uint64
		changeStreamEvents uint64
		tasksTotal         uint64
		tasksCompleted     uint64
	}

	readerProgress := ReaderProgress{ //XXX (AK, 6/2024): should we handle overflow? Also, should we use atomic types?
		changeStreamEvents: 0,
		tasksTotal:         uint64(len(tasks)),
		tasksCompleted:     0,
	}

	readerProgress.initialSyncDocs.Store(0)

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		startTime := time.Now()
		operations := uint64(0)
		for {
			select {
			case <-mc.flowCtx.Done():
				return
			case <-ticker.C:
				elapsedTime := time.Since(startTime).Seconds()
				operations_delta := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents - operations
				opsPerSec := math.Floor(float64(operations_delta) / elapsedTime)
				// Print reader progress
				slog.Info(fmt.Sprintf("Reader Progress: Initial Sync Docs - %d (%d/%d tasks completed), Change Stream Events - %d, Operations per Second - %.2f",
					readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted, readerProgress.tasksTotal, readerProgress.changeStreamEvents, opsPerSec))

				startTime = time.Now()
				operations = readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
			}
		}
	}()

	// kick off LSN tracking
	// TODO (AK, 6/2024): implement this proper - this is a very BAD, bad placeholder.
	go func() {
		slog.Info(fmt.Sprintf("Connector %s is starting to track LSN for flow %s", mc.id, flowId))
		opts := moptions.ChangeStream().SetStartAfter(mc.flowCDCResumeToken)
		var nsFilter bson.D
		if options.Namespace != nil { //means namespace filtering was requested
			nsFilter = createChangeStreamNamespaceFilterFromTasks(tasks)
		} else {
			nsFilter = createChangeStreamNamespaceFilter()
		}

		changeStream, err := mc.client.Watch(mc.flowCtx, mongo.Pipeline{
			{{"$match", nsFilter}},
		}, opts)
		if err != nil {
			slog.Error(fmt.Sprintf("LSN tracker: Failed to open change stream: %v", err))
			return
		}
		defer changeStream.Close(mc.flowCtx)

		for changeStream.Next(mc.flowCtx) {
			var change bson.M
			if err := changeStream.Decode(&change); err != nil {
				slog.Error(fmt.Sprintf("LSN tracker: Failed to decode change stream event: %v", err))
				continue
			}

			if mc.shouldIgnoreChangeStreamEvent(change) {
				continue
			}

			mc.status.WriteLSN++
		}

		if err := changeStream.Err(); err != nil {
			if errors.Is(context.Canceled, mc.flowCtx.Err()) {
				slog.Debug(fmt.Sprintf("Change stream error: %v, but the context was cancelled", err))
			} else {
				slog.Error(fmt.Sprintf("Change stream error: %v", err))
			}
		}
	}()

	// kick off the change stream reader
	go func() {
		//wait for the initial sync to finish
		<-initialSyncDone
		defer close(changeStreamDone)

		// start sending periodic barrier messages with cdc resume token updates
		go func() {
			ticker := time.NewTicker(mc.settings.CdcResumeTokenUpdateInterval)
			defer ticker.Stop()
			for {
				select {
				case <-mc.flowCtx.Done():
					return
				case <-changeStreamDone:
					return
				case <-ticker.C:
					// send a barrier message with the updated resume token
					dataChannel <- iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_CdcResumeTokenUpdate, BarrierCdcResumeToken: mc.flowCDCResumeToken}
				}
			}
		}()

		var lsn int64 = 0

		slog.Info(fmt.Sprintf("Connector %s is starting to read change stream for flow %s", mc.id, flowId))
		slog.Debug(fmt.Sprintf("Connector %s change stream start@ %v", mc.id, mc.flowCDCResumeToken))

		opts := moptions.ChangeStream().SetStartAfter(mc.flowCDCResumeToken).SetFullDocument("updateLookup")
		var nsFilter bson.D
		if options.Namespace != nil { //means namespace filtering was requested
			nsFilter = createChangeStreamNamespaceFilterFromTasks(tasks)
		} else {
			nsFilter = createChangeStreamNamespaceFilter()
		}
		slog.Debug(fmt.Sprintf("Change stream namespace filter: %v", nsFilter))

		changeStream, err := mc.client.Watch(mc.flowCtx, mongo.Pipeline{
			{{"$match", nsFilter}},
		}, opts)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to open change stream: %v", err))
			return
		}
		defer changeStream.Close(mc.flowCtx)

		mc.status.CDCActive = true

		for changeStream.Next(mc.flowCtx) {
			var change bson.M
			if err := changeStream.Decode(&change); err != nil {
				slog.Error(fmt.Sprintf("Failed to decode change stream event: %v", err))
				continue
			}

			if mc.shouldIgnoreChangeStreamEvent(change) {
				continue
			}

			readerProgress.changeStreamEvents++ //XXX Should we do atomic add here as well, shared variable multiple threads
			lsn++

			dataMsg, err := mc.convertChangeStreamEventToDataMessage(change)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to convert change stream event to data message: %v", err))
				continue
			}
			if dataMsg.MutationType == iface.MutationType_Reserved { //TODO (AK, 6/2024): find a better way to indicate that we need to skip this event
				slog.Debug(fmt.Sprintf("Skipping the event: %v", change))
				continue
			}
			//send the data message
			dataMsg.SeqNum = lsn
			dataChannel <- dataMsg

			//update the last seen resume token
			mc.flowCDCResumeToken = changeStream.ResumeToken()
		}

		if err := changeStream.Err(); err != nil {
			if errors.Is(context.Canceled, mc.flowCtx.Err()) {
				slog.Debug(fmt.Sprintf("Change stream error: %v, but the context was cancelled", err))
			} else {
				slog.Error(fmt.Sprintf("Change stream error: %v", err))
			}
		}
	}()

	// kick off the initial sync
	go func() {
		defer close(initialSyncDone)

		slog.Info(fmt.Sprintf("Connector %s is starting initial sync for flow %s", mc.id, flowId))

		//create a channel to distribute tasks to copiers
		taskChannel := make(chan iface.ReadPlanTask)
		//create a wait group to wait for all copiers to finish
		var wg sync.WaitGroup
		wg.Add(mc.settings.initialSyncNumParallelCopiers)

		//start 4 copiers
		for i := 0; i < mc.settings.initialSyncNumParallelCopiers; i++ {
			go func() {
				defer wg.Done()
				for task := range taskChannel {
					slog.Debug(fmt.Sprintf("Processing task: %v", task))
					db := task.Def.Db
					col := task.Def.Col
					collection := mc.client.Database(db).Collection(col)
					cursor, err := collection.Find(mc.flowCtx, bson.D{})
					if err != nil {
						if errors.Is(context.Canceled, mc.flowCtx.Err()) {
							slog.Debug(fmt.Sprintf("Find error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Failed to find documents in collection: %v", err))
						}
						continue //XXX: what happens later with this task if we skip it?
					}
					loc := iface.Location{Database: db, Collection: col}
					var dataBatch [][]byte
					var batch_idx int
					for cursor.Next(mc.flowCtx) {
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
						if errors.Is(context.Canceled, mc.flowCtx.Err()) {
							slog.Debug(fmt.Sprintf("Cursor error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Cursor error: %v", err))
						}
					} else {
						cursor.Close(mc.flowCtx)
						readerProgress.tasksCompleted++ //XXX Should we do atomic add here as well, shared variable multiple threads
						slog.Debug(fmt.Sprintf("Done processing task: %v", task))
						//notify the coordinator that the task is done from our side
						mc.coord.NotifyTaskDone(mc.flowId, mc.id, task.Id, nil)
						//send a barrier message to signal the end of the task
						if mc.flowConnCapabilities.Resumability { //send only if the flow supports resumability otherwise who knows what will happen on the recieving side
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

		slog.Info(fmt.Sprintf("Connector %s is done reading for flow %s", mc.id, flowId))
		err := mc.coord.NotifyDone(flowId, mc.id) //TODO (AK, 6/2024): Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", mc.id, flowId, err))
		}
	}()

	return nil
}

func (mc *MongoConnector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	mc.flowCtx, mc.flowCancelFunc = context.WithCancel(mc.ctx)
	mc.flowId = flowId

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := mc.t.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	type WriterProgress struct {
		dataMessages atomic.Uint64
	}

	writerProgress := WriterProgress{
		//XXX (AK, 6/2024): should we handle overflow? Also, should we use atomic types?
	}
	writerProgress.dataMessages.Store(0)

	// create a batch assembly
	flowParallelWriter := NewParallelWriter(mc.flowCtx, mc, mc.settings.numParallelWriters)
	flowParallelWriter.Start()

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		for {

			select {
			case <-mc.flowCtx.Done():
				return
			case <-ticker.C:
				// Print writer progress
				slog.Debug(fmt.Sprintf("Writer Progress: Data Messages - %d", writerProgress.dataMessages.Load()))
			}
		}
	}()

	go func() {
		for loop := true; loop; {
			select {
			case <-mc.flowCtx.Done():
				loop = false
			case dataMsg, ok := <-dataChannel:
				if !ok {
					// channel is closed which is a signal for us to stop
					loop = false
					break
				}
				// Check if this is a barrier first
				if dataMsg.MutationType == iface.MutationType_Barrier {
					err := flowParallelWriter.ScheduleBarrier(dataMsg)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to schedule barrier message: %v", err))
					}
				} else {
					// Process the data message
					writerProgress.dataMessages.Add(1)
					err := flowParallelWriter.ScheduleDataMessage(dataMsg)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to schedule data message: %v", err))
					}
				}
			}
		}

		slog.Info(fmt.Sprintf("Connector %s is done writing for flow %s", mc.id, flowId))
		err := mc.coord.NotifyDone(flowId, mc.id)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", mc.id, flowId, err))
		}
	}()

	return nil
}

func (mc *MongoConnector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions) error {
	// need to make this async to honor the spec
	go mc.doIntegrityCheck_sync(flowId, options)

	return nil
}

func (mc *MongoConnector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return mc.status
}

func (mc *MongoConnector) Interrupt(flowId iface.FlowID) error {
	mc.flowCancelFunc()
	return nil
}

func (mc *MongoConnector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go func() {
		// Retrieve the latest resume token before we start reading anything
		// We will use the resume token to start the change stream
		resumeToken, err := getLatestResumeToken(mc.ctx, mc.client)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get latest resume token: %v", err))
			return
		}

		tasks, err := mc.createInitialCopyTasks(options.Namespace)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to create initial copy tasks: %v", err))
			return
		}
		mc.flowCDCResumeToken = resumeToken
		plan := iface.ConnectorReadPlan{Tasks: tasks, CdcResumeToken: mc.flowCDCResumeToken}

		err = mc.coord.PostReadPlanningResult(flowId, mc.id, iface.ConnectorReadPlanResult{ReadPlan: plan, Success: true})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed notifying coordinator about read planning done: %v", err))
		}
	}()
	return nil
}
