/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package mongo

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
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

type BaseMongoConnector struct {
	Desc string

	Settings ConnectorSettings
	Client   *mongo.Client
	Ctx      context.Context

	T  iface.Transport
	ID iface.ConnectorID

	ConnectorType         iface.ConnectorType
	ConnectorCapabilities iface.ConnectorCapabilities

	Coord iface.CoordinatorIConnectorSignal

	//TODO (AK, 6/2024): these should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	Status               iface.ConnectorStatus
	FlowCtx              context.Context
	FlowCancelFunc       context.CancelFunc
	FlowId               iface.FlowID
	FlowConnCapabilities iface.ConnectorCapabilities
	FlowCDCResumeToken   bson.Raw

	ProgressTracker *ProgressTracker
}

type ConnectorSettings struct {
	ConnectionString string

	ServerConnectTimeout          time.Duration
	PingTimeout                   time.Duration
	InitialSyncNumParallelCopiers int
	WriterMaxBatchSize            int // applies to batch inserts only; 0 means no limit
	NumParallelWriters            int
	CdcResumeTokenUpdateInterval  time.Duration
}

type Connector struct {
	BaseMongoConnector
}

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

func NewMongoConnector(desc string, settings ConnectorSettings) *Connector {
	// Set default values
	setDefault(&settings.ServerConnectTimeout, 10*time.Second)
	setDefault(&settings.PingTimeout, 2*time.Second)
	setDefault(&settings.InitialSyncNumParallelCopiers, 4)
	setDefault(&settings.NumParallelWriters, 4)
	setDefault(&settings.CdcResumeTokenUpdateInterval, 60*time.Second)
	setDefault(&settings.WriterMaxBatchSize, 0)
	return &Connector{BaseMongoConnector: BaseMongoConnector{Desc: desc, Settings: settings}}
}

func (mc *Connector) Setup(ctx context.Context, t iface.Transport) error {
	mc.Ctx = ctx
	mc.T = t

	// Connect to the MongoDB instance
	ctxConnect, cancel := context.WithTimeout(mc.Ctx, mc.Settings.ServerConnectTimeout)
	defer cancel()
	clientOptions := moptions.Client().ApplyURI(mc.Settings.ConnectionString).SetConnectTimeout(mc.Settings.ServerConnectTimeout)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	mc.Client = client

	// Check the connection
	ctxPing, cancel := context.WithTimeout(mc.Ctx, mc.Settings.PingTimeout)
	defer cancel()
	err = mc.Client.Ping(ctxPing, nil)
	if err != nil {
		return err
	}

	// Get version of the MongoDB server
	var commandResult bson.M
	err = mc.Client.Database("admin").RunCommand(mc.Ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&commandResult)
	if err != nil {
		return err
	}
	version := commandResult["version"]

	// Instantiate ConnectorType
	mc.ConnectorType = iface.ConnectorType{DbType: connectorDBType, Version: version.(string), Spec: connectorSpec}
	// Instantiate ConnectorCapabilities
	mc.ConnectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: true, IntegrityCheck: true, Resumability: true}
	// Instantiate ConnectorStatus and ProgressTracker
	mc.Status = iface.ConnectorStatus{
		WriteLSN: 0,
	}
	mc.ProgressTracker = NewProgressTracker(&mc.Status, mc.Client, mc.Ctx)

	// Get the coordinator endpoint
	coord, err := mc.T.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	mc.Coord = coord

	// Generate connector ID for resumability purposes
	id := generateConnectorID(mc.Settings.ConnectionString)

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: mc.Desc, Type: mc.ConnectorType, Cap: mc.ConnectorCapabilities, Id: id}
	// Register the connector
	mc.ID, err = coord.RegisterConnector(connectorDetails, mc)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	slog.Info("MongoConnector has been configured with ID " + (string)(mc.ID))
	slog.Debug(fmt.Sprintf("Connector config: %+v", redactedSettings(mc.Settings)))

	return nil
}

func (mc *Connector) Teardown() {
	if mc.Client != nil {
		if err := mc.Client.Disconnect(mc.Ctx); err != nil {
			slog.Warn(fmt.Sprintf("Failed to disconnect from MongoDb: %v", err))
		}
	}
}

func (mc *BaseMongoConnector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	// this is what came for the flow
	mc.FlowConnCapabilities = reqCap
	slog.Debug(fmt.Sprintf("Connector %s set capabilities for flow %s: %+v", mc.ID, flowId, reqCap))
}

// TODO (AK, 6/2024): this should be split to a separate class and/or functions
func (mc *Connector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	mc.FlowCtx, mc.FlowCancelFunc = context.WithCancel(mc.Ctx)
	mc.FlowId = flowId

	tasks := readPlan.Tasks
	slog.Info(fmt.Sprintf("number of tasks: %d", len(tasks)))

	// reset doc counts for all namespaces to actual for more accurate progress reporting
	mc.ProgressTracker.RestoreProgressDetails(tasks)
	go mc.ProgressTracker.ResetNsProgressEstimatedDocCounts()

	if len(tasks) == 0 && options.Mode != iface.SyncModeCDC {
		return errors.New("no tasks to copy")
	}
	mc.FlowCDCResumeToken = readPlan.CdcResumeToken

	slog.Debug(fmt.Sprintf("StartReadToChannel Tasks: %+v", tasks))

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := mc.T.GetDataChannelEndpoint(dataChannelId)
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
		tasksStarted       uint64
		tasksCompleted     uint64
	}

	readerProgress := ReaderProgress{ //XXX (AK, 6/2024): should we handle overflow? Also, should we use atomic types?
		changeStreamEvents: 0,
		tasksTotal:         uint64(len(tasks)),
		tasksStarted:       0,
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
			case <-mc.FlowCtx.Done():
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
		slog.Info(fmt.Sprintf("Connector %s is starting to track LSN for flow %s", mc.ID, flowId))
		opts := moptions.ChangeStream().SetStartAfter(mc.FlowCDCResumeToken)
		var nsFilter bson.D
		if options.Namespace != nil { //means namespace filtering was requested
			nsFilter = createChangeStreamNamespaceFilterFromTasks(tasks)
		} else {
			nsFilter = createChangeStreamNamespaceFilter()
		}

		changeStream, err := mc.Client.Watch(mc.FlowCtx, mongo.Pipeline{
			{{"$match", nsFilter}},
		}, opts)
		if err != nil {
			slog.Error(fmt.Sprintf("LSN tracker: Failed to open change stream: %v", err))
			return
		}
		defer changeStream.Close(mc.FlowCtx)

		for changeStream.Next(mc.FlowCtx) {
			var change bson.M
			if err := changeStream.Decode(&change); err != nil {
				slog.Error(fmt.Sprintf("LSN tracker: Failed to decode change stream event: %v", err))
				continue
			}

			if mc.shouldIgnoreChangeStreamEvent(change) {
				continue
			}

			mc.Status.WriteLSN++
		}

		if err := changeStream.Err(); err != nil {
			if errors.Is(mc.FlowCtx.Err(), context.Canceled) {
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
		mc.Status.SyncState = iface.ChangeStreamSyncState
		defer close(changeStreamDone)

		// start sending periodic barrier messages with cdc resume token updates
		go func() {
			ticker := time.NewTicker(mc.Settings.CdcResumeTokenUpdateInterval)
			defer ticker.Stop()
			for {
				select {
				case <-mc.FlowCtx.Done():
					return
				case <-changeStreamDone:
					return
				case <-ticker.C:
					// send a barrier message with the updated resume token
					dataChannel <- iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_CdcResumeTokenUpdate, BarrierCdcResumeToken: mc.FlowCDCResumeToken}
				}
			}
		}()

		var lsn int64 = 0

		slog.Info(fmt.Sprintf("Connector %s is starting to read change stream for flow %s", mc.ID, flowId))
		slog.Debug(fmt.Sprintf("Connector %s change stream start@ %v", mc.ID, mc.FlowCDCResumeToken))

		opts := moptions.ChangeStream().SetStartAfter(mc.FlowCDCResumeToken).SetFullDocument("updateLookup")
		var nsFilter bson.D
		if options.Namespace != nil { //means namespace filtering was requested
			nsFilter = createChangeStreamNamespaceFilterFromTasks(tasks)
		} else {
			nsFilter = createChangeStreamNamespaceFilter()
		}
		slog.Debug(fmt.Sprintf("Change stream namespace filter: %v", nsFilter))

		changeStream, err := mc.Client.Watch(mc.FlowCtx, mongo.Pipeline{
			{{"$match", nsFilter}},
		}, opts)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to open change stream: %v", err))
			return
		}
		defer changeStream.Close(mc.FlowCtx)

		mc.Status.CDCActive = true

		for changeStream.Next(mc.FlowCtx) {
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
			mc.FlowCDCResumeToken = changeStream.ResumeToken()
		}

		if err := changeStream.Err(); err != nil {
			if errors.Is(mc.FlowCtx.Err(), context.Canceled) {
				slog.Debug(fmt.Sprintf("Change stream error: %v, but the context was cancelled", err))
			} else {
				slog.Error(fmt.Sprintf("Change stream error: %v", err))
			}
		}
	}()

	// kick off the initial sync
	go func() {
		defer close(initialSyncDone)
		// if we have no tasks (e.g. we are in CDC mode), we skip the initial sync
		if len(tasks) == 0 {
			slog.Info(fmt.Sprintf("Connector %s is skipping initial sync for flow %s", mc.ID, flowId))
			return
		}

		slog.Info(fmt.Sprintf("Connector %s is starting initial sync for flow %s", mc.ID, flowId))
		mc.Status.SyncState = iface.InitialSyncSyncState

		//create a channel to distribute tasks to copiers
		taskChannel := make(chan iface.ReadPlanTask)
		//create a wait group to wait for all copiers to finish
		var wg sync.WaitGroup
		wg.Add(mc.Settings.InitialSyncNumParallelCopiers)

		//start 4 copiers
		for i := 0; i < mc.Settings.InitialSyncNumParallelCopiers; i++ {
			go func() {
				defer wg.Done()
				for task := range taskChannel {
					slog.Debug(fmt.Sprintf("Processing task: %v", task))
					db := task.Def.Db
					col := task.Def.Col
					collection := mc.Client.Database(db).Collection(col)
					cursor, err := collection.Find(mc.FlowCtx, bson.D{})

					//retrieve namespace status struct for this namespace to update accordingly
					ns := iface.Namespace{Db: db, Col: col}
					mc.ProgressTracker.TaskStartedProgressUpdate(ns, task.Id)

					if err != nil {
						if errors.Is(mc.FlowCtx.Err(), context.Canceled) {
							slog.Debug(fmt.Sprintf("Find error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Failed to find documents in collection: %v", err))
						}
						continue //XXX: what happens later with this task if we skip it?
					}
					loc := iface.Location{Database: db, Collection: col}
					var dataBatch [][]byte
					var batch_idx int
					var docs int64
					for cursor.Next(mc.FlowCtx) {
						if dataBatch == nil {
							dataBatch = make([][]byte, cursor.RemainingBatchLength()+1) //preallocate the batch
							batch_idx = 0
						}
						rawData := cursor.Current
						data := []byte(rawData)
						readerProgress.initialSyncDocs.Add(1)

						mc.ProgressTracker.TaskInProgressUpdate(ns)
						docs++

						dataBatch[batch_idx] = data
						batch_idx++

						if cursor.RemainingBatchLength() == 0 { //no more left in the batch
							dataChannel <- iface.DataMessage{DataBatch: dataBatch, MutationType: iface.MutationType_InsertBatch, Loc: loc}
							//TODO (AK, 6/2024): is it ok that this blocks until the app is terminated if no one reads? (e.g. reader crashes)
							dataBatch = nil
						}
					}
					if err := cursor.Err(); err != nil {
						if errors.Is(mc.FlowCtx.Err(), context.Canceled) {
							slog.Debug(fmt.Sprintf("Cursor error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Cursor error: %v", err))
						}
					} else {
						cursor.Close(mc.FlowCtx)
						readerProgress.tasksCompleted++ //XXX Should we do atomic add here as well, shared variable multiple threads
						// update progress after completing the task and create task metadata to pass to coordinator to persist
						mc.ProgressTracker.TaskDoneProgressUpdate(ns, task.Id)
						slog.Debug(fmt.Sprintf("Done processing task: %v", task))
						//notify the coordinator that the task is done from our side
						taskData := iface.TaskDoneMeta{DocsCopied: docs}
						mc.Coord.NotifyTaskDone(mc.FlowId, mc.ID, task.Id, &taskData)
						//send a barrier message to signal the end of the task
						if mc.FlowConnCapabilities.Resumability { //send only if the flow supports resumability otherwise who knows what will happen on the recieving side
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

		dataChannel <- iface.DataMessage{
			MutationType: iface.MutationType_Barrier,
			BarrierType:  iface.BarrierType_Block,
		}
	}()

	// wait for both the change stream reader and the initial sync to finish
	go func() {
		<-initialSyncDone
		<-changeStreamDone

		close(dataChannel) //send a signal downstream that we are done sending data //TODO (AK, 6/2024): is this the right way to do it?

		slog.Info(fmt.Sprintf("Connector %s is done reading for flow %s", mc.ID, flowId))
		err := mc.Coord.NotifyDone(flowId, mc.ID) //TODO (AK, 6/2024): Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", mc.ID, flowId, err))
		}
	}()

	return nil
}

func (mc *Connector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	mc.FlowCtx, mc.FlowCancelFunc = context.WithCancel(mc.Ctx)
	mc.FlowId = flowId

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := mc.T.GetDataChannelEndpoint(dataChannelId)
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
	flowParallelWriter := NewParallelWriter(mc.FlowCtx, mc, mc.Settings.NumParallelWriters, mc.Settings.WriterMaxBatchSize)
	flowParallelWriter.Start()

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		for {

			select {
			case <-mc.FlowCtx.Done():
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
			case <-mc.FlowCtx.Done():
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

		flowParallelWriter.StopAndWait()
		slog.Info(fmt.Sprintf("Connector %s is done writing for flow %s", mc.ID, flowId))
		err := mc.Coord.NotifyDone(flowId, mc.ID)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", mc.ID, flowId, err))
		}
	}()

	return nil
}

func (mc *BaseMongoConnector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return mc.Status
}

func (mc *BaseMongoConnector) Interrupt(flowId iface.FlowID) error {
	if mc.FlowCancelFunc != nil {
		mc.FlowCancelFunc()
	}
	return nil
}

func (mc *Connector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go func() {
		// Retrieve the latest resume token before we start reading anything
		// We will use the resume token to start the change stream
		mc.Status.SyncState = iface.ReadPlanningSyncState
		resumeToken, err := getLatestResumeToken(mc.Ctx, mc.Client)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to get latest resume token: %v", err))
			return
		}

		var tasks []iface.ReadPlanTask
		if options.Mode == iface.SyncModeCDC {
			tasks = nil
		} else {
			tasks, err = mc.createInitialCopyTasks(options.Namespace)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to create initial copy tasks: %v", err))
				return
			}
		}
		mc.FlowCDCResumeToken = resumeToken
		plan := iface.ConnectorReadPlan{Tasks: tasks, CdcResumeToken: mc.FlowCDCResumeToken}

		err = mc.Coord.PostReadPlanningResult(flowId, mc.ID, iface.ConnectorReadPlanResult{ReadPlan: plan, Success: true})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed notifying coordinator about read planning done: %v", err))
		}
	}()
	return nil
}
