/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package cosmos

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	mongoconn "github.com/adiom-data/dsync/connectors/mongo"
	"github.com/adiom-data/dsync/protocol/iface"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	moptions "go.mongodb.org/mongo-driver/mongo/options"
)

const (
	connectorDBType string = "CosmosDB"               // We're a CosmosDB-compatible connector
	connectorSpec   string = "MongoDB Provisioned RU" // Only compatible with MongoDB API and provisioned deployments
)

type Connector struct {
	mongoconn.BaseMongoConnector
	settings ConnectorSettings

	//TODO (AK, 6/2024): these should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	flowCDCResumeTokenMap     *TokenMap     //stores the resume token for each namespace
	flowDeletesTriggerChannel chan struct{} //channel to trigger deletes

	witnessMongoClient *mongo.Client //for use in emulating deletes
}

type ConnectorSettings struct {
	mongoconn.ConnectorSettings
	MaxNumNamespaces            int    //we don't want to have too many parallel changestreams (after 10-15 we saw perf impact)
	TargetDocCountPerPartition  int64  //target number of documents per partition (256k docs is 256MB with 1KB average doc size)
	NumParallelPartitionWorkers int    //number of workers used for partitioning
	partitionKey                string //partition key to use for collections

	EmulateDeletes         bool // if true, we will generate delete events
	DeletesCheckInterval   time.Duration
	WitnessMongoConnString string
}

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

func NewCosmosConnector(desc string, settings ConnectorSettings) *Connector {
	// Set default values
	setDefault(&settings.ServerConnectTimeout, 15*time.Second)
	setDefault(&settings.PingTimeout, 10*time.Second)
	setDefault(&settings.InitialSyncNumParallelCopiers, 8)
	setDefault(&settings.WriterMaxBatchSize, 0)
	setDefault(&settings.NumParallelWriters, 4)
	setDefault(&settings.CdcResumeTokenUpdateInterval, 60*time.Second)
	setDefault(&settings.MaxNumNamespaces, 8)
	setDefault(&settings.TargetDocCountPerPartition, 512*1000)
	setDefault(&settings.NumParallelPartitionWorkers, 4)
	setDefault(&settings.DeletesCheckInterval, 60*time.Second)
	// settings.WriterMaxBatchSize = 0 // default 0, no limit

	settings.partitionKey = "_id"

	return &Connector{BaseMongoConnector: mongoconn.BaseMongoConnector{Desc: desc, Settings: settings.ConnectorSettings}, settings: settings}
}

func (cc *Connector) Setup(ctx context.Context, t iface.Transport) error {
	cc.Ctx = ctx
	cc.T = t

	// Connect to the witness MongoDB instance
	if cc.settings.EmulateDeletes {
		ctxConnect, cancel := context.WithTimeout(cc.Ctx, cc.settings.ServerConnectTimeout)
		defer cancel()
		clientOptions := moptions.Client().ApplyURI(cc.settings.WitnessMongoConnString).SetConnectTimeout(cc.settings.ServerConnectTimeout)
		client, err := mongo.Connect(ctxConnect, clientOptions)
		if err != nil {
			return err
		}
		cc.witnessMongoClient = client
	}

	// Connect to the MongoDB instance
	ctxConnect, cancel := context.WithTimeout(cc.Ctx, cc.settings.ServerConnectTimeout)
	defer cancel()
	clientOptions := moptions.Client().ApplyURI(cc.settings.ConnectionString).SetConnectTimeout(cc.settings.ServerConnectTimeout)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	cc.Client = client

	// Check the connection
	ctxPing, cancel := context.WithTimeout(cc.Ctx, cc.settings.PingTimeout)
	defer cancel()
	err = cc.Client.Ping(ctxPing, nil)
	if err != nil {
		return err
	}

	// Get version of the MongoDB server
	var commandResult bson.M
	err = cc.Client.Database("admin").RunCommand(cc.Ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&commandResult)
	if err != nil {
		return err
	}
	version := commandResult["version"]

	// Instantiate ConnectorType
	cc.ConnectorType = iface.ConnectorType{DbType: connectorDBType, Version: version.(string), Spec: connectorSpec}
	// Instantiate ConnectorCapabilities, current capabilities are source only
	cc.ConnectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: true, IntegrityCheck: true, Resumability: true}
	// Instantiate ConnectorStatus and ProgressTracker
	cc.Status = iface.ConnectorStatus{
		WriteLSN: 0,
	}
	cc.ProgressTracker = mongoconn.NewProgressTracker(&cc.Status, cc.Client, cc.Ctx)

	// Get the coordinator endpoint
	coord, err := cc.T.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	cc.Coord = coord

	// Generate connector ID for resumability purposes
	id := generateConnectorID(cc.settings.ConnectionString)

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: cc.Desc, Type: cc.ConnectorType, Cap: cc.ConnectorCapabilities, Id: id}
	// Register the connector
	cc.ID, err = coord.RegisterConnector(connectorDetails, cc)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	slog.Info("Cosmos Connector has been configured with ID " + (string)(cc.ID))
	slog.Debug(fmt.Sprintf("Connector config: %+v", redactedSettings(cc.settings)))

	return nil
}

func (cc *Connector) Teardown() {
	if cc.Client != nil {
		cc.Client.Disconnect(cc.Ctx)
	}

	if cc.witnessMongoClient != nil {
		cc.witnessMongoClient.Disconnect(cc.Ctx)
	}
}

func (cc *Connector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	// this is what came for the flow
	cc.FlowConnCapabilities = reqCap
	slog.Debug(fmt.Sprintf("Connector %s set capabilities for flow %s: %+v", cc.ID, flowId, reqCap))
}

// TODO (AK, 6/2024): this should be split to a separate class and/or functions
func (cc *Connector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	cc.FlowCtx, cc.FlowCancelFunc = context.WithCancel(cc.Ctx)
	cc.FlowId = flowId

	tasks := readPlan.Tasks
	slog.Info(fmt.Sprintf("number of tasks: %d", len(tasks)))
	namespaces := make([]iface.Namespace, 0)

	cc.ProgressTracker.RestoreProgressDetails(tasks)
	// reset doc counts for all namespaces to actual for more accurate progress reporting
	//XXX: is it safe to just do it async? we don't want to block the command from returning
	go cc.ProgressTracker.ResetNsProgressEstimatedDocCounts()

	if len(tasks) == 0 && options.Mode != iface.SyncModeCDC {
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

	//placeholder for how to get namespaces, can use status struct from progress output later
	for loc := range cc.flowCDCResumeTokenMap.Map {
		namespaces = append(namespaces, iface.Namespace{Db: loc.Database, Col: loc.Collection})
	}

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := cc.T.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	// Declare two channels to wait for the change stream reader and the initial sync to finish
	changeStreamDone := make(chan struct{})
	initialSyncDone := make(chan struct{})

	readerProgress := ReaderProgress{ //initialSyncDocs is atomic counters
		tasksTotal:         uint64(len(tasks)),
		tasksStarted:       0,
		tasksCompleted:     0,
		changeStreamEvents: 0,
		deletesCaught:      0,
	}

	readerProgress.initialSyncDocs.Store(0)

	// start printing progress
	go cc.printProgress(&readerProgress)

	// kick off LSN tracking
	go cc.startGlobalLsnWorkers(cc.FlowCtx, namespaces, readPlan.CreatedAtEpoch)

	// kick off the change stream reader
	go func() {
		//wait for the initial sync to finish
		<-initialSyncDone
		cc.Status.SyncState = iface.ChangeStreamSyncState
		defer close(changeStreamDone)

		// prepare the delete trigger channel and start the deletes worker, if necessary
		if cc.settings.EmulateDeletes {
			slog.Info(fmt.Sprintf("Connector %s is starting deletes emulation worker for flow %s", cc.ID, flowId))
			cc.flowDeletesTriggerChannel = make(chan struct{}, 100) //XXX: do we need to close or reset this later?
			//start the worker that will emulate deletes
			cc.flowDeletesTriggerChannel <- struct{}{} //trigger the first cycle
			go func() {
				ticker := time.NewTicker(cc.settings.DeletesCheckInterval)
				defer ticker.Stop()
				for {
					select {
					case <-cc.FlowCtx.Done():
						return
					case <-changeStreamDone:
						return
					case <-ticker.C:
						cc.flowDeletesTriggerChannel <- struct{}{}
					case <-cc.flowDeletesTriggerChannel:
						// check for deletes
						slog.Debug(fmt.Sprintf("Checking for deletes for flow %s", flowId))
						cc.Status.AdditionalInfo = "Deletes Cycle Active"

						readerProgress.deletesCaught += cc.checkForDeletes_sync(flowId, options, dataChannel)
						cc.Status.ProgressMetrics.DeletesCaught = readerProgress.deletesCaught
						// reset the timer - no point in checking too often
						ticker.Reset(cc.settings.DeletesCheckInterval)

						slog.Debug(fmt.Sprintf("Done checking for deletes for flow %s", flowId))
						cc.Status.AdditionalInfo = ""
					}
				}
			}()
		}

		// start sending periodic barrier messages with cdc resume token updates
		go func() {
			ticker := time.NewTicker(cc.settings.CdcResumeTokenUpdateInterval)
			defer ticker.Stop()
			for {
				select {
				case <-cc.FlowCtx.Done():
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
		cc.StartConcurrentChangeStreams(cc.FlowCtx, namespaces, &readerProgress, readPlan.CreatedAtEpoch, dataChannel)
	}()

	// kick off the initial sync
	go func() {
		defer close(initialSyncDone)

		// if we have no tasks (e.g. we are in CDC mode), we skip the initial sync
		if len(tasks) == 0 {
			slog.Info(fmt.Sprintf("Connector %s is skipping initial sync for flow %s", cc.ID, flowId))
			return
		}

		slog.Info(fmt.Sprintf("Connector %s is starting initial sync for flow %s", cc.ID, flowId))
		cc.Status.SyncState = iface.InitialSyncSyncState

		//create a channel to distribute tasks to copiers
		taskChannel := make(chan iface.ReadPlanTask)
		//create a wait group to wait for all copiers to finish
		var wg sync.WaitGroup
		wg.Add(cc.settings.InitialSyncNumParallelCopiers)

		//start 4 copiers
		for i := 0; i < cc.settings.InitialSyncNumParallelCopiers; i++ {
			go func() {
				defer wg.Done()
				for task := range taskChannel {
					slog.Debug(fmt.Sprintf("Processing task: %v", task))
					db := task.Def.Db
					col := task.Def.Col

					//retrieve namespace status struct for this namespace to update accordingly
					ns := iface.Namespace{Db: db, Col: col}

					cc.ProgressTracker.TaskStartedProgressUpdate(ns, task.Id)

					collection := cc.Client.Database(db).Collection(col)
					cursor, err := createFindQuery(cc.FlowCtx, collection, task)
					if err != nil {
						if cc.FlowCtx.Err() == context.Canceled {
							slog.Debug(fmt.Sprintf("Find error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Failed to find documents in collection: %v", err))
						}
						continue
					}
					loc := iface.Location{Database: db, Collection: col}
					var dataBatch [][]byte
					var batch_idx int
					var docs int64
					for cursor.Next(cc.FlowCtx) {
						if dataBatch == nil {
							dataBatch = make([][]byte, cursor.RemainingBatchLength()+1) //preallocate the batch
							batch_idx = 0
						}
						rawData := cursor.Current
						data := []byte(rawData)
						//update the docs counters
						readerProgress.initialSyncDocs.Add(1)

						cc.ProgressTracker.TaskInProgressUpdate(ns)
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
						if cc.FlowCtx.Err() == context.Canceled {
							slog.Debug(fmt.Sprintf("Cursor error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Cursor error: %v", err))
						}
					} else {
						cursor.Close(cc.FlowCtx)
						readerProgress.tasksCompleted++ //XXX Should we do atomic add here as well, shared variable multiple threads

						//update the progress after completing the task and create task metadata to pass to coordinator to persist
						cc.ProgressTracker.TaskDoneProgressUpdate(ns, task.Id)

						slog.Debug(fmt.Sprintf("Done processing task: %v", task))
						//notify the coordinator that the task is done from our side
						taskData := iface.TaskDoneMeta{DocsCopied: docs}
						cc.Coord.NotifyTaskDone(cc.FlowId, cc.ID, task.Id, &taskData)
						//send a barrier message to signal the end of the task
						if cc.FlowConnCapabilities.Resumability { //send only if the flow supports resumability otherwise who knows what will happen on the receiving side
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

		slog.Info(fmt.Sprintf("Connector %s is done reading for flow %s", cc.ID, flowId))
		err := cc.Coord.NotifyDone(flowId, cc.ID) //TODO (AK, 6/2024): Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", cc.ID, flowId, err))
		}
	}()

	return nil
}

func (cc *Connector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	cc.FlowCtx, cc.FlowCancelFunc = context.WithCancel(cc.Ctx)
	cc.FlowId = flowId

	// get data channel from transport interface based on the provided ID
	dataChannel, err := cc.T.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	// create writer progress for keeping track of number of data messages
	type WriterProgress struct {
		dataMessages atomic.Uint64
	}
	writerProgress := WriterProgress{}

	// initialize with 0 data messages
	writerProgress.dataMessages.Store(0)

	// create a batch assembly for initializing/starting parallel writers
	flowParallelWriter := mongoconn.NewParallelWriter(cc.FlowCtx, cc, cc.settings.NumParallelWriters, cc.settings.WriterMaxBatchSize)
	flowParallelWriter.Start()

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-cc.FlowCtx.Done():
				return
			case <-ticker.C:
				// print writer progress
				slog.Debug(fmt.Sprintf("Writer Progress: Data Messages - %d", writerProgress.dataMessages.Load()))
			}
		}
	}()

	// start processing messages
	go func() {
		for loop := true; loop; {
			select {
			case <-cc.FlowCtx.Done():
				loop = false
			case dataMsg, ok := <-dataChannel:
				if !ok {
					// channel is closed which is a signal for us to stop
					loop = false
					break
				}
				// check message is a barrier first
				if dataMsg.MutationType == iface.MutationType_Barrier {
					err := flowParallelWriter.ScheduleBarrier(dataMsg)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to schedule barrier message: %v", err))
					}
				} else {
					// process the data message
					writerProgress.dataMessages.Add(1)
					cc.Status.WriteLSN = max(dataMsg.SeqNum, cc.Status.WriteLSN)
					err := flowParallelWriter.ScheduleDataMessage(dataMsg)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to schedule data message: %v", err))
					}
				}
			}
		}

		flowParallelWriter.StopAndWait()
		slog.Info(fmt.Sprintf("Connector %s is done writing for flow %s", cc.ID, flowId))
		err := cc.Coord.NotifyDone(flowId, cc.ID)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", cc.ID, flowId, err))
		}

	}()
	return nil
}

func (cc *Connector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go func() {
		// Retrieve the latest resume token before we start reading anything
		// We will use the resume token to start the change stream
		cc.Status.SyncState = iface.ReadPlanningSyncState
		namespaces, tasks, err := cc.createInitialCopyTasks(options.Namespace, options.Mode)

		if err != nil {
			slog.Error(fmt.Sprintf("Failed to create initial copy tasks: %v", err))
			return
		}
		slog.Debug(fmt.Sprintf("created tasks: %v", tasks))

		tokenMap := NewTokenMap()

		//create resume token for each task
		wg := sync.WaitGroup{}
		for _, ns := range namespaces {
			wg.Add(1)
			go func(ns iface.Namespace) {
				defer wg.Done()
				loc := iface.Location{Database: ns.Db, Collection: ns.Col}
				resumeToken, err := getLatestResumeToken(cc.Ctx, cc.Client, loc)
				if err != nil {
					slog.Error(fmt.Sprintf("Failed to get latest resume token for namespace %v: %v", ns, err))
					return
				}
				tokenMap.AddToken(loc, resumeToken)
			}(ns)
		}
		wg.Wait()

		slog.Debug(fmt.Sprintf("Read Plan Resume token map: %v", tokenMap.Map))
		//serialize the resume token map
		flowCDCResumeToken, err := tokenMap.encodeMap()
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to serialize the resume token map: %v", err))
		}

		plan := iface.ConnectorReadPlan{Tasks: tasks, CdcResumeToken: flowCDCResumeToken, CreatedAtEpoch: time.Now().Unix()}

		err = cc.Coord.PostReadPlanningResult(flowId, cc.ID, iface.ConnectorReadPlanResult{ReadPlan: plan, Success: true})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed notifying coordinator about read planning done: %v", err))
		}
	}()
	return nil
}
