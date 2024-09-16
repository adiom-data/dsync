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
	desc string

	settings ConnectorSettings
	client   *mongo.Client
	ctx      context.Context

	t  iface.Transport
	id iface.ConnectorID

	connectorType         iface.ConnectorType
	connectorCapabilities iface.ConnectorCapabilities

	coord iface.CoordinatorIConnectorSignal

	muProgressMetrics sync.Mutex

	//TODO (AK, 6/2024): these should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	status                    iface.ConnectorStatus
	flowCtx                   context.Context
	flowCancelFunc            context.CancelFunc
	flowId                    iface.FlowID
	flowConnCapabilities      iface.ConnectorCapabilities
	flowCDCResumeTokenMap     *TokenMap     //stores the resume token for each namespace
	flowDeletesTriggerChannel chan struct{} //channel to trigger deletes

	witnessMongoClient *mongo.Client //for use in emulating deletes
}

type ConnectorSettings struct {
	ConnectionString string

	ServerConnectTimeout           time.Duration
	PingTimeout                    time.Duration
	InitialSyncNumParallelCopiers  int
	WriterMaxBatchSize             int //0 means no limit (in # of documents)
	NumParallelWriters             int
	CdcResumeTokenUpdateInterval   time.Duration
	NumParallelIntegrityCheckTasks int

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
	setDefault(&settings.NumParallelIntegrityCheckTasks, 4)
	setDefault(&settings.CdcResumeTokenUpdateInterval, 60*time.Second)
	setDefault(&settings.MaxNumNamespaces, 8)
	setDefault(&settings.TargetDocCountPerPartition, 512*1000)
	setDefault(&settings.NumParallelPartitionWorkers, 4)
	setDefault(&settings.DeletesCheckInterval, 60*time.Second)
	// settings.WriterMaxBatchSize = 0 // default 0, no limit

	settings.partitionKey = "_id"

	return &Connector{desc: desc, settings: settings}
}

func (cc *Connector) Setup(ctx context.Context, t iface.Transport) error {
	cc.ctx = ctx
	cc.t = t

	// Connect to the witness MongoDB instance
	if cc.settings.EmulateDeletes {
		ctxConnect, cancel := context.WithTimeout(cc.ctx, cc.settings.ServerConnectTimeout)
		defer cancel()
		clientOptions := moptions.Client().ApplyURI(cc.settings.WitnessMongoConnString).SetConnectTimeout(cc.settings.ServerConnectTimeout)
		client, err := mongo.Connect(ctxConnect, clientOptions)
		if err != nil {
			return err
		}
		cc.witnessMongoClient = client
	}

	// Connect to the MongoDB instance
	ctxConnect, cancel := context.WithTimeout(cc.ctx, cc.settings.ServerConnectTimeout)
	defer cancel()
	clientOptions := moptions.Client().ApplyURI(cc.settings.ConnectionString).SetConnectTimeout(cc.settings.ServerConnectTimeout)
	client, err := mongo.Connect(ctxConnect, clientOptions)
	if err != nil {
		return err
	}
	cc.client = client

	// Check the connection
	ctxPing, cancel := context.WithTimeout(cc.ctx, cc.settings.PingTimeout)
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
	cc.connectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: true, IntegrityCheck: true, Resumability: true}
	// Instantiate ConnectorStatus
	progressMetrics := iface.ProgressMetrics{
		NumDocsSynced:          0,
		TasksTotal:             0,
		TasksStarted:           0,
		TasksCompleted:         0,
		NumNamespaces:          0,
		NumNamespacesCompleted: 0,
		DeletesCaught:          0,
		ChangeStreamEvents:     0,

		NamespaceProgress: make(map[iface.Namespace]*iface.NamespaceStatus),
		Namespaces:        make([]iface.Namespace, 0),
	}

	cc.status = iface.ConnectorStatus{
		WriteLSN:        0,
		ProgressMetrics: progressMetrics,
	}

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
	slog.Debug(fmt.Sprintf("Connector config: %+v", cc.settings))

	return nil
}

func (cc *Connector) Teardown() {
	if cc.client != nil {
		cc.client.Disconnect(cc.ctx)
	}

	if cc.witnessMongoClient != nil {
		cc.witnessMongoClient.Disconnect(cc.ctx)
	}
}

func (cc *Connector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	// this is what came for the flow
	cc.flowConnCapabilities = reqCap
	slog.Debug(fmt.Sprintf("Connector %s set capabilities for flow %s: %+v", cc.id, flowId, reqCap))
}

// TODO (AK, 6/2024): this should be split to a separate class and/or functions
func (cc *Connector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	cc.flowCtx, cc.flowCancelFunc = context.WithCancel(cc.ctx)
	cc.flowId = flowId

	tasks := readPlan.Tasks
	slog.Info(fmt.Sprintf("number of tasks: %d", len(tasks)))
	namespaces := make([]namespace, 0)

	cc.restoreProgressDetails(tasks)
	// reset doc counts for all namespaces to actual for more accurate progress reporting
	//XXX: is it safe to just do it async? we don't want to block the command from returning
	go cc.resetNsProgressEstimatedDocCounts()

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
	for loc, _ := range cc.flowCDCResumeTokenMap.Map {
		namespaces = append(namespaces, namespace{db: loc.Database, col: loc.Collection})
	}

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
		tasksStarted:       0,
		tasksCompleted:     0,
		changeStreamEvents: 0,
		deletesCaught:      0,
	}

	readerProgress.initialSyncDocs.Store(0)

	// start printing progress
	go cc.printProgress(&readerProgress)

	// kick off LSN tracking
	go cc.startGlobalLsnWorkers(cc.flowCtx, namespaces, readPlan.CreatedAtEpoch)

	// kick off the change stream reader
	go func() {
		//wait for the initial sync to finish
		<-initialSyncDone
		cc.status.SyncState = iface.ChangeStreamSyncState
		defer close(changeStreamDone)

		// prepare the delete trigger channel and start the deletes worker, if necessary
		if cc.settings.EmulateDeletes {
			slog.Info(fmt.Sprintf("Connector %s is starting deletes emulation worker for flow %s", cc.id, flowId))
			cc.flowDeletesTriggerChannel = make(chan struct{}, 100) //XXX: do we need to close or reset this later?
			//start the worker that will emulate deletes
			cc.flowDeletesTriggerChannel <- struct{}{} //trigger the first cycle
			go func() {
				ticker := time.NewTicker(cc.settings.DeletesCheckInterval)
				defer ticker.Stop()
				for {
					select {
					case <-cc.flowCtx.Done():
						return
					case <-changeStreamDone:
						return
					case <-ticker.C:
						cc.flowDeletesTriggerChannel <- struct{}{}
					case <-cc.flowDeletesTriggerChannel:
						// check for deletes
						slog.Debug(fmt.Sprintf("Checking for deletes for flow %s", flowId))
						cc.status.AdditionalInfo = "Deletes Cycle Active"

						readerProgress.deletesCaught += cc.checkForDeletes_sync(flowId, options, dataChannel)
						cc.status.ProgressMetrics.DeletesCaught = readerProgress.deletesCaught
						// reset the timer - no point in checking too often
						ticker.Reset(cc.settings.DeletesCheckInterval)

						slog.Debug(fmt.Sprintf("Done checking for deletes for flow %s", flowId))
						cc.status.AdditionalInfo = ""
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
		cc.StartConcurrentChangeStreams(cc.flowCtx, namespaces, &readerProgress, readPlan.CreatedAtEpoch, dataChannel)
	}()

	// kick off the initial sync
	go func() {
		defer close(initialSyncDone)

		// if we have no tasks (e.g. we are in CDC mode), we skip the initial sync
		if len(tasks) == 0 {
			slog.Info(fmt.Sprintf("Connector %s is skipping initial sync for flow %s", cc.id, flowId))
			return
		}

		slog.Info(fmt.Sprintf("Connector %s is starting initial sync for flow %s", cc.id, flowId))
		cc.status.SyncState = iface.InitialSyncSyncState

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

					nsStatus := cc.status.ProgressMetrics.NamespaceProgress[ns]
					cc.taskStartedProgressUpdate(nsStatus, task.Id)

					collection := cc.client.Database(db).Collection(col)
					cursor, err := createFindQuery(cc.flowCtx, collection, task)
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
					var docs int64
					for cursor.Next(cc.flowCtx) {
						if dataBatch == nil {
							dataBatch = make([][]byte, cursor.RemainingBatchLength()+1) //preallocate the batch
							batch_idx = 0
						}
						rawData := cursor.Current
						data := []byte(rawData)
						//update the docs counters
						readerProgress.initialSyncDocs.Add(1)

						cc.taskInProgressUpdate(nsStatus)
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
						if cc.flowCtx.Err() == context.Canceled {
							slog.Debug(fmt.Sprintf("Cursor error: %v, but the context was cancelled", err))
						} else {
							slog.Error(fmt.Sprintf("Cursor error: %v", err))
						}
					} else {
						cursor.Close(cc.flowCtx)
						readerProgress.tasksCompleted++ //XXX Should we do atomic add here as well, shared variable multiple threads

						//update the progress after completing the task and create task metadata to pass to coordinator to persist
						cc.taskDoneProgressUpdate(nsStatus, task.Id)

						slog.Debug(fmt.Sprintf("Done processing task: %v", task))
						//notify the coordinator that the task is done from our side
						taskData := iface.TaskDoneMeta{DocsCopied: docs}
						cc.coord.NotifyTaskDone(cc.flowId, cc.id, task.Id, &taskData)
						//send a barrier message to signal the end of the task
						if cc.flowConnCapabilities.Resumability { //send only if the flow supports resumability otherwise who knows what will happen on the receiving side
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

func (cc *Connector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	// create new context so that the flow can be cancelled gracefully if needed
	cc.flowCtx, cc.flowCancelFunc = context.WithCancel(cc.ctx)
	cc.flowId = flowId

	// get data channel from transport interface based on the provided ID
	dataChannel, err := cc.t.GetDataChannelEndpoint(dataChannelId)
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
	flowParallelWriter := NewParallelWriter(cc.flowCtx, cc, cc.settings.NumParallelWriters)
	flowParallelWriter.Start()

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-cc.flowCtx.Done():
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
			case <-cc.flowCtx.Done():
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
					cc.status.WriteLSN = max(dataMsg.SeqNum, cc.status.WriteLSN)
					err := flowParallelWriter.ScheduleDataMessage(dataMsg)
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to schedule data message: %v", err))
					}
				}
			}
		}

		slog.Info(fmt.Sprintf("Connector %s is done writing for flow %s", cc.id, flowId))
		err := cc.coord.NotifyDone(flowId, cc.id)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", cc.id, flowId, err))
		}

	}()
	return nil
}

func (cc *Connector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go cc.doIntegrityCheck_sync(flowId, options)
	return nil
}

func (cc *Connector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return cc.status
}

func (cc *Connector) Interrupt(flowId iface.FlowID) error {
	cc.flowCancelFunc()
	return nil
}

func (cc *Connector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	go func() {
		// Retrieve the latest resume token before we start reading anything
		// We will use the resume token to start the change stream
		cc.status.SyncState = iface.ReadPlanningSyncState
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
				resumeToken, err := cc.getLatestResumeToken(cc.ctx, loc)
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

		err = cc.coord.PostReadPlanningResult(flowId, cc.id, iface.ConnectorReadPlanResult{ReadPlan: plan, Success: true})
		if err != nil {
			slog.Error(fmt.Sprintf("Failed notifying coordinator about read planning done: %v", err))
		}
	}()
	return nil
}
