/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/cespare/xxhash"
	"go.akshayshah.org/memhttp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const progressReportingIntervalSec = 10

type Teardownable interface {
	Teardown()
}

type ConnectorSettings struct {
	NumParallelCopiers        int
	NumParallelWriters        int
	MaxWriterBatchSize        int
	ResumeTokenUpdateInterval time.Duration
}

type maybeOptimizedConnectorService interface {
	ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error)
	WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error)
	WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error)
}

type connector struct {
	impl               adiomv1connect.ConnectorServiceClient
	maybeOptimizedImpl maybeOptimizedConnectorService

	desc string

	id             iface.ConnectorID
	t              iface.Transport
	coord          iface.CoordinatorIConnectorSignal
	ctx            context.Context
	flowID         iface.FlowID
	flowCtx        context.Context
	flowCancelFunc context.CancelFunc
	status         iface.ConnectorStatus

	settings         ConnectorSettings
	resumable        bool
	resumeToken      []byte
	resumeTokenMutex sync.RWMutex

	progressTracker *ProgressTracker

	namespaceMappings map[string]string
}

// GetConnectorStatus implements iface.Connector.
func (c *connector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return c.status
}

func HashBson(hasher hash.Hash64, b bson.Raw) error {
	elems, err := b.Elements()
	if err != nil {
		return err
	}
	slices.SortFunc(elems, func(i, j bson.RawElement) int {
		return strings.Compare(i.Key(), j.Key())
	})
	for _, e := range elems {
		hasher.Write([]byte(e.Key()))
		v := e.Value()
		hasher.Write([]byte{byte(v.Type)})
		if v.Type == bson.TypeEmbeddedDocument {
			hasher.Write([]byte(e.Key()))
			if err := HashBson(hasher, v.Document()); err != nil {
				return err
			}
		} else {
			hasher.Write(e)
		}
	}
	return err
}

// IntegrityCheck implements iface.Connector.
func (c *connector) IntegrityCheck(ctx context.Context, task iface.IntegrityCheckQuery) (iface.ConnectorDataIntegrityCheckResult, error) {
	if task.CountOnly {
		res, err := c.impl.GetNamespaceMetadata(ctx, connect.NewRequest(&adiomv1.GetNamespaceMetadataRequest{Namespace: task.Namespace}))
		if err != nil {
			return iface.ConnectorDataIntegrityCheckResult{}, err
		}
		return iface.ConnectorDataIntegrityCheckResult{
			Count: int64(res.Msg.GetCount()),
		}, nil
	}

	hasher := xxhash.New()
	var hash uint64
	var count int64
	var cursor []byte

	var pCursor []byte
	if task.Low != nil {
		pCursor = task.Low.(primitive.Binary).Data
	}
	namespace := task.Namespace
	for {
		res, err := c.maybeOptimizedImpl.ListData(ctx, connect.NewRequest(&adiomv1.ListDataRequest{
			Partition: &adiomv1.Partition{
				Namespace: namespace,
				Cursor:    pCursor,
			},
			Type:   adiomv1.DataType_DATA_TYPE_MONGO_BSON,
			Cursor: cursor,
		}))
		if err != nil {
			if !errors.Is(ctx.Err(), context.Canceled) {
				slog.Error(fmt.Sprintf("Failed to fetch documents for integrity check: %v", err))
			}
			return iface.ConnectorDataIntegrityCheckResult{}, err
		}

		for _, data := range res.Msg.Data {
			hasher.Reset()
			err = HashBson(hasher, bson.Raw(data))
			if err != nil {
				slog.Error(fmt.Sprintf("Error hashing during integrity check: %v", err))
				return iface.ConnectorDataIntegrityCheckResult{}, err
			}
			hash ^= hasher.Sum64()
			count += 1
		}

		nextCursor := res.Msg.GetNextCursor()
		if len(nextCursor) == 0 {
			break
		}
		if bytes.Equal(cursor, nextCursor) {
			slog.Error("Cursor and next cursor are non empty and the same")
			break
		}
		cursor = nextCursor
	}
	return iface.ConnectorDataIntegrityCheckResult{
		XXHash: hash,
		Count:  count,
	}, nil
}

// Interrupt implements iface.Connector.
func (c *connector) Interrupt(flowId iface.FlowID) error {
	if c.flowCancelFunc != nil {
		c.flowCancelFunc()
	}
	return nil
}

func (c *connector) mapNamespace(namespace string) string {
	if res, ok := c.namespaceMappings[namespace]; ok {
		return res
	}
	if left, right, ok := strings.Cut(namespace, "."); ok {
		if res, ok := c.namespaceMappings[left]; ok {
			return res + "." + right
		}
	}
	return namespace
}

func (c *connector) parseNamespaceOptionAndUpdateMap(namespaces []string) ([]string, []string) {
	var left []string
	var right []string
	for _, namespace := range namespaces {
		if l, r, ok := strings.Cut(namespace, ":"); ok {
			left = append(left, l)
			right = append(right, r)
			c.namespaceMappings[l] = r
		} else {
			left = append(left, namespace)
			right = append(right, namespace)
		}
	}
	return left, right
}

// RequestCreateReadPlan implements iface.Connector.
func (c *connector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	// Retrieve the latest resume token before we start reading anything
	// We will use the resume token to start the change stream
	c.status.SyncState = iface.ReadPlanningSyncState
	namespaces, _ := c.parseNamespaceOptionAndUpdateMap(options.Namespace)
	resp, err := c.impl.GeneratePlan(c.ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces:  namespaces,
		InitialSync: options.Mode != iface.SyncModeCDC,
		Updates:     true,
	}))
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to generate plan: %v", err))
		return c.coord.PostReadPlanningResult(flowId, c.id, iface.ConnectorReadPlanResult{})
	}

	c.resumeToken = resp.Msg.GetUpdatesPartitions()[0].GetCursor()

	curID := 0
	var tasks []iface.ReadPlanTask
	if options.Mode == iface.SyncModeCDC {
		tasks = nil
	} else {
		for _, partition := range resp.Msg.GetPartitions() {
			curID++
			task := iface.ReadPlanTask{
				Id: iface.ReadPlanTaskID(curID),
			}
			task.Def.Col = partition.GetNamespace()
			task.Def.Low = primitive.Binary{Data: partition.GetCursor()}
			task.EstimatedDocCount = int64(partition.GetEstimatedCount())
			tasks = append(tasks, task)
		}
	}
	plan := iface.ConnectorReadPlan{Tasks: tasks, CdcResumeToken: c.resumeToken}

	err = c.coord.PostReadPlanningResult(flowId, c.id, iface.ConnectorReadPlanResult{ReadPlan: plan, Success: true})
	if err != nil {
		slog.Error(fmt.Sprintf("Failed notifying coordinator about read planning done: %v", err))
		return err
	}
	return nil
}

// SetParameters implements iface.Connector.
func (c *connector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	c.resumable = reqCap.Resumability
	slog.Debug(fmt.Sprintf("Connector %s set capabilities for flow %s: %+v", c.id, flowId, reqCap))
}

// Setup implements iface.Connector.
func (c *connector) Setup(ctx context.Context, t iface.Transport) error {
	c.ctx = ctx
	c.t = t

	c.status = iface.ConnectorStatus{WriteLSN: 0}
	c.progressTracker = NewProgressTracker(&c.status, c.ctx)

	coord, err := c.t.GetCoordinatorEndpoint("local")
	if err != nil {
		return err
	}
	c.coord = coord

	res, err := c.impl.GetInfo(ctx, connect.NewRequest(&adiomv1.GetInfoRequest{}))
	if err != nil {
		return err
	}

	capabilities := res.Msg.GetCapabilities()

	connectorType := iface.ConnectorType{
		DbType:  res.Msg.GetDbType(),
		Version: res.Msg.GetVersion(),
		Spec:    res.Msg.GetSpec(),
	}
	connectorCapabilities := iface.ConnectorCapabilities{
		Source:         capabilities.GetSource() != nil,
		Sink:           capabilities.GetSink() != nil,
		IntegrityCheck: capabilities.GetSource() != nil,
		Resumability:   true,
	}

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{
		Desc: c.desc,
		Type: connectorType,
		Cap:  connectorCapabilities,
		Id:   iface.ConnectorID(res.Msg.GetId()),
	}
	// Register the connector
	c.id, err = coord.RegisterConnector(connectorDetails, c)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	return nil
}

// StartReadToChannel implements iface.Connector.
func (c *connector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelID iface.DataChannelID) error {
	// We'll do namespace mappings on the read side for now, so ensure we have the mapping ready
	_, _ = c.parseNamespaceOptionAndUpdateMap(options.Namespace)

	c.flowCtx, c.flowCancelFunc = context.WithCancel(c.ctx)
	c.flowID = flowId

	tasks := readPlan.Tasks
	slog.Info(fmt.Sprintf("number of tasks: %d", len(tasks)))

	// If everything, then don't pass any namespaces
	// Otherwise, we want only unique fully qualified namespaces (which currently is fully contained in task.Def.Col)
	var streamUpdatesNamespaces []string
	streamUpdatesNamespacesMap := map[string]struct{}{}
	if len(options.Namespace) > 0 {
		for _, task := range readPlan.Tasks {
			streamUpdatesNamespacesMap[task.Def.Col] = struct{}{}
		}
		for k := range streamUpdatesNamespacesMap {
			streamUpdatesNamespaces = append(streamUpdatesNamespaces, k)
		}
	}

	// reset doc counts for all namespaces to actual for more accurate progress reporting
	progressResetChan := make(chan struct{})
	c.progressTracker.RestoreProgressDetails(tasks)
	go func() {
		defer close(progressResetChan)
		c.progressTracker.ResetNsProgressEstimatedDocCounts(func(ctx context.Context, ns iface.Namespace) (int64, error) {
			res, err := c.impl.GetNamespaceMetadata(ctx, connect.NewRequest(&adiomv1.GetNamespaceMetadataRequest{Namespace: ns.Col}))
			if err != nil {
				return 0, err
			}
			return int64(res.Msg.GetCount()), nil
		})
	}()

	if len(tasks) == 0 && options.Mode != iface.SyncModeCDC {
		return errors.New("no tasks to copy")
	}
	c.resumeToken = readPlan.CdcResumeToken
	initialResumeToken := c.resumeToken

	slog.Debug(fmt.Sprintf("StartReadToChannel Tasks: %+v", tasks))

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := c.t.GetDataChannelEndpoint(dataChannelID)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	lsnDone := make(chan struct{})

	// Declare two channels to wait for the change stream reader and the initial sync to finish
	initialSyncDone := make(chan struct{})
	changeStreamDone := make(chan struct{})

	type ReaderProgress struct {
		initialSyncDocs    atomic.Uint64
		changeStreamEvents atomic.Uint64
		tasksTotal         uint64
		tasksCompleted     atomic.Uint64
	}

	readerProgress := ReaderProgress{ //XXX (AK, 6/2024): should we handle overflow? Also, should we use atomic types?
		tasksTotal: uint64(len(tasks)),
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
			case <-c.flowCtx.Done():
				return
			case <-ticker.C:
				elapsedTime := time.Since(startTime).Seconds()
				operations_delta := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents.Load() - operations
				opsPerSec := math.Floor(float64(operations_delta) / elapsedTime)
				// Print reader progress
				slog.Info(fmt.Sprintf("Reader Progress: Initial Sync Docs - %d (%d/%d tasks completed), Change Stream Events - %d, Operations per Second - %.2f",
					readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted.Load(), readerProgress.tasksTotal, readerProgress.changeStreamEvents.Load(), opsPerSec))

				startTime = time.Now()
				operations = readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents.Load()
			}
		}
	}()

	// kick off LSN tracking
	// TODO (AK, 6/2024): implement this proper - this is a very BAD, bad placeholder.
	go func() {
		defer close(lsnDone)
		slog.Info(fmt.Sprintf("Connector %s is starting to track LSN for flow %s", c.id, flowId))
		res, err := c.impl.StreamLSN(c.flowCtx, connect.NewRequest(&adiomv1.StreamLSNRequest{
			Namespaces: streamUpdatesNamespaces,
			Cursor:     initialResumeToken,
		}))
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error(fmt.Sprintf("LSN tracker: Failed to open change stream: %v", err))
			}
			return
		}
		defer res.Close()
		for res.Receive() {
			c.status.WriteLSN = int64(res.Msg().Lsn)
		}
		if res.Err() != nil {
			if !errors.Is(res.Err(), context.Canceled) {
				slog.Error(fmt.Sprintf("Failed during lsn stream: %v", res.Err()))
			}
		}
	}()

	// kick off the initial sync
	go func() {
		defer close(initialSyncDone)
		<-progressResetChan
		if len(tasks) == 0 {
			slog.Info(fmt.Sprintf("Connector %s is skipping initial sync for flow %s", c.id, flowId))
			return
		}

		slog.Info(fmt.Sprintf("Connector %s is starting initial sync for flow %s", c.id, flowId))
		c.status.SyncState = iface.InitialSyncSyncState

		//create a channel to distribute tasks to copiers
		taskChannel := make(chan iface.ReadPlanTask)
		//create a wait group to wait for all copiers to finish
		var wg sync.WaitGroup
		wg.Add(c.settings.NumParallelCopiers)

		for i := 0; i < c.settings.NumParallelCopiers; i++ {
			go func() {
				defer wg.Done()
			Loop:
				for task := range taskChannel {
					sourceNamespace := task.Def.Col
					destinationNamespace := c.mapNamespace(sourceNamespace)
					slog.Debug(fmt.Sprintf("Processing task: %v", task))
					var docs int64
					ns := iface.Namespace{Db: task.Def.Db, Col: task.Def.Col}
					c.progressTracker.TaskStartedProgressUpdate(ns, task.Id)

					var cursor []byte

					var pCursor []byte
					if task.Def.Low != nil {
						pCursor = task.Def.Low.(primitive.Binary).Data
					}
					for {
						res, err := c.maybeOptimizedImpl.ListData(c.flowCtx, connect.NewRequest(&adiomv1.ListDataRequest{
							Partition: &adiomv1.Partition{
								Namespace: sourceNamespace,
								Cursor:    pCursor,
							},
							Type:   adiomv1.DataType_DATA_TYPE_MONGO_BSON,
							Cursor: cursor,
						}))
						if err != nil {
							if !errors.Is(err, context.Canceled) {
								slog.Error(fmt.Sprintf("Error listing data: %v", err))
							}
							continue Loop
						}

						readerProgress.initialSyncDocs.Add(uint64(len(res.Msg.Data)))
						c.progressTracker.TaskInProgressUpdate(ns, int64(len(res.Msg.Data)))
						docs += int64(len(res.Msg.Data))

						dataMessage := iface.DataMessage{
							DataBatch:    res.Msg.Data,
							MutationType: iface.MutationType_InsertBatch,
							Loc:          destinationNamespace,
						}
						dataChannel <- dataMessage
						nextCursor := res.Msg.GetNextCursor()
						if len(nextCursor) == 0 {
							break
						}
						if bytes.Equal(cursor, nextCursor) {
							slog.Error("Cursor and next cursor are non empty and the same")
							continue Loop
						}
						cursor = nextCursor
					}

					readerProgress.tasksCompleted.Add(1)
					// update progress after completing the task and create task metadata to pass to coordinator to persist
					c.progressTracker.TaskDoneProgressUpdate(ns, task.Id)
					slog.Debug(fmt.Sprintf("Done processing task: %v", task))
					//notify the coordinator that the task is done from our side
					c.coord.NotifyTaskDone(flowId, c.id, task.Id, &iface.TaskDoneMeta{DocsCopied: docs})
					//send a barrier message to signal the end of the task
					if c.resumable { //send only if the flow supports resumability otherwise who knows what will happen on the recieving side
						dataChannel <- iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_TaskComplete, BarrierTaskId: (uint)(task.Id)}
					}
				}
			}()
		}

		//iterate over all the tasks and distribute them to copiers
		for _, task := range tasks {
			if task.Status == iface.ReadPlanTaskStatus_Completed {
				// the task is already completed, so we can just skip it
				readerProgress.tasksCompleted.Add(1)
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

	// kick off the change stream reader
	go func() {
		//wait for the initial sync to finish
		<-initialSyncDone
		c.status.SyncState = iface.ChangeStreamSyncState
		defer close(changeStreamDone)

		go func() {
			ticker := time.NewTicker(c.settings.ResumeTokenUpdateInterval)
			defer ticker.Stop()
			for {
				select {
				case <-c.flowCtx.Done():
					return
				case <-changeStreamDone:
					return
				case <-ticker.C:
					c.resumeTokenMutex.RLock()
					resumeToken := c.resumeToken
					c.resumeTokenMutex.RUnlock()
					// send a barrier message with the updated resume token
					dataChannel <- iface.DataMessage{MutationType: iface.MutationType_Barrier, BarrierType: iface.BarrierType_CdcResumeTokenUpdate, BarrierCdcResumeToken: resumeToken}
				}
			}
		}()

		var lsn int64

		slog.Info(fmt.Sprintf("Connector %s is starting to read change stream for flow %s", c.id, flowId))
		slog.Debug(fmt.Sprintf("Connector %s change stream start@ %v", c.id, initialResumeToken))

		res, err := c.impl.StreamUpdates(c.flowCtx, connect.NewRequest(&adiomv1.StreamUpdatesRequest{
			Namespaces: streamUpdatesNamespaces,
			Type:       adiomv1.DataType_DATA_TYPE_MONGO_BSON,
			Cursor:     initialResumeToken,
		}))
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error(fmt.Sprintf("Failed to stream updates: %v", err))
			}
			return
		}
		c.status.CDCActive = true

		for res.Receive() {
			msg := res.Msg()
			for _, d := range msg.Updates {
				var mutationType uint = iface.MutationType_Reserved
				switch d.Type {
				case adiomv1.UpdateType_UPDATE_TYPE_INSERT:
					mutationType = iface.MutationType_Insert
				case adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
					mutationType = iface.MutationType_Update
				case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
					mutationType = iface.MutationType_Delete
				}
				id := d.GetId()[0].GetData()
				c.progressTracker.UpdateChangeStreamProgressTracking()
				readerProgress.changeStreamEvents.Add(1)
				lsn++

				destinationNamespace := c.mapNamespace(msg.GetNamespace())
				dataChannel <- iface.DataMessage{
					Data:         &d.Data,
					MutationType: mutationType,
					Loc:          destinationNamespace,
					Id:           &id,
					IdType:       byte(d.GetId()[0].GetType()),
					SeqNum:       lsn,
				}
			}

			// update the last seen resume token
			c.resumeTokenMutex.Lock()
			c.resumeToken = msg.GetNextCursor()
			c.resumeTokenMutex.Unlock()
		}
		if res.Err() != nil {
			if !errors.Is(res.Err(), context.Canceled) {
				slog.Error(fmt.Sprintf("Failed during stream updates: %v", res.Err()))
			}
		}
	}()

	// wait for both the change stream reader and the initial sync to finish
	go func() {
		<-initialSyncDone
		<-changeStreamDone
		<-lsnDone

		close(dataChannel) //send a signal downstream that we are done sending data //TODO (AK, 6/2024): is this the right way to do it?

		slog.Info(fmt.Sprintf("Connector %s is done reading for flow %s", c.id, flowId))
		err := c.coord.NotifyDone(flowId, c.id) //TODO (AK, 6/2024): Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", c.id, flowId, err))
		}
	}()

	return nil
}

// StartWriteFromChannel implements iface.Connector.
func (c *connector) StartWriteFromChannel(flowId iface.FlowID, dataChannelID iface.DataChannelID) error {
	c.flowCtx, c.flowCancelFunc = context.WithCancel(c.ctx)
	c.flowID = flowId

	// Get data channel from transport interface based on the provided ID
	dataChannel, err := c.t.GetDataChannelEndpoint(dataChannelID)
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
	flowParallelWriter := NewParallelWriter(c.flowCtx, c, c.settings.NumParallelWriters, c.settings.MaxWriterBatchSize)
	flowParallelWriter.Start()

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-c.flowCtx.Done():
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
			case <-c.flowCtx.Done():
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
		slog.Info(fmt.Sprintf("Connector %s is done writing for flow %s", c.id, flowId))
		err := c.coord.NotifyDone(flowId, c.id)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", c.id, flowId, err))
		}
	}()

	return nil
}

func (c *connector) HandleBarrierMessage(barrierMsg iface.DataMessage) error {
	switch barrierMsg.BarrierType {
	case iface.BarrierType_TaskComplete:
		// notify the coordinator that the task is done from our side
		if err := c.coord.NotifyTaskDone(c.flowID, c.id, (iface.ReadPlanTaskID)(barrierMsg.BarrierTaskId), nil); err != nil {
			return err
		}
		return nil
	case iface.BarrierType_CdcResumeTokenUpdate:
		// notify the coordinator that the task is done from our side
		c.coord.UpdateCDCResumeToken(c.flowID, c.id, barrierMsg.BarrierCdcResumeToken)
		if err := c.coord.UpdateCDCResumeToken(c.flowID, c.id, barrierMsg.BarrierCdcResumeToken); err != nil {
			return err
		}
		return nil
	}

	return nil
}

// Teardown implements iface.Connector.
func (c *connector) Teardown() {}

func (c *localConnector) Teardown() {
	_ = c.srv.Shutdown(context.Background())
	if teardownable, ok := c.impl.(Teardownable); ok {
		teardownable.Teardown()
	}
}

func (c *localConnector) Impl() adiomv1connect.ConnectorServiceHandler {
	return c.impl
}

type localConnector struct {
	*connector
	impl adiomv1connect.ConnectorServiceHandler
	srv  *memhttp.Server
}

func NewLocalConnector(desc string, impl adiomv1connect.ConnectorServiceHandler, settings ConnectorSettings) *localConnector {
	_, handler := adiomv1connect.NewConnectorServiceHandler(impl)
	srv, err := memhttp.New(handler)
	if err != nil {
		panic(err)
	}
	client := adiomv1connect.NewConnectorServiceClient(srv.Client(), srv.URL())
	return &localConnector{
		NewConnector(desc, client, impl, settings),
		impl,
		srv,
	}
}

func NewRemoteConnector(desc string, impl adiomv1connect.ConnectorServiceClient, settings ConnectorSettings) *connector {
	return NewConnector(desc, impl, impl, settings)
}

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

func NewConnector(desc string, impl adiomv1connect.ConnectorServiceClient, underlying maybeOptimizedConnectorService, settings ConnectorSettings) *connector {
	setDefault(&settings.NumParallelCopiers, 4)
	setDefault(&settings.NumParallelWriters, 4)
	setDefault(&settings.ResumeTokenUpdateInterval, 60*time.Second)
	setDefault(&settings.MaxWriterBatchSize, 0)

	maybeOptimizedConnectorService := underlying
	if maybeOptimizedConnectorService == nil {
		underlying = impl
	}
	return &connector{
		desc:               desc,
		impl:               impl,
		maybeOptimizedImpl: maybeOptimizedConnectorService,
		settings:           settings,
		namespaceMappings:  map[string]string{},
	}
}

// ProcesDataMessages assumes all have the same namespace
func (c *connector) ProcessDataMessages(dataMsgs []iface.DataMessage) error {
	var msgs []*adiomv1.Update
	for i, dataMsg := range dataMsgs {
		switch dataMsg.MutationType {
		case iface.MutationType_InsertBatch:
			if len(msgs) > 0 {
				ns := dataMsg.Loc
				_, err := c.maybeOptimizedImpl.WriteUpdates(c.flowCtx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
					Namespace: ns,
					Updates:   msgs,
					Type:      adiomv1.DataType_DATA_TYPE_MONGO_BSON, // TODO
				}))
				if err != nil {
					return err
				}
				c.status.WriteLSN = max(c.status.WriteLSN, dataMsgs[i-1].SeqNum)
				msgs = nil
			}
			_, err := c.maybeOptimizedImpl.WriteData(c.flowCtx, connect.NewRequest(&adiomv1.WriteDataRequest{
				Namespace: dataMsg.Loc,
				Data:      dataMsg.DataBatch,
				Type:      adiomv1.DataType_DATA_TYPE_MONGO_BSON, // TODO
			}))
			if err != nil {
				return err
			}
			c.status.WriteLSN = max(c.status.WriteLSN, dataMsg.SeqNum)
		case iface.MutationType_Insert:
			msgs = append(msgs, &adiomv1.Update{
				Id:   []*adiomv1.BsonValue{{Data: *dataMsg.Id, Type: uint32(dataMsg.IdType)}},
				Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
				Data: *dataMsg.Data,
			})
		case iface.MutationType_Update:
			msgs = append(msgs, &adiomv1.Update{
				Id:   []*adiomv1.BsonValue{{Data: *dataMsg.Id, Type: uint32(dataMsg.IdType)}},
				Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
				Data: *dataMsg.Data,
			})
		case iface.MutationType_Delete:
			msgs = append(msgs, &adiomv1.Update{
				Id:   []*adiomv1.BsonValue{{Data: *dataMsg.Id, Type: uint32(dataMsg.IdType)}},
				Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
			})
		default:
			slog.Error(fmt.Sprintf("unsupported operation type during batch: %v", dataMsg.MutationType))
		}
	}
	if len(msgs) > 0 {
		ns := dataMsgs[0].Loc
		_, err := c.impl.WriteUpdates(c.flowCtx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
			Namespace: ns,
			Updates:   msgs,
			Type:      adiomv1.DataType_DATA_TYPE_MONGO_BSON, // TODO
		}))
		if err != nil {
			return err
		}
		c.status.WriteLSN = max(c.status.WriteLSN, dataMsgs[len(dataMsgs)-1].SeqNum)
	}
	return nil
}
