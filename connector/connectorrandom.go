package connector

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
	"github.com/brianvoe/gofakeit/v7"
)

type NullReadConnector struct {
	desc string

	settings RandomConnectorSettings
	ctx      context.Context

	t  iface.Transport
	id iface.ConnectorID

	connectorType         iface.ConnectorType
	connectorCapabilities iface.ConnectorCapabilities

	coord iface.CoordinatorIConnectorSignal

	//TODO (AK, 6/2024): this should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	status iface.ConnectorStatus
}

type RandomConnectorSettings struct {
	initialSyncNumParallelCopiers int
	writerMaxBatchSize            int //0 means no limit (in # of documents)
	numParallelWriters            int

	numDatabases                     int
	numCollectionsPerDatabase        int
	numInitialDocumentsPerCollection int
	numFields                        int
	docSize                          uint
}

func NewNullReadConnector(desc string, settings RandomConnectorSettings) *NullReadConnector {
	// Set default values

	settings.initialSyncNumParallelCopiers = 4
	settings.writerMaxBatchSize = 0
	settings.numParallelWriters = 4
	settings.numDatabases = 1
	settings.numCollectionsPerDatabase = 1
	settings.numInitialDocumentsPerCollection = 1000
	settings.numFields = 10

	return &NullReadConnector{desc: desc, settings: settings}
}

func (rc *NullReadConnector) Setup(ctx context.Context, t iface.Transport) error {
	//setup the connector
	rc.ctx = ctx
	rc.t = t

	// Instantiate ConnectorType
	rc.connectorType = iface.ConnectorType{DbType: "/dev/random"}
	// Instantiate ConnectorCapabilities
	rc.connectorCapabilities = iface.ConnectorCapabilities{Source: true, Sink: false}
	// Instantiate ConnectorStatus
	rc.status = iface.ConnectorStatus{WriteLSN: 0}

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

	slog.Info("NullReadConnector has been configured with ID " + rc.id.ID)

	return nil
}

func (rc *NullReadConnector) Teardown() {
	//does nothing, no client to disconnect
}

func (rc *NullReadConnector) SetParameters(reqCap iface.ConnectorCapabilities) {
	//not necessary always source
}

func (rc *NullReadConnector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, dataChannelId iface.DataChannelID) error {
	tasks := rc.CreateInitialGenerationTasks()

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
			case <-rc.ctx.Done():
				return
			case <-ticker.C:
				elapsedTime := time.Since(startTime).Seconds()
				operations_delta := readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents - operations
				opsPerSec := math.Floor(float64(operations_delta) / elapsedTime)
				// Print reader progress
				slog.Info(fmt.Sprintf("Random Reader Progress: Initial Docs Generation - %d (%d/%d tasks completed), Change Stream Events - %d, Operations per Second - %.2f",
					readerProgress.initialSyncDocs.Load(), readerProgress.tasksCompleted, readerProgress.tasksTotal, readerProgress.changeStreamEvents, opsPerSec))

				startTime = time.Now()
				operations = readerProgress.initialSyncDocs.Load() + readerProgress.changeStreamEvents
			}
		}
	}()

	// lsn tracking

	// Start the initial generation

	go func() {
		defer close(initialGenerationDone)

		slog.Info(fmt.Sprintf("Null Read Connector %s is starting initial data generation for flow %s", mc.id, flowId))
		//create a channel to distribute tasks to copiers
		taskChannel := make(chan DataCopyTask)
		//create a wait group to wait for all copiers to finish
		var wg sync.WaitGroup
		wg.Add(rc.settings.initialSyncNumParallelCopiers)

		for i := 0; i < rc.settings.initialSyncNumParallelCopiers; i++ {
			go func() {
				defer wg.Done()
				for task := range taskChannel {
					slog.Debug(fmt.Sprintf("Generating task: %v", task))
					db := task.Db
					col := task.Col
					loc := iface.Location{Database: db, Collection: col}
					//need to generate random documents

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

	// Wait for initial generation to finish
	go func() {
		<-initialGenerationDone

		close(dataChannel) //send a signal downstream that we are done sending data //TODO (AK, 6/2024): is this the right way to do it?

		slog.Info(fmt.Sprintf("Null Read Connector %s is done generating data for flow %s", rc.id, flowId))
		err := rc.coord.NotifyDone(flowId, rc.id) //TODO (AK, 6/2024): Should we also pass an error to the coord notification if applicable?
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done reading for flow %s: %v", rc.id, flowId, err))
		}
	}()
	return nil
}

func (rc *NullReadConnector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	//never writes to destination, errors
	return errors.New("NullReadConnector does not write to destination")
}

func (rc *NullReadConnector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions) error {
	//no client, errors
	return errors.New("NullReadConnector does not have a client to request data integrity check")
}

func (rc *NullReadConnector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	//get connector status
	return rc.status
}

func (rc *NullReadConnector) CreateInitialGenerationTasks() []DataCopyTask {
	var tasks []DataCopyTask

	for i := 1; i <= rc.settings.numDatabases; i++ {
		for j := 1; j <= rc.settings.numCollectionsPerDatabase; j++ {
			task := DataCopyTask{
				Db:  fmt.Sprintf("db%d", i),
				Col: fmt.Sprintf("col%d", j),
			}
			tasks = append(tasks, task)
		}
	}
	return tasks
}

func generateRandomDocument(numFields int, docSize uint) map[string]interface{} {
	doc := make(map[string]interface{})
	for i := 1; i <= numFields; i++ {
		doc[fmt.Sprintf("field%d", i)] = gofakeit.LetterN(docSize)
	}
	return doc
}

func generateDataMessage(loc iface.Location, doc map[string]interface{}) iface.DataMessage {
	return iface.DataMessage{
		Loc: loc,
		//Data: doc,
		MutationType: iface.MutationType_Insert,
	}
}

func generateDataMessageBatch(loc iface.Location, docs []map[string]interface{}) iface.DataMessage {
	return iface.DataMessage{
		Loc: loc,
		//DataBatch: docs,
		MutationType: iface.MutationType_InsertBatch,
	}
}
