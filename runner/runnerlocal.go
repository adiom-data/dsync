package runner

import (
	"context"
	"log/slog"
	"sync"

	"github.com/adiom-data/dsync/connector"
	"github.com/adiom-data/dsync/coordinator"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/statestore"
	"github.com/adiom-data/dsync/transport"
)

// Implements the protocol.iface.Runner interface
// Supports two connectors (source and destination)
// Sets up all the components to run locally in a single binary

type RunnerLocal struct {
	trans      iface.Transport
	statestore iface.Statestore
	coord      iface.Coordinator
	src, dst   iface.Connector

	ctx context.Context
}

type RunnerLocalSettings struct {
	SrcConnString        string
	DstConnString        string
	StateStoreConnString string
}

const (
	sourceName      = "Source"
	destinationName = "Destination"
)

func NewRunnerLocal(settings RunnerLocalSettings) *RunnerLocal {
	r := &RunnerLocal{}
	r.src = connector.NewMongoConnector(sourceName, connector.MongoConnectorSettings{ConnectionString: settings.SrcConnString})
	r.dst = connector.NewMongoConnector(destinationName, connector.MongoConnectorSettings{ConnectionString: settings.DstConnString})
	r.statestore = statestore.NewMongoStateStore(statestore.MongoStateStoreSettings{ConnectionString: settings.StateStoreConnString})
	r.coord = coordinator.NewSimpleCoordinator()
	r.trans = transport.NewTransportLocal(r.coord)

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

func (r *RunnerLocal) Run() {
	slog.Debug("RunnerLocal Run")

	// Implement the run logic here

	// create waitgroup
	waitGroup := sync.WaitGroup{}
	// add the components to the waitgroup
	waitGroup.Add(3)
	// start the components
	go func() {
		defer waitGroup.Done()
		r.coord.Run()
	}()
	go func() {
		defer waitGroup.Done()
		r.src.Run()
	}()
	go func() {
		defer waitGroup.Done()
		r.dst.Run()
	}()

	// get available connectors from the coordinator
	connectors := r.coord.GetConnectors()
	// print the available connectors
	slog.Debug("Connectors: ", connectors)

	// find connector ids based on their descriptions
	var srcId, dstId iface.ConnectorID
	for _, connectorDetails := range connectors {
		if connectorDetails.Desc == sourceName {
			srcId = connectorDetails.Id
		} else if connectorDetails.Desc == destinationName {
			dstId = connectorDetails.Id
		}
	}

	if (srcId == iface.ConnectorID{}) {
		slog.Error("Source connector not found")
		return
	}
	if (dstId == iface.ConnectorID{}) {
		slog.Error("Source connector not found")
		return
	}

	// create a flow
	flowOptions := iface.FlowOptions{
		SrcId: srcId,
		DstId: dstId,
		Type:  iface.UnidirectionalFlowType,
	}
	flowID, err := r.coord.FlowCreate(flowOptions)
	if err != nil {
		slog.Error("Failed to create flow", err)
	} else {
		// start the flow
		r.coord.FlowStart(flowID)
		// stop the flow
		r.coord.FlowStop(flowID)
		// destroy the flow
		r.coord.FlowDestroy(flowID)
	}

	//wait for the components to finish
	waitGroup.Wait()
}

func (r *RunnerLocal) Teardown() {
	slog.Debug("RunnerLocal Teardown")

	r.coord.Teardown()
	r.src.Teardown()
	r.dst.Teardown()
	r.statestore.Teardown()
}