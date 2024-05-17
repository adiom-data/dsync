package runner

import (
	"context"
	"fmt"
	"log/slog"

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
	settings RunnerLocalSettings

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

	NsFromString []string
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

func (r *RunnerLocal) Run() {
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
		SrcId:               srcId,
		DstId:               dstId,
		Type:                iface.UnidirectionalFlowType,
		SrcConnectorOptions: iface.ConnectorOptions{Namespace: r.settings.NsFromString},
	}
	flowID, err := r.coord.FlowCreate(flowOptions)
	if err != nil {
		slog.Error("Failed to create flow", err)
		return
	}
	// start the flow
	err = r.coord.FlowStart(flowID)
	if err != nil {
		slog.Error("Failed to start flow", err)
	} else {
		// wait for the flow to finish
		r.coord.WaitForFlowDone(flowID)
	}
	// destroy the flow
	r.coord.FlowDestroy(flowID)
}

func (r *RunnerLocal) Teardown() {
	slog.Debug("RunnerLocal Teardown")

	r.coord.Teardown()
	r.src.Teardown()
	r.dst.Teardown()
	r.statestore.Teardown()
}
