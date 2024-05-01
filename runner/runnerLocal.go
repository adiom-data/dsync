package runner

import (
	"context"
	"log/slog"
	"time"

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
}

func (r *RunnerLocal) Setup() {
	slog.Debug("RunnerLocal Setup")

	//Create objects
	r.trans = &transport.TransportLocal{}
	r.coord = &coordinator.SimpleCoordinator{}
	r.statestore = &statestore.MongoStateStore{}
	r.src = &connector.MongoConnector{}
	r.dst = &connector.MongoConnector{}

	//Initialize in sequence
	r.statestore.Setup()
	r.coord.Setup(r.trans, r.statestore)
	r.src.Setup(r.trans)
	r.dst.Setup(r.trans)
}

func (r *RunnerLocal) Run(ctx context.Context) {
	slog.Debug("RunnerLocal Run")

	// Implement the run logic here
	sleep, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	<-sleep.Done()
}

func (r *RunnerLocal) Teardown() {
	slog.Debug("RunnerLocal Teardown")

	r.coord.Teardown()
	r.src.Teardown()
	r.dst.Teardown()
	r.statestore.Teardown()
}
