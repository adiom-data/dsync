package connector

import (
	"context"
	"errors"
	"log/slog"

	"github.com/adiom-data/dsync/protocol/iface"
)

type NullWriteConnector struct {
	desc             string
	ConnectionString string
	ctx              context.Context

	t     iface.Transport
	id    iface.ConnectorID
	coord iface.CoordinatorIConnectorSignal

	//TODO (AK, 6/2024): this should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	status iface.ConnectorStatus
}

func NewNullConnector(desc string, connectionString string) *NullWriteConnector {
	return &NullWriteConnector{
		desc:             desc,
		ConnectionString: connectionString,
	}
}

func (nc *NullWriteConnector) Setup(ctx context.Context, t iface.Transport) error {
	nc.ctx = ctx
	nc.t = t

	//Instantiate ConnectorStatus
	nc.status = iface.ConnectorStatus{WriteLSN: 0}
	// Get the coordinator endpoint
	coord, err := nc.t.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	nc.coord = coord

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: nc.desc}
	// Register the connector
	nc.id, err = coord.RegisterConnector(connectorDetails, nc)
	if err != nil {
		return errors.New("Failed registering the connector: " + err.Error())
	}

	slog.Info("NullWriteConnector has been configured with ID " + nc.id.ID)

	return nil
}

func (nc *NullWriteConnector) Teardown() {
	// does nothing, no server connections to close
}

func (nc *NullWriteConnector) SetParameters(reqCap iface.ConnectorCapabilities) {
	// not necessary always destination
}

func (nc *NullWriteConnector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, dataChannelId iface.DataChannelID) error {
	// does nothing, no read from channel
	return nil
}

func (nc *NullWriteConnector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	//write null to destination
	return nil
}

func (nc *NullWriteConnector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions) error {
	//does nothing, no data to check
	return nil
}
func (nc *NullWriteConnector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return nc.status
}
