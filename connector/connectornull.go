package connector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/adiom-data/dsync/protocol/iface"
)

type NullWriteConnector struct {
	desc string
	ctx  context.Context

	t                     iface.Transport
	id                    iface.ConnectorID
	coord                 iface.CoordinatorIConnectorSignal
	connectorType         iface.ConnectorType
	connectorCapabilities iface.ConnectorCapabilities
	//TODO (AK, 6/2024): this should be per-flow (as well as the other bunch of things)
	// ducktaping for now
	status iface.ConnectorStatus
}

func NewNullConnector(desc string) *NullWriteConnector {
	return &NullWriteConnector{
		desc: desc,
	}
}

func (nc *NullWriteConnector) Setup(ctx context.Context, t iface.Transport) error {
	nc.ctx = ctx
	nc.t = t
	// Instantiate ConnectorType
	nc.connectorType = iface.ConnectorType{DbType: "/dev/null"}
	// Instantiate ConnectorCapabilities
	nc.connectorCapabilities = iface.ConnectorCapabilities{Source: false, Sink: true}
	//Instantiate ConnectorStatus
	nc.status = iface.ConnectorStatus{WriteLSN: 0}
	// Get the coordinator endpoint
	coord, err := nc.t.GetCoordinatorEndpoint("local")
	if err != nil {
		return errors.New("Failed to get coordinator endpoint: " + err.Error())
	}
	nc.coord = coord

	// Create a new connector details structure
	connectorDetails := iface.ConnectorDetails{Desc: nc.desc, Type: nc.connectorType, Cap: nc.connectorCapabilities}
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
	slog.Info(fmt.Sprintf("Null Write Connector %s is completed", nc.id))
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
	dataChannel, err := nc.t.GetDataChannelEndpoint(dataChannelId)
	if err != nil {
		slog.Error(fmt.Sprintf("Failed to get data channel by ID: %v", err))
		return err
	}

	type WriterProgress struct {
		dataMessages uint
	}

	writerProgress := WriterProgress{
		dataMessages: 0, //XXX (AK, 6/2024): should we handle overflow? Also, should we use atomic types?
	}

	// start printing progress
	go func() {
		ticker := time.NewTicker(progressReportingIntervalSec * time.Second)
		defer ticker.Stop()
		for {

			select {
			case <-nc.ctx.Done():
				return
			case <-ticker.C:
				// Print writer progress
				slog.Debug(fmt.Sprintf("Writer Progress: Data Messages - %d", writerProgress.dataMessages))
			}
		}
	}()
	for loop := true; loop; {
		select {
		case <-nc.ctx.Done():
			loop = false
		case dataMsg, ok := <-dataChannel:
			if !ok {
				// channel is closed which is a signal for us to stop
				loop = false
				break
			}
			// Process the data message
			writerProgress.dataMessages++
			nc.status.WriteLSN = max(dataMsg.SeqNum, nc.status.WriteLSN)
		}
	}
	return nil
}

func (nc *NullWriteConnector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions) error {
	//does nothing, no data to check
	return nil
}
func (nc *NullWriteConnector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return nc.status
}
