/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package null

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
	status         iface.ConnectorStatus
	flowctx        context.Context
	flowCancelFunc context.CancelFunc
}

const (
	connectorDBType              = "/dev/null"
	progressReportingIntervalSec = 10
)

func NewNullConnector(desc string) *NullWriteConnector {
	return &NullWriteConnector{
		desc: desc,
	}
}

func (nc *NullWriteConnector) Setup(ctx context.Context, t iface.Transport) error {
	nc.ctx = ctx
	nc.t = t
	// Instantiate ConnectorType
	nc.connectorType = iface.ConnectorType{DbType: connectorDBType}
	// Instantiate ConnectorCapabilities
	nc.connectorCapabilities = iface.ConnectorCapabilities{Source: false, Sink: true, IntegrityCheck: false}
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

	slog.Info("NullWriteConnector has been configured with ID " + (string)(nc.id))

	return nil
}

func (nc *NullWriteConnector) Teardown() {
	// does nothing, no server connections to close
	slog.Info(fmt.Sprintf("Null Write Connector %s is completed", nc.id))
}

func (nc *NullWriteConnector) SetParameters(flowId iface.FlowID, reqCap iface.ConnectorCapabilities) {
	// not necessary - Null write connector is always a destination connector and doesn't set parameters
}

func (nc *NullWriteConnector) StartReadToChannel(flowId iface.FlowID, options iface.ConnectorOptions, readPlan iface.ConnectorReadPlan, dataChannelId iface.DataChannelID) error {
	// does nothing, no read from channel
	return fmt.Errorf("null write connector does not support read from channel")
}

func (nc *NullWriteConnector) StartWriteFromChannel(flowId iface.FlowID, dataChannelId iface.DataChannelID) error {
	//write null to destination
	nc.flowctx, nc.flowCancelFunc = context.WithCancel(nc.ctx)
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
			case <-nc.flowctx.Done():
				return
			case <-ticker.C:
				// Print writer progress
				slog.Debug(fmt.Sprintf("Writer Progress: Data Messages - %d", writerProgress.dataMessages))
			}
		}
	}()

	go func() {
		for loop := true; loop; {
			select {
			case <-nc.flowctx.Done():
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
		err := nc.coord.NotifyDone(flowId, nc.id)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to notify coordinator that the connector %s is done writing for flow %s: %v", nc.id, flowId, err))
		}

	}()
	return nil
}

func (nc *NullWriteConnector) RequestDataIntegrityCheck(flowId iface.FlowID, options iface.ConnectorOptions) error {
	//does nothing, no data to check
	return fmt.Errorf("null write connector does not support data integrity check")
}
func (nc *NullWriteConnector) GetConnectorStatus(flowId iface.FlowID) iface.ConnectorStatus {
	return nc.status
}
func (nc *NullWriteConnector) Interrupt(flowId iface.FlowID) error {
	//TODO: Put code here
	nc.flowCancelFunc()
	return nil
}

func (nc *NullWriteConnector) RequestCreateReadPlan(flowId iface.FlowID, options iface.ConnectorOptions) error {
	return fmt.Errorf("null write connector does not make plans for reads")
}
