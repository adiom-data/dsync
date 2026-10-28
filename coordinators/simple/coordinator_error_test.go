/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package simple

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/common"
	nullconn "github.com/adiom-data/dsync/connectors/null"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/statestores/temp"
	transportlocal "github.com/adiom-data/dsync/transports/local"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestWaitForFlowDoneReturnsConnectorError(t *testing.T) {
	coordinator := NewSimpleCoordinator(1)
	coordinator.ctx, coordinator.flowCancel = context.WithCancel(context.Background())
	flowID := iface.FlowID("flow")
	flow := &FlowDetails{FlowID: flowID, flowDone: make(chan struct{})}
	coordinator.addFlow(flow)

	streamErr := errors.New("change stream failed")
	coordinator.NotifyError(streamErr)
	close(flow.flowDone)

	require.ErrorIs(t, coordinator.WaitForFlowDone(flowID), streamErr)
}

type streamFailureSource struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	initialWritten <-chan struct{}
	document       []byte
}

func (s streamFailureSource) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id: "source", Capabilities: &adiomv1.Capabilities{Source: &adiomv1.Capabilities_Source{}},
	}), nil
}

func (s streamFailureSource) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{Count: 1}), nil
}

func (s streamFailureSource) GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        []*adiomv1.Partition{{Namespace: "db.col", EstimatedCount: 1}},
		UpdatesPartitions: []*adiomv1.UpdatesPartition{{Cursor: []byte("cursor")}},
	}), nil
}

func (s streamFailureSource) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	return connect.NewResponse(&adiomv1.ListDataResponse{Data: [][]byte{s.document}}), nil
}

func (s streamFailureSource) StreamUpdates(ctx context.Context, _ *connect.Request[adiomv1.StreamUpdatesRequest], _ *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	select {
	case <-s.initialWritten:
		return connect.NewError(connect.CodeInternal, errors.New("source change stream failed"))
	case <-ctx.Done():
		return ctx.Err()
	}
}

type recordingSink struct {
	adiomv1connect.ConnectorServiceHandler
	written chan struct{}
}

func (s recordingSink) WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	close(s.written)
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

func TestStandaloneFlowFailsAfterInitialCopyWhenStreamFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	coordinator := NewSimpleCoordinator(1)
	transport := transportlocal.NewTransportLocal(coordinator)
	coordinator.Setup(ctx, transport, temp.NewTempStateStore())

	initialWritten := make(chan struct{})
	document, err := bson.Marshal(bson.D{{Key: "_id", Value: 1}})
	require.NoError(t, err)
	settings := common.ConnectorSettings{
		NumParallelCopiers: 1, NumParallelWriters: 1, MaxWriterBatchSize: 1,
		SourceDataType:      adiomv1.DataType_DATA_TYPE_MONGO_BSON,
		DestinationDataType: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}
	source := common.NewLocalConnector("source", streamFailureSource{initialWritten: initialWritten, document: document}, settings)
	sink := common.NewLocalConnector("sink", recordingSink{
		ConnectorServiceHandler: nullconn.NewConn("sink", false, 0, 0), written: initialWritten,
	}, settings)
	t.Cleanup(source.Teardown)
	t.Cleanup(sink.Teardown)
	require.NoError(t, source.Setup(ctx, transport))
	require.NoError(t, sink.Setup(ctx, transport))

	var sourceID, sinkID iface.ConnectorID
	for _, connector := range coordinator.GetConnectors() {
		if connector.Cap.Source {
			sourceID = connector.Id
		} else {
			sinkID = connector.Id
		}
	}
	flowID, err := coordinator.FlowGetOrCreate(iface.FlowOptions{
		Type: iface.UnidirectionalFlowType, SrcId: sourceID, DstId: sinkID,
	})
	require.NoError(t, err)
	require.NoError(t, coordinator.FlowStart(flowID))

	// The real reader and coordinator must produce the final error without
	// manually signaling completion. LSN is unimplemented and remains optional.
	err = coordinator.WaitForFlowDone(flowID)
	require.ErrorContains(t, err, "source change stream failed")
	select {
	case <-initialWritten:
	default:
		t.Fatal("initial copy did not reach the sink")
	}
}
