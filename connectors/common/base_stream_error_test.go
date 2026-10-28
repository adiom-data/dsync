/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/protocol/iface"
	"github.com/adiom-data/dsync/protocol/iface/mocks"
	transportlocal "github.com/adiom-data/dsync/transports/local"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type failingUpdateStreamHandler struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	err error
}

func (h failingUpdateStreamHandler) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:           "failing-source",
		Capabilities: &adiomv1.Capabilities{Source: &adiomv1.Capabilities_Source{}},
	}), nil
}

func (h failingUpdateStreamHandler) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{}), nil
}

func (h failingUpdateStreamHandler) StreamLSN(ctx context.Context, _ *connect.Request[adiomv1.StreamLSNRequest], _ *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	<-ctx.Done()
	return ctx.Err()
}

func (h failingUpdateStreamHandler) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return h.err
}

func TestStreamUpdatesFailureNotifiesCoordinator(t *testing.T) {
	streamErr := errors.New("stream broke")
	coordinator := mocks.NewCoordinator(t)
	coordinator.On("RegisterConnector", mock.Anything, mock.Anything).Return(iface.ConnectorID("source"), nil)
	errReported := make(chan error, 1)
	coordinator.On("NotifyError", mock.Anything).Run(func(args mock.Arguments) {
		errReported <- args.Error(0)
	})
	coordinator.On("NotifyDone", iface.FlowID("flow"), iface.ConnectorID("source")).Return(nil).Maybe()

	transport := transportlocal.NewTransportLocal(coordinator)
	dataChannel, err := transport.CreateDataChannel()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	connector := NewLocalConnector("source", failingUpdateStreamHandler{err: streamErr}, ConnectorSettings{})
	t.Cleanup(connector.Teardown)
	require.NoError(t, connector.Setup(ctx, transport))
	require.NoError(t, connector.StartReadToChannel(
		iface.FlowID("flow"),
		iface.ConnectorOptions{Mode: iface.SyncModeCDC},
		iface.ConnectorReadPlan{},
		dataChannel,
	))

	select {
	case reported := <-errReported:
		require.ErrorContains(t, reported, streamErr.Error())
	case <-time.After(5 * time.Second):
		t.Fatal("stream failure was not reported to the coordinator")
	}
}

type gatedUpdateStreamHandler struct {
	failingUpdateStreamHandler
	release <-chan struct{}
}

func (h gatedUpdateStreamHandler) StreamUpdates(ctx context.Context, _ *connect.Request[adiomv1.StreamUpdatesRequest], _ *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	select {
	case <-h.release:
		return h.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h gatedUpdateStreamHandler) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

func TestReaderStopsBlockedCheckpointSenderBeforeClosingChannel(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "stream ends normally"
		var streamErr error
		if fail {
			name = "stream fails"
			streamErr = errors.New("stream broke while checkpoint was blocked")
		}
		t.Run(name, func(t *testing.T) {
			coordinator := mocks.NewCoordinator(t)
			coordinator.On("RegisterConnector", mock.Anything, mock.Anything).Return(iface.ConnectorID("source"), nil)
			if fail {
				coordinator.On("NotifyError", mock.Anything).Run(func(args mock.Arguments) {
					require.ErrorContains(t, args.Error(0), streamErr.Error())
				}).Once()
			}
			done := make(chan struct{})
			coordinator.On("NotifyDone", iface.FlowID("flow"), iface.ConnectorID("source")).Run(func(mock.Arguments) {
				close(done)
			}).Return(nil).Once()

			transport := transportlocal.NewTransportLocal(coordinator)
			channelID, err := transport.CreateDataChannel()
			require.NoError(t, err)
			dataChannel, err := transport.GetDataChannelEndpoint(channelID)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			release := make(chan struct{})
			connector := NewLocalConnector("source", gatedUpdateStreamHandler{
				failingUpdateStreamHandler: failingUpdateStreamHandler{err: streamErr},
				release:                    release,
			}, ConnectorSettings{ResumeTokenUpdateInterval: time.Millisecond})
			t.Cleanup(connector.Teardown)
			require.NoError(t, connector.Setup(ctx, transport))
			require.NoError(t, connector.StartReadToChannel("flow", iface.ConnectorOptions{Mode: iface.SyncModeCDC}, iface.ConnectorReadPlan{}, channelID))

			// Observe a checkpoint to establish that the ticker has started,
			// then leave the unbuffered channel unread across subsequent ticks.
			select {
			case checkpoint := <-dataChannel:
				require.Equal(t, uint(iface.BarrierType_CdcResumeTokenUpdate), checkpoint.BarrierType)
			case <-ctx.Done():
				t.Fatal("checkpoint ticker did not start")
			}
			time.Sleep(20 * time.Millisecond)
			close(release)

			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("reader did not finish with a blocked checkpoint send")
			}
			_, open := <-dataChannel
			require.False(t, open, "data channel must be closed after all senders exit")
		})
	}
}
