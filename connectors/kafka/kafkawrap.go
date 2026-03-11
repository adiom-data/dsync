package kafka

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"google.golang.org/protobuf/proto"
)

type kafkaWrapUnderlying interface {
	GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error)
	GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error)
	WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error)
	WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error)
	GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error)
	ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error)
	GetByIds(context.Context, *connect.Request[adiomv1.GetByIdsRequest]) (*connect.Response[adiomv1.GetByIdsResponse], error)
}

type kafkaWrapConn struct {
	kafkaWrapUnderlying
	kafkaConn adiomv1connect.ConnectorServiceHandler
}

func NewKafkaWrapConn(k adiomv1connect.ConnectorServiceHandler, underlying kafkaWrapUnderlying) *kafkaWrapConn {
	return &kafkaWrapConn{
		kafkaWrapUnderlying: underlying,
		kafkaConn:           k,
	}
}

func (k *kafkaWrapConn) GetInfo(ctx context.Context, r *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	underlyingInfo, err := k.kafkaWrapUnderlying.GetInfo(ctx, r)
	if err != nil {
		return nil, err
	}
	kInfo, err := k.kafkaConn.GetInfo(ctx, r)
	if err != nil {
		return nil, err
	}
	var sourceSupported []adiomv1.DataType
	for _, s := range underlyingInfo.Msg.GetCapabilities().GetSource().GetSupportedDataTypes() {
		for _, s2 := range kInfo.Msg.GetCapabilities().GetSource().GetSupportedDataTypes() {
			if s == s2 {
				sourceSupported = append(sourceSupported, s)
			}
		}
	}
	if len(sourceSupported) == 0 {
		if underlyingInfo.Msg.GetCapabilities() != nil {
			underlyingInfo.Msg.GetCapabilities().Source = nil
		}
	} else if underlyingInfo.Msg.GetCapabilities().GetSource() != nil {
		underlyingInfo.Msg.GetCapabilities().GetSource().SupportedDataTypes = sourceSupported
	}
	return underlyingInfo, nil
}

func (k *kafkaWrapConn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	finalResp := &adiomv1.GeneratePlanResponse{}
	req := r.Msg
	if req.GetInitialSync() {
		r2 := proto.Clone(req).(*adiomv1.GeneratePlanRequest)
		r2.Updates = false
		resp, err := k.kafkaWrapUnderlying.GeneratePlan(ctx, connect.NewRequest(r2))
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err generating underlying initial sync plan: %w", err))
		}
		finalResp.Partitions = resp.Msg.GetPartitions()
	}
	if req.GetUpdates() {
		resp, err := k.kafkaConn.GeneratePlan(ctx, r)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err generating kafka plan: %w", err))
		}
		finalResp.UpdatesPartitions = resp.Msg.GetUpdatesPartitions()
	}
	return connect.NewResponse(finalResp), nil
}

func (k *kafkaWrapConn) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return k.kafkaConn.StreamLSN(ctx, r, s)
}

func (k *kafkaWrapConn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return k.kafkaConn.StreamUpdates(ctx, r, s)
}
