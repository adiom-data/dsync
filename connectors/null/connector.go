/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package null

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
)

type conn struct {
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType: "/dev/null",
		Capabilities: &adiomv1.Capabilities{
			Sink: &adiomv1.Capabilities_Sink{SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON}},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: 0,
	}), nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func NewConn() adiomv1connect.ConnectorServiceHandler {
	return &conn{}
}
