/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package null

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
)

type conn struct {
	id          string
	logJson     bool
	sleep       time.Duration
	sleepJitter time.Duration
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	var all []adiomv1.DataType
	for d := range adiomv1.DataType_name {
		all = append(all, adiomv1.DataType(d))
	}
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     c.id,
		DbType: "/dev/null",
		Capabilities: &adiomv1.Capabilities{
			Sink: &adiomv1.Capabilities_Sink{SupportedDataTypes: all},
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
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
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
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	if c.sleep > 0 {
		select {
		case <-time.After(c.sleep + time.Duration(rand.Int63n(int64(1+c.sleepJitter)))): // #nosec G404
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
	if c.logJson {
		for i, data := range r.Msg.GetData() {
			var output = "unknown"
			switch r.Msg.GetType() {
			case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
				var m map[string]any
				if err := bson.Unmarshal(data, &m); err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}
				jsonOutput, err := json.Marshal(m)
				if err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}
				output = string(jsonOutput)
			case adiomv1.DataType_DATA_TYPE_JSON_ID:
				output = string(data)
			}
			slog.Info("write-data", "i", i, "namespace", r.Msg.GetNamespace(), "data", output, "size", len(data))
		}
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	if c.sleep > 0 {
		select {
		case <-time.After(c.sleep + time.Duration(rand.Int63n(int64(1+c.sleepJitter)))): // #nosec G404
		case <-ctx.Done():
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err())
		}
	}
	if c.logJson {
		for i, updates := range r.Msg.GetUpdates() {
			var output = "unknown"
			var idOutput []any
			for _, id := range updates.GetId() {
				var v any
				if err := bson.UnmarshalValue(bsontype.Type(id.GetType()), id.GetData(), &v); err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}
				idOutput = append(idOutput, v)
			}
			if updates.Type == adiomv1.UpdateType_UPDATE_TYPE_DELETE {
				slog.Info("write-updates", "i", i, "namespace", r.Msg.GetNamespace(), "type", updates.Type.String(), "id", idOutput)
				continue
			}
			switch r.Msg.GetType() {
			case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
				var m map[string]any
				if err := bson.Unmarshal(updates.GetData(), &m); err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}
				jsonOutput, err := json.Marshal(m)
				if err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}
				output = string(jsonOutput)
			case adiomv1.DataType_DATA_TYPE_JSON_ID:
				output = string(updates.GetData())
			}
			slog.Info("write-updates", "i", i, "namespace", r.Msg.GetNamespace(), "type", updates.Type.String(), "id", idOutput, "data", output, "size", len(updates.GetData()))
		}
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func NewConn(id string, logJson bool, sleep time.Duration, sleepJitter time.Duration) adiomv1connect.ConnectorServiceHandler {
	return &conn{id, logJson, sleep, sleepJitter}
}
