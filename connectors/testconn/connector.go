/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package testconn

import (
	"bufio"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type conn struct {
	bootstrapPath string
	updatesPath   string
	loop          bool

	writeMutex     sync.Mutex
	writeBootstrap *os.File
	writeUpdates   *os.File
}

var defaultNamespaces = []*adiomv1.Namespace{{Db: "testconn", Col: "testconncol"}}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	namespaces := r.Msg.GetNamespaces()
	var partitions []*adiomv1.Partition
	if len(namespaces) < 1 {
		namespaces = defaultNamespaces
	}
	for _, namespace := range namespaces {
		namespace.GetDb()
		partitions = append(partitions, &adiomv1.Partition{
			Namespace: namespace,
		})
	}
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions: partitions,
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType:             "testconn",
		Version:            "1",
		Spec:               "testconn spec",
		SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
		Capabilities: &adiomv1.Capabilities{
			Source:    true,
			Sink:      true,
			Resumable: false,
			LsnStream: true,
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	lines, err := readFile(c.bootstrapPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(len(lines)),
	}), nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	lines, err := readFile(c.bootstrapPath)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	var batch [][]byte
	for _, line := range lines {
		var res interface{}
		err := bson.UnmarshalExtJSON([]byte(line), true, &res)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		bsonBytes, err := bson.Marshal(res)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		batch = append(batch, bsonBytes)
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data: batch,
		Type: adiomv1.DataType_DATA_TYPE_MONGO_BSON,
	}), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

func readFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var allLines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		allLines = append(allLines, line)
	}
	return allLines, nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	lines, err := readFile(c.updatesPath)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}

	namespaces := r.Msg.GetNamespaces()
	if len(namespaces) < 1 {
		namespaces = defaultNamespaces
	}
Loop:
	for {
		for _, line := range lines {
			splitted := strings.SplitN(line, "\t", 3)
			var idRes interface{}
			// Weird, but wrapping the id this lets us parse the type properly
			err := bson.UnmarshalExtJSON([]byte("{\"id\": "+splitted[1]+"}"), true, &idRes)
			if err != nil {
				slog.Error(err.Error())
				break Loop
			}
			idType, idVal, err := bson.MarshalValue(idRes.(bson.D)[0].Value)

			if err != nil {
				slog.Error(err.Error())
				break Loop
			}

			var data []byte
			var updateType adiomv1.UpdateType

			if splitted[0] == "delete" {
				updateType = adiomv1.UpdateType_UPDATE_TYPE_DELETE
			} else if splitted[0] == "insert" {
				updateType = adiomv1.UpdateType_UPDATE_TYPE_INSERT
				var res interface{}
				err = bson.UnmarshalExtJSON([]byte(splitted[2]), true, &res)
				if err != nil {
					slog.Error(err.Error())
					break Loop
				}
				doc, err := bson.Marshal(res)
				if err != nil {
					slog.Error(err.Error())
					break Loop
				}
				data = doc
			} else if splitted[0] == "update" {
				updateType = adiomv1.UpdateType_UPDATE_TYPE_UPDATE
				var res interface{}
				err = bson.UnmarshalExtJSON([]byte(splitted[2]), true, &res)
				if err != nil {
					slog.Error(err.Error())
					break Loop
				}
				doc, err := bson.Marshal(res)
				if err != nil {
					slog.Error(err.Error())
					break Loop
				}
				data = doc
			}

			for _, namespace := range namespaces {
				err := s.Send(&adiomv1.StreamUpdatesResponse{
					Updates: []*adiomv1.Update{{
						Id: &adiomv1.BsonValue{
							Data: idVal,
							Type: uint32(idType),
						},
						Type: updateType,
						Data: data,
					}},
					Namespace: namespace,
					Type:      adiomv1.DataType_DATA_TYPE_MONGO_BSON,
				})
				if err != nil {
					st, ok := status.FromError(err)
					if !ok || !errors.Is(st.Err(), context.Canceled) {
						slog.Error(err.Error())
					}
					break Loop
				}
			}
		}

		if !c.loop {
			break
		}
	}
	return nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	if c.writeBootstrap == nil {
		f, err := os.Create(c.bootstrapPath)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		c.writeBootstrap = f
	}
	f := c.writeBootstrap

	for _, data := range r.Msg.GetData() {
		_, err := f.WriteString(bson.Raw(data).String() + "\n")
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	err := f.Sync()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()
	if c.writeUpdates == nil {
		f, err := os.Create(c.updatesPath)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		c.writeUpdates = f
	}
	f := c.writeUpdates

	for _, update := range r.Msg.GetUpdates() {
		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT:
			idBson := bson.RawValue{Type: bsontype.Type(update.GetId().GetType()), Value: update.GetId().GetData()}
			_, err := f.WriteString("insert\t" + idBson.String() + "\t" + bson.Raw(update.GetData()).String() + "\n")
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		case adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			idBson := bson.RawValue{Type: bsontype.Type(update.GetId().GetType()), Value: update.GetId().GetData()}
			_, err := f.WriteString("update\t" + idBson.String() + "\t" + bson.Raw(update.GetData()).String() + "\n")
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			idBson := bson.RawValue{Type: bsontype.Type(update.GetId().GetType()), Value: update.GetId().GetData()}
			_, err := f.WriteString("delete\t" + idBson.String() + "\n")
			if err != nil {
				return nil, status.Error(codes.Internal, err.Error())
			}
		default:
			slog.Warn("unsupported update type")
		}
	}

	err := f.Sync()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

// TODO: hook into graceful shutdown
func (c *conn) Teardown() {
	_ = c.writeBootstrap.Close()
	_ = c.writeUpdates.Close()
}

func NewConn(path string) adiomv1connect.ConnectorServiceHandler {
	return &conn{
		bootstrapPath: filepath.Join(path, "bootstrap.json"),
		updatesPath:   filepath.Join(path, "updates.json"),
		loop:          false,
	}
}
