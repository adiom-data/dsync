/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package testconn

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	bootstrapPath string
	updatesPath   string
	loop          bool

	writeMutex     sync.Mutex
	writeBootstrap *os.File
	writeUpdates   *os.File
}

const defaultCol = "testconncol"

var defaultNamespaces = []string{"testconn.testconncol"}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	namespaces := r.Msg.GetNamespaces()
	var partitions []*adiomv1.Partition
	if len(namespaces) < 1 {
		namespaces = defaultNamespaces
	}
	for _, namespace := range namespaces {
		partitions = append(partitions, &adiomv1.Partition{
			Namespace: namespace,
		})
	}
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        partitions,
		UpdatesPartitions: []*adiomv1.UpdatesPartition{{Namespaces: r.Msg.GetNamespaces()}},
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType:  "testconn",
		Version: "1",
		Spec:    "testconn spec",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON, adiomv1.DataType_DATA_TYPE_JSON_ID},
				LsnStream:          false,
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
			Sink: &adiomv1.Capabilities_Sink{SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON}},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	lines, err := readFile(c.bootstrapPath)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(len(lines)),
	}), nil
}

func bsonToJsonable(bs interface{}) (interface{}, error) {
	switch b := bs.(type) {
	case bson.A:
		var arr []interface{}
		for _, v := range b {
			vv, err := bsonToJsonable(v)
			if err != nil {
				return nil, err
			}
			arr = append(arr, vv)
		}
		return arr, nil
	case bson.D:
		m := map[string]interface{}{}
		for _, v := range b {
			vv, err := bsonToJsonable(v.Value)
			if err != nil {
				return nil, err
			}
			m[v.Key] = vv
		}
		return m, nil
	case bson.M:
		m := map[string]interface{}{}
		for k, v := range b {
			vv, err := bsonToJsonable(v)
			if err != nil {
				return nil, err
			}
			m[k] = vv
		}
		return m, nil
	case bool:
		return b, nil
	case int32:
		return b, nil
	case int64:
		return b, nil
	case float64:
		return b, nil
	case string:
		return b, nil
	case primitive.DateTime:
		return b.Time().Format(time.RFC3339), nil
	case primitive.ObjectID:
		return b.Hex(), nil
	case primitive.Binary:
		return base64.StdEncoding.EncodeToString(b.Data), nil
	case primitive.Decimal128:
		return b.String(), nil
	default:
		return &types.AttributeValueMemberS{Value: "XUnsupportedX"}, nil
	}
}

func toBytes(typ adiomv1.DataType, res interface{}) ([]byte, error) {
	switch typ {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		return bson.Marshal(res)
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		jsonable, err := bsonToJsonable(res)
		if err != nil {
			return nil, err
		}
		asMap := jsonable.(map[string]interface{})
		asMap["id"] = asMap["_id"]
		return json.Marshal(jsonable)
	}
	return nil, fmt.Errorf("unsupported type")
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	lines, err := readFile(c.bootstrapPath)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	var batch [][]byte
	for _, line := range lines {
		var res interface{}
		err := bson.UnmarshalExtJSON([]byte(line), true, &res)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		byts, err := toBytes(r.Msg.GetType(), res)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		batch = append(batch, byts)
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data: batch,
	}), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

func readFile(path string) ([]string, error) {
	f, err := os.Open(filepath.Clean(path))
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
		return connect.NewError(connect.CodeInternal, err)
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

			toMarshal := idRes.(bson.D)[0].Value
			if r.Msg.GetType() == adiomv1.DataType_DATA_TYPE_JSON_ID {
				toMarshal = toMarshal.(primitive.ObjectID).Hex()
			}

			idType, idVal, err := bson.MarshalValue(toMarshal)

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
				doc, err := toBytes(r.Msg.GetType(), res)
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
				doc, err := toBytes(r.Msg.GetType(), res)
				if err != nil {
					slog.Error(err.Error())
					break Loop
				}
				data = doc
			}

			for _, namespace := range namespaces {
				err := s.Send(&adiomv1.StreamUpdatesResponse{
					Updates: []*adiomv1.Update{{
						Id: []*adiomv1.BsonValue{{
							Data: idVal,
							Type: uint32(idType),
						}},
						Type: updateType,
						Data: data,
					}},
					Namespace: namespace,
				})
				if err != nil {
					if !errors.Is(err, context.Canceled) {
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
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		c.writeBootstrap = f
	}
	f := c.writeBootstrap

	for _, data := range r.Msg.GetData() {
		_, err := f.WriteString(bson.Raw(data).String() + "\n")
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	err := f.Sync()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
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
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		c.writeUpdates = f
	}
	f := c.writeUpdates

	for _, update := range r.Msg.GetUpdates() {
		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT:
			idBson := bson.RawValue{Type: bsontype.Type(update.GetId()[0].GetType()), Value: update.GetId()[0].GetData()}
			_, err := f.WriteString("insert\t" + idBson.String() + "\t" + bson.Raw(update.GetData()).String() + "\n")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		case adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			idBson := bson.RawValue{Type: bsontype.Type(update.GetId()[0].GetType()), Value: update.GetId()[0].GetData()}
			_, err := f.WriteString("update\t" + idBson.String() + "\t" + bson.Raw(update.GetData()).String() + "\n")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			idBson := bson.RawValue{Type: bsontype.Type(update.GetId()[0].GetType()), Value: update.GetId()[0].GetData()}
			_, err := f.WriteString("delete\t" + idBson.String() + "\n")
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		default:
			slog.Warn("unsupported update type")
		}
	}

	err := f.Sync()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

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
