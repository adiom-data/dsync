/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package random

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
)

type conn struct {
	settings ConnectorSettings

	docMap      map[string]*IndexMap //map of locations to map of document IDs
	docMapMutex sync.RWMutex
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	if len(r.Msg.GetNamespaces()) == 1 {
		return connect.NewResponse(&adiomv1.GeneratePlanResponse{
			Partitions: []*adiomv1.Partition{{
				Namespace:      r.Msg.GetNamespaces()[0],
				EstimatedCount: uint64(c.settings.numCollectionsPerDatabase),
			}},
			UpdatesPartitions: []*adiomv1.UpdatesPartition{{Namespaces: r.Msg.GetNamespaces(), Cursor: []byte{1}}},
		}), nil
	}
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        c.CreateInitialGenerationTasks(),
		UpdatesPartitions: []*adiomv1.UpdatesPartition{{Cursor: []byte{1}}},
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType: "/dev/random",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
				DefaultPlan:        true,
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(c.settings.maxDocsPerCollection),
	}), nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	res, err := c.ProcessDataGenerationTaskBatch(r.Msg.GetPartition())
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(res), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (rc *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	//timeout for changestream generation
	ctx, cancel := context.WithTimeout(ctx, time.Duration(rc.settings.changeStreamDuration)*time.Second)
	defer cancel()

	//continuos change stream generator simulates a random change operation every second,
	//either a single insert, batch insert, single update, or single delete.
	//XXX: change stream currently uses one thread, should we parallelize it with multiple threads to generate more changes?
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			//generate random operation
			operation, err := rc.generateOperation()
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to generate operation: %v", err))
				return err
			}
			dataMsg, err := rc.generateChangeStreamEvent(operation) //XXX: ID logic will not work with insertBatch, will need to change
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to generate change stream event: %v", err))
				return err
			}
			err = s.Send(dataMsg)
			if err != nil {
				return err
			}
		}
	}
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(context.Context, *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func NewConn(settings ConnectorSettings) adiomv1connect.ConnectorServiceHandler {
	settings.numParallelGenerators = 4

	settings.numDatabases = 10
	settings.numCollectionsPerDatabase = 2
	settings.numInitialDocumentsPerCollection = 500
	settings.numFields = 10
	settings.docSize = 15 //
	settings.maxDocsPerCollection = 1000
	settings.changeStreamDuration = 60

	settings.probabilities = []float64{0.34, 0.33, 0.33}

	return &conn{settings: settings, docMap: map[string]*IndexMap{}}
}

type ConnectorSettings struct {
	numParallelGenerators int //number of parallel data generators

	numDatabases                     int  //must be at least 1
	numCollectionsPerDatabase        int  //must be at least 1
	numInitialDocumentsPerCollection int  //must be at least 1
	numFields                        int  //number of fields in each document, must be at least 1
	docSize                          uint //size of field values in number of chars/bytes, must be at least 1
	maxDocsPerCollection             int  //maximum number of documents per collection, cap the change stream
	changeStreamDuration             int  //duration of change stream in seconds
	//list of size 4, representing probabilities of change stream operations in order: insert, insertBatch, update, delete
	//sum of probabilities must add to 1.0
	probabilities []float64
}
