package spanner

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
	"google.golang.org/api/iterator"
)

type SpannerSettings struct {
	Database string
	Limit    int
}

type conn struct {
	client   *spanner.Client
	settings SpannerSettings
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	panic("unimplemented")
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	panic("unimplemented")
}

func NewConn(ctx context.Context, settings SpannerSettings) (*conn, error) {
	client, err := spanner.NewClient(ctx, settings.Database)
	if err != nil {
		return nil, err
	}
	return &conn{
		client:   client,
		settings: settings,
	}, nil
}

func (c *conn) Teardown() {
	c.client.Close()
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	// TODO: Implement partitioning logic for Spanner
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     "spanner-conn",
		DbType: "spanner",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
				MultiNamespacePlan: true,
			},
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_MONGO_BSON},
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(ctx context.Context, r *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	namespace := r.Msg.GetNamespace()
	stmt := spanner.Statement{SQL: fmt.Sprintf("SELECT COUNT(*) FROM %s", namespace)}
	iter := c.client.Single().Query(ctx, stmt)
	defer iter.Stop()
	var count int64
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if err := row.Columns(&count); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(count),
	}), nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	namespace := r.Msg.GetPartition().GetNamespace()
	limit := c.settings.Limit
	if limit == 0 {
		limit = 1000
	}
	stmt := spanner.Statement{SQL: fmt.Sprintf("SELECT * FROM %s LIMIT %d", namespace, limit)}
	iter := c.client.Single().Query(ctx, stmt)
	defer iter.Stop()
	var data [][]byte
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		// TODO: Convert row to map[string]interface{} and marshal to BSON
		m := make(map[string]interface{})
		if err := row.ToStruct(&m); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		marshalled, err := bson.Marshal(m)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		data = append(data, marshalled)
	}
	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data: data,
	}), nil
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, req *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	namespace := req.Msg.GetNamespace()
	data := req.Msg.GetData()
	if len(data) == 0 {
		return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
	}
	var muts []*spanner.Mutation
	for _, raw := range data {
		var m map[string]interface{}
		if err := bson.Unmarshal(raw, &m); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to unmarshal BSON: %w", err))
		}
		muts = append(muts, spanner.InsertOrUpdateMap(namespace, m))
	}
	_, err := c.client.Apply(ctx, muts)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, req *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	namespace := req.Msg.GetNamespace()
	updates := req.Msg.GetUpdates()
	var muts []*spanner.Mutation
	for _, update := range updates {
		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			var m map[string]interface{}
			if err := bson.Unmarshal(update.GetData(), &m); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to unmarshal BSON: %w", err))
			}
			muts = append(muts, spanner.InsertOrUpdateMap(namespace, m))
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			// TODO: Extract primary key from update.GetId() and create Delete mutation
		}
	}
	_, err := c.client.Apply(ctx, muts)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

var _ adiomv1connect.ConnectorServiceHandler = &conn{}
