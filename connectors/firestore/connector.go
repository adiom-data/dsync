/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package firestore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"cloud.google.com/go/firestore"
	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/api/option"
)

const (
	DefaultBatchSize = 500 // Firestore max batch size
	connectorDBType  = "firestore"
)

var (
	ErrProjectIDRequired = errors.New("project ID is required in connection string")
	ErrInvalidURI        = errors.New("invalid firestore connection string format")
	ErrUnsupportedType   = errors.New("unsupported data type for Firestore connector")
	ErrMissingDocumentID = errors.New("document missing _id field")
)

type ConnectorSettings struct {
	Uri             string
	ProjectID       string
	DatabaseID      string
	CredentialsFile string
	BatchSize       int
	ID              string
}

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler

	client   *firestore.Client
	settings ConnectorSettings
}

func parseFirestoreURI(uri string) (projectID, databaseID string, err error) {
	const prefix = "firestore://"
	if !strings.HasPrefix(strings.ToLower(uri), prefix) {
		return "", "", ErrInvalidURI
	}

	path := uri[len(prefix):]
	if path == "" {
		return "", "", ErrProjectIDRequired
	}

	parts := strings.SplitN(path, "/", 2)
	projectID = parts[0]
	if projectID == "" {
		return "", "", ErrProjectIDRequired
	}

	databaseID = "(default)"
	if len(parts) == 2 && parts[1] != "" {
		databaseID = parts[1]
	}

	return projectID, databaseID, nil
}

func namespaceToCollection(namespace string) string {
	return strings.ReplaceAll(namespace, ".", "_")
}

func NewConn(ctx context.Context, settings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	projectID, databaseID, err := parseFirestoreURI(settings.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to parse firestore URI: %w", err)
	}

	settings.ProjectID = projectID
	settings.DatabaseID = databaseID

	if settings.BatchSize <= 0 || settings.BatchSize > DefaultBatchSize {
		settings.BatchSize = DefaultBatchSize
	}

	var opts []option.ClientOption
	if settings.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(settings.CredentialsFile))
	}

	client, err := firestore.NewClientWithDatabase(ctx, projectID, databaseID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create firestore client: %w", err)
	}

	return &conn{
		client:   client,
		settings: settings,
	}, nil
}

func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     c.settings.ID,
		DbType: connectorDBType,
		Spec:   fmt.Sprintf("project=%s,database=%s", c.settings.ProjectID, c.settings.DatabaseID),
		Capabilities: &adiomv1.Capabilities{
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{
					adiomv1.DataType_DATA_TYPE_MONGO_BSON,
					adiomv1.DataType_DATA_TYPE_JSON_ID,
				},
			},
		},
	}), nil
}

func (c *conn) GetNamespaceMetadata(context.Context, *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: 0,
	}), nil
}

func (c *conn) GeneratePlan(context.Context, *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func (c *conn) ListData(context.Context, *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func (c *conn) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	data := r.Msg.GetData()
	if len(data) == 0 {
		return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
	}

	namespace := r.Msg.GetNamespace()
	collectionName := namespaceToCollection(namespace)
	dataType := r.Msg.GetType()

	for i := 0; i < len(data); i += c.settings.BatchSize {
		end := i + c.settings.BatchSize
		if end > len(data) {
			end = len(data)
		}
		batch := data[i:end]

		if err := c.writeBatch(ctx, collectionName, batch, dataType); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to write batch to %s: %w", collectionName, err))
		}
	}

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	updates := r.Msg.GetUpdates()
	if len(updates) == 0 {
		return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
	}

	namespace := r.Msg.GetNamespace()
	collectionName := namespaceToCollection(namespace)
	dataType := r.Msg.GetType()

	for i := 0; i < len(updates); i += c.settings.BatchSize {
		end := i + c.settings.BatchSize
		if end > len(updates) {
			end = len(updates)
		}
		batch := updates[i:end]

		if err := c.writeUpdatesBatch(ctx, collectionName, batch, dataType); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to write updates batch to %s: %w", collectionName, err))
		}
	}

	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func (c *conn) writeBatch(ctx context.Context, collectionName string, data [][]byte, dataType adiomv1.DataType) error {
	batch := c.client.Batch()
	collection := c.client.Collection(collectionName)

	for _, raw := range data {
		docID, docData, err := extractDocumentIDAndData(raw, dataType)
		if err != nil {
			return fmt.Errorf("failed to extract document ID: %w", err)
		}

		docRef := collection.Doc(docID)
		batch.Set(docRef, docData)
	}

	_, err := batch.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}

	slog.Debug("wrote batch to firestore", "collection", collectionName, "count", len(data))
	return nil
}

func (c *conn) writeUpdatesBatch(ctx context.Context, collectionName string, updates []*adiomv1.Update, dataType adiomv1.DataType) error {
	batch := c.client.Batch()
	collection := c.client.Collection(collectionName)
	idKey := getIDFieldName(dataType)

	for _, update := range updates {
		docID, err := extractIDFromBsonValues(update.GetId())
		if err != nil {
			return fmt.Errorf("failed to extract document ID from update: %w", err)
		}

		docRef := collection.Doc(docID)

		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			batch.Delete(docRef)
		case adiomv1.UpdateType_UPDATE_TYPE_UPDATE, adiomv1.UpdateType_UPDATE_TYPE_INSERT:
			docData, err := rawToMap(update.GetData(), dataType)
			if err != nil {
				return fmt.Errorf("failed to convert update data: %w", err)
			}
			delete(docData, idKey)
			batch.Set(docRef, docData)
		default:
			slog.Warn("unknown update type, treating as upsert", "type", update.GetType())
			docData, err := rawToMap(update.GetData(), dataType)
			if err != nil {
				return fmt.Errorf("failed to convert update data: %w", err)
			}
			delete(docData, idKey)
			batch.Set(docRef, docData)
		}
	}

	_, err := batch.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit updates batch: %w", err)
	}

	slog.Debug("wrote updates batch to firestore", "collection", collectionName, "count", len(updates))
	return nil
}

func getIDFieldName(dataType adiomv1.DataType) string {
	switch dataType {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		return "_id"
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		return "id"
	default:
		return "_id"
	}
}

func extractDocumentIDAndData(raw []byte, dataType adiomv1.DataType) (string, map[string]any, error) {
	docData, err := rawToMap(raw, dataType)
	if err != nil {
		return "", nil, err
	}

	idKey := getIDFieldName(dataType)
	idVal, ok := docData[idKey]
	if !ok {
		return "", nil, fmt.Errorf("%w: expected field '%s'", ErrMissingDocumentID, idKey)
	}

	docID, err := valueToString(idVal)
	if err != nil {
		return "", nil, fmt.Errorf("failed to convert %s to string: %w", idKey, err)
	}

	delete(docData, idKey)
	return docID, docData, nil
}

func rawToMap(raw []byte, dataType adiomv1.DataType) (map[string]any, error) {
	var docData map[string]any

	switch dataType {
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		if err := bson.Unmarshal(raw, &docData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal BSON: %w", err)
		}
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		if err := json.Unmarshal(raw, &docData); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	default:
		return nil, ErrUnsupportedType
	}

	return docData, nil
}

func valueToString(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case bson.ObjectID:
		return val.Hex(), nil
	case int, int32, int64:
		return fmt.Sprintf("%d", val), nil
	case uint, uint32, uint64:
		return fmt.Sprintf("%d", val), nil
	case float32, float64:
		return fmt.Sprintf("%v", val), nil
	case bson.Binary:
		return fmt.Sprintf("%x", val.Data), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

func extractIDFromBsonValues(ids []*adiomv1.BsonValue) (string, error) {
	if len(ids) == 0 {
		return "", ErrMissingDocumentID
	}

	// Use the first ID field (typically _id)
	id := ids[0]
	var v any
	if err := bson.UnmarshalValue(bson.Type(id.GetType()), id.GetData(), &v); err != nil {
		return "", fmt.Errorf("failed to unmarshal BSON value: %w", err)
	}

	return valueToString(v)
}

func (c *conn) Teardown() {
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			slog.Error("failed to close firestore client", "err", err)
		}
	}
}
