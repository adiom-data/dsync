/*
 * Copyright (C) 2025 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	ErrBucketRequired  = errors.New("bucket is required")
	ErrRegionRequired  = errors.New("region is required")
	ErrUnsupportedType = errors.New("unsupported data type for S3 connector")
)

// ConnectorSettings configures the S3 connector.
type ConnectorSettings struct {
	Bucket          string
	Region          string
	OutputFormat    string
	Prefix          string
	Endpoint        string
	Profile         string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	UsePathStyle    bool
}

type taskKey struct {
	taskID uint
}

type storedBatch struct {
	namespace string
	docs      [][]byte
}

type connector struct {
	adiomv1connect.UnimplementedConnectorServiceHandler

	client       *s3.Client
	settings     ConnectorSettings
	batchesMutex sync.Mutex
	batches      map[taskKey]*storedBatch

	metadataMutex sync.Mutex // Serialize metadata updates

	errMutex sync.RWMutex
	err      error
}

// NewConn creates a new S3 sink connector.
func NewConn(settings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	if settings.Bucket == "" {
		return nil, ErrBucketRequired
	}
	if settings.Region == "" {
		return nil, ErrRegionRequired
	}
	if settings.OutputFormat == "" {
		settings.OutputFormat = "json"
	}
	if !strings.EqualFold(settings.OutputFormat, "json") {
		return nil, fmt.Errorf("unsupported output format %q", settings.OutputFormat)
	}

	cfgOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(settings.Region),
	}
	if settings.Profile != "" {
		cfgOpts = append(cfgOpts, awsconfig.WithSharedConfigProfile(settings.Profile))
	}
	if settings.AccessKeyID != "" && settings.SecretAccessKey != "" {
		static := credentials.NewStaticCredentialsProvider(settings.AccessKeyID, settings.SecretAccessKey, settings.SessionToken)
		cfgOpts = append(cfgOpts, awsconfig.WithCredentialsProvider(static))
	}
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), cfgOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if settings.Endpoint != "" {
			o.BaseEndpoint = aws.String(settings.Endpoint)
		}
		if settings.UsePathStyle {
			o.UsePathStyle = true
		}
	})

	return &connector{
		client:   client,
		settings: settings,
		batches:  make(map[taskKey]*storedBatch),
	}, nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType: "s3",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{
					adiomv1.DataType_DATA_TYPE_JSON_ID,
				},
				LsnStream:          false,
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{
					adiomv1.DataType_DATA_TYPE_MONGO_BSON,
					adiomv1.DataType_DATA_TYPE_JSON_ID,
				},
			},
		},
	}), nil
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) GeneratePlan(ctx context.Context, req *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(c.settings.Bucket),
	}

	basePrefix := strings.Trim(c.settings.Prefix, "/")
	if basePrefix != "" {
		// Ensure we only list under the configured prefix
		basePrefix = basePrefix + "/"
		input.Prefix = aws.String(basePrefix)
	}

	paginator := s3.NewListObjectsV2Paginator(c.client, input)

	// If namespaces are specified, only include tasks for those namespaces.
	// If none are specified, include all discovered namespaces.
	requestedNamespaces := map[string]struct{}{}
	for _, ns := range req.Msg.GetNamespaces() {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			continue
		}
		requestedNamespaces[ns] = struct{}{}
	}
	filterByNamespace := len(requestedNamespaces) > 0

	var partitions []*adiomv1.Partition
	// Track which namespaces we've seen to load metadata once per namespace
	namespaceMetadataCache := make(map[string]map[string]uint64)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("list objects: %w", err))
		}

		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			// Only treat JSON files as tasks (skip metadata files)
			if !strings.HasSuffix(strings.ToLower(key), ".json") {
				continue
			}
			// Skip metadata files
			if strings.HasSuffix(key, ".metadata.json") {
				continue
			}

			relKey := key
			if basePrefix != "" {
				relKey = strings.TrimPrefix(relKey, basePrefix)
			}
			relKey = strings.TrimPrefix(relKey, "/")
			if relKey == "" {
				continue
			}

			parts := strings.SplitN(relKey, "/", 2)
			namespace := ""
			if len(parts) > 1 {
				namespace = parts[0]
			}
			if namespace == "" {
				namespace = "default"
			}

			if filterByNamespace {
				if _, ok := requestedNamespaces[namespace]; !ok {
					continue
				}
			}

			// Load metadata for this namespace if not already cached
			if _, ok := namespaceMetadataCache[namespace]; !ok {
				metadata, err := c.readMetadata(ctx, namespace)
				if err != nil {
					slog.Warn("failed to read metadata file for namespace, using default count", "namespace", namespace, "err", err)
				}
				if metadata == nil {
					slog.Warn("metadata file not found for namespace, using default count", "namespace", namespace)
				}
				namespaceMetadataCache[namespace] = metadata
			}

			// Get estimated count from metadata
			estimatedCount := uint64(0)
			if namespaceMetadataCache[namespace] != nil {
				fileName := path.Base(key)
				if count, ok := namespaceMetadataCache[namespace][fileName]; ok {
					estimatedCount = count
				}
			}

			partitions = append(partitions, &adiomv1.Partition{
				Namespace:      namespace,
				Cursor:         []byte(key), // Use the S3 object key as the partition cursor
				EstimatedCount: estimatedCount,
			})
		}
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions: partitions,
		// Updates are not supported for S3 JSON source
		UpdatesPartitions: nil,
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) GetNamespaceMetadata(ctx context.Context, req *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	namespace := req.Msg.GetNamespace()
	if namespace == "" {
		return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
			Count: 0,
		}), nil
	}

	// Read metadata file for the namespace
	metadata, err := c.readMetadata(ctx, namespace)
	if err != nil {
		slog.Warn("failed to read metadata file for namespace, using default count", "namespace", namespace, "err", err)
		return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
			Count: 0,
		}), nil
	}

	if metadata == nil {
		slog.Warn("metadata file not found for namespace, using default count", "namespace", namespace)
		return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
			Count: 0,
		}), nil
	}

	// Sum up all record counts from the metadata
	var totalCount uint64
	for _, count := range metadata {
		totalCount += count
	}

	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: totalCount,
	}), nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) ListData(ctx context.Context, req *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	if req.Msg.GetType() != adiomv1.DataType_DATA_TYPE_JSON_ID {
		return nil, connect.NewError(connect.CodeInvalidArgument, ErrUnsupportedType)
	}

	part := req.Msg.GetPartition()
	if part == nil || len(part.GetCursor()) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partition cursor (S3 object key) is required"))
	}

	// We ignore req.Msg.Cursor for now and return the full contents of the JSON file
	key := string(part.GetCursor())

	getOut, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.settings.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("get object %s: %w", key, err))
	}
	defer getOut.Body.Close()

	var rawDocs []json.RawMessage
	if err := json.NewDecoder(getOut.Body).Decode(&rawDocs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("decode json array from %s: %w", key, err))
	}

	data := make([][]byte, 0, len(rawDocs))
	for _, d := range rawDocs {
		// Ensure each element is valid JSON
		if !json.Valid(d) {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("invalid json element in %s", key))
		}
		// Copy bytes to avoid retaining backing array
		data = append(data, append([]byte(nil), d...))
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       data,
		NextCursor: nil, // Entire file is returned in a single call
	}), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) WriteData(ctx context.Context, req *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	if err := c.currentError(); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	slog.Debug("received write data request", "namespace", req.Msg.GetNamespace(), "taskId", req.Msg.GetTaskId(), "numDocs", len(req.Msg.GetData()))

	taskID := uint(req.Msg.GetTaskId())
	if taskID == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("task id is required"))
	}

	if len(req.Msg.GetData()) == 0 {
		return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
	}

	jsonDocs := make([][]byte, 0, len(req.Msg.GetData()))
	for _, doc := range req.Msg.GetData() {
		converted, err := convertToJSON(doc, req.Msg.GetType())
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		jsonDocs = append(jsonDocs, converted)
	}
	c.appendBatch(req.Msg.GetNamespace(), taskID, jsonDocs)

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *connector) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("updates not supported by S3 connector"))
}

// OnTaskCompletionBarrierHandler flushes buffered data to S3 and updates metadata.
func (c *connector) OnTaskCompletionBarrierHandler(taskID uint) error {
	batch := c.detachBatch(taskID)
	if batch == nil {
		slog.Debug("s3 connector received barrier with no data", "taskId", taskID)
		return nil
	}
	if len(batch.docs) == 0 {
		slog.Debug("s3 connector received barrier with empty batch", "taskId", taskID)
		return nil
	}
	if err := c.flushBatch(batch.namespace, taskID, batch.docs); err != nil {
		slog.Error("failed to flush s3 batch", "namespace", batch.namespace, "taskId", taskID, "err", err)
		c.setError(err)
		return err
	}

	// Update metadata file atomically after successful flush
	if len(batch.docs) > 0 {
		if err := c.updateMetadataAfterFlush(context.Background(), batch.namespace, taskID, uint64(len(batch.docs))); err != nil {
			slog.Error("failed to update metadata", "namespace", batch.namespace, "taskId", taskID, "err", err)
			// Log error but don't fail the barrier - the data was successfully flushed
		}
	}

	return nil
}

func (c *connector) appendBatch(namespace string, taskID uint, docs [][]byte) {
	c.batchesMutex.Lock()
	defer c.batchesMutex.Unlock()
	key := taskKey{taskID}
	batch, ok := c.batches[key]
	if !ok {
		batch = &storedBatch{namespace: namespace}
		c.batches[key] = batch
	}
	batch.docs = append(batch.docs, docs...)
}

func (c *connector) detachBatch(taskID uint) *storedBatch {
	c.batchesMutex.Lock()
	defer c.batchesMutex.Unlock()
	key := taskKey{taskID}
	batch := c.batches[key]
	delete(c.batches, key)
	return batch
}

func (c *connector) flushBatch(namespace string, taskID uint, docs [][]byte) error {
	payload := buildJSONArray(docs)
	key := c.objectKey(namespace, taskID)
	_, err := c.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:      aws.String(c.settings.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	slog.Debug("flushed s3 batch", "namespace", namespace, "taskId", taskID, "key", key, "numDocs", len(docs))
	return nil
}

func (c *connector) objectKey(namespace string, taskID uint) string {
	nsPath := strings.ReplaceAll(strings.Trim(namespace, "/"), ".", "/")
	if nsPath == "" {
		nsPath = "default"
	}
	fileName := fmt.Sprintf("task-%d.json", taskID)

	prefix := strings.Trim(c.settings.Prefix, "/")
	if prefix == "" {
		return path.Join(nsPath, fileName)
	}
	return path.Join(prefix, nsPath, fileName)
}

func (c *connector) currentError() error {
	c.errMutex.RLock()
	defer c.errMutex.RUnlock()
	return c.err
}

func (c *connector) setError(err error) {
	c.errMutex.Lock()
	defer c.errMutex.Unlock()
	if c.err == nil {
		c.err = err
	}
}

func buildJSONArray(docs [][]byte) []byte {
	var buf bytes.Buffer
	buf.Grow(len(docs) * 2)
	buf.WriteByte('[')
	for i, doc := range docs {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(doc)
	}
	buf.WriteByte(']')
	return buf.Bytes()
}

func convertToJSON(data []byte, dataType adiomv1.DataType) ([]byte, error) {
	switch dataType {
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		if !json.Valid(data) {
			return nil, fmt.Errorf("invalid json payload")
		}
		return append([]byte(nil), data...), nil
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		var doc map[string]any
		if err := bson.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("bson to json: %w", err)
		}
		converted, err := json.Marshal(doc)
		if err != nil {
			return nil, fmt.Errorf("marshal json: %w", err)
		}
		return converted, nil
	default:
		return nil, ErrUnsupportedType
	}
}

// metadataKey returns the S3 key for the metadata file for a given namespace.
func (c *connector) metadataKey(namespace string) string {
	nsPath := strings.ReplaceAll(strings.Trim(namespace, "/"), ".", "/")
	if nsPath == "" {
		nsPath = "default"
	}

	prefix := strings.Trim(c.settings.Prefix, "/")
	if prefix == "" {
		return path.Join(nsPath, ".metadata.json")
	}
	return path.Join(prefix, nsPath, ".metadata.json")
}

// readMetadata reads the metadata.json file for a namespace from S3.
// Returns nil, nil if the file doesn't exist.
func (c *connector) readMetadata(ctx context.Context, namespace string) (map[string]uint64, error) {
	key := c.metadataKey(namespace)
	getOut, err := c.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.settings.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if it's a "NoSuchKey" error - this is expected if metadata file doesn't exist yet
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			// Check for NoSuchKey error code (used by S3)
			if apiErr.ErrorCode() == "NoSuchKey" {
				return nil, nil
			}
		}
		return nil, fmt.Errorf("get metadata object %s: %w", key, err)
	}
	defer getOut.Body.Close()

	var metadata map[string]uint64
	if err := json.NewDecoder(getOut.Body).Decode(&metadata); err != nil {
		return nil, fmt.Errorf("decode metadata json from %s: %w", key, err)
	}
	return metadata, nil
}

// writeMetadata atomically writes the metadata.json file for a namespace to S3.
func (c *connector) writeMetadata(ctx context.Context, namespace string, metadata map[string]uint64) error {
	key := c.metadataKey(namespace)
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata json: %w", err)
	}

	_, err = c.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(c.settings.Bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(metadataJSON),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("put metadata object %s: %w", key, err)
	}
	return nil
}

// updateMetadataAfterFlush updates the metadata file after a batch has been flushed.
// It reads the current metadata, adds/updates the file entry, and writes it back atomically.
func (c *connector) updateMetadataAfterFlush(ctx context.Context, namespace string, taskID uint, recordCount uint64) error {
	c.metadataMutex.Lock()
	defer c.metadataMutex.Unlock()

	// Read current metadata (or create empty map if doesn't exist)
	metadata, err := c.readMetadata(ctx, namespace)
	if err != nil {
		return fmt.Errorf("read metadata: %w", err)
	}
	if metadata == nil {
		metadata = make(map[string]uint64)
	}

	// Get the file key that was just written
	fileKey := c.objectKey(namespace, taskID)
	// Extract just the filename for the metadata map
	// The metadata should map filename -> record count
	fileName := path.Base(fileKey)

	// Update the metadata with the new file and record count
	metadata[fileName] = recordCount

	// Write back atomically
	if err := c.writeMetadata(ctx, namespace, metadata); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	return nil
}
