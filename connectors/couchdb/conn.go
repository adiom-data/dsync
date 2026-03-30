/*
 * Copyright (C) 2024 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package couchdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/go-kivik/kivik/v4"
	_ "github.com/go-kivik/kivik/v4/couchdb"
	"github.com/mitchellh/hashstructure"
)

const (
	connectorDBType string = "CouchDB"
	connectorSpec   string = "Apache CouchDB / IBM Cloudant"
)

var ExcludedSystemDatabases = []string{"_users", "_replicator", "_global_changes"}

type ConnectorSettings struct {
	Uri                        string
	ServerConnectTimeout       time.Duration
	PingTimeout                time.Duration
	WriterMaxBatchSize         int
	TargetDocCountPerPartition int64
	MaxPageSize                int
	IncludeSystemDbs           bool
}

func setDefault[T comparable](field *T, defaultValue T) {
	if *field == *new(T) {
		*field = defaultValue
	}
}

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	client   *kivik.Client
	settings ConnectorSettings
	dsn      string
}

func generateConnectorID(connectionString string) string {
	id, err := hashstructure.Hash(connectionString, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to hash the connection string: %v", err))
	}
	return strconv.FormatUint(id, 16)
}

func convertUriToDSN(uri string) (string, error) {
	lower := strings.ToLower(uri)
	if strings.HasPrefix(lower, "couchdb://") {
		return "http://" + strings.TrimPrefix(uri, "couchdb://"), nil
	}
	if strings.HasPrefix(lower, "couchdbs://") {
		return "https://" + strings.TrimPrefix(uri, "couchdbs://"), nil
	}
	if strings.HasPrefix(lower, "cloudant://") {
		return "https://" + strings.TrimPrefix(uri, "cloudant://"), nil
	}
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return uri, nil
	}
	return "", fmt.Errorf("unsupported URI scheme: %s", uri)
}

func getAllDatabases(ctx context.Context, client *kivik.Client, includeSystem bool) ([]string, error) {
	dbs, err := client.AllDBs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	if includeSystem {
		return dbs, nil
	}

	filtered := slices.DeleteFunc(dbs, func(db string) bool {
		return strings.HasPrefix(db, "_") || slices.Contains(ExcludedSystemDatabases, db)
	})

	return filtered, nil
}

func (c *conn) GetInfo(ctx context.Context, r *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	version, err := c.client.Version(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get server version: %w", err))
	}

	dbType := connectorDBType
	if strings.Contains(strings.ToLower(version.Vendor), "cloudant") {
		dbType = "Cloudant"
	}

	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:      generateConnectorID(c.settings.Uri),
		DbType:  dbType,
		Version: version.Version,
		Spec:    connectorSpec,
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_JSON_ID},
				LsnStream:          true,
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_JSON_ID},
			},
		},
	}), nil
}

func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	var namespaces []string

	if len(r.Msg.GetNamespaces()) > 0 {
		namespaces = r.Msg.GetNamespaces()
	} else {
		dbs, err := getAllDatabases(ctx, c.client, c.settings.IncludeSystemDbs)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		namespaces = dbs
	}

	var partitions []*adiomv1.Partition
	var updatesPartitions []*adiomv1.UpdatesPartition

	for _, ns := range namespaces {
		db := c.client.DB(ns)

		stats, err := db.Stats(ctx)
		if err != nil {
			slog.Warn(fmt.Sprintf("Failed to get stats for database %s: %v", ns, err))
			continue
		}

		if r.Msg.GetInitialSync() {
			count := int64(stats.DocCount)
			if count <= c.settings.TargetDocCountPerPartition {
				partitions = append(partitions, &adiomv1.Partition{
					Namespace:      ns,
					EstimatedCount: uint64(count),
				})
			} else {
				numPartitions := (count + c.settings.TargetDocCountPerPartition - 1) / c.settings.TargetDocCountPerPartition
				docsPerPartition := count / numPartitions

				// Find partition boundaries using chained startkey queries
				// Each query skips docsPerPartition from the previous boundary
				var boundaries []string
				var startKey string

				for i := 1; i < int(numPartitions); i++ {
					params := map[string]interface{}{
						"include_docs": false,
						"limit":        1,
						"skip":         docsPerPartition,
					}
					if startKey != "" {
						params["startkey"] = startKey
					}

					rows := db.AllDocs(ctx, kivik.Params(params))
					if rows.Next() {
						id, err := rows.ID()
						if err == nil {
							boundaries = append(boundaries, id)
							startKey = id
						}
					}
					_ = rows.Close()

					if len(boundaries) < i {
						break // No more docs available
					}
				}

				var prevKey string
				for i, boundary := range boundaries {
					cursor := encodeCursor(prevKey, boundary)
					partitions = append(partitions, &adiomv1.Partition{
						Namespace:      ns,
						Cursor:         cursor,
						EstimatedCount: uint64(docsPerPartition),
					})
					prevKey = boundary
					if i == len(boundaries)-1 {
						partitions = append(partitions, &adiomv1.Partition{
							Namespace:      ns,
							Cursor:         encodeCursor(boundary, ""),
							EstimatedCount: uint64(docsPerPartition),
						})
					}
				}

				if len(boundaries) == 0 {
					partitions = append(partitions, &adiomv1.Partition{
						Namespace:      ns,
						EstimatedCount: uint64(count),
					})
				}
			}
		} else {
			partitions = append(partitions, &adiomv1.Partition{
				Namespace: ns,
			})
		}

		if r.Msg.GetUpdates() {
			changes := db.Changes(ctx, kivik.Params(map[string]interface{}{
				"descending": true,
				"limit":      1,
			}))
			var lastSeq string
			for changes.Next() {
				lastSeq = changes.Seq()
			}
			if err := changes.Err(); err != nil {
				slog.Warn(fmt.Sprintf("Failed to get changes for database %s: %v", ns, err))
			}
			_ = changes.Close()

			if lastSeq == "" {
				info := db.Changes(ctx, kivik.Params(map[string]interface{}{
					"since": "now",
					"limit": 0,
				}))
				for info.Next() {
				}
				meta, err := info.Metadata()
				if err == nil && meta != nil {
					lastSeq = meta.LastSeq
				}
				_ = info.Close()
			}

			updatesPartitions = append(updatesPartitions, &adiomv1.UpdatesPartition{
				Namespaces: []string{ns},
				Cursor:     []byte(lastSeq),
			})
		}
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        partitions,
		UpdatesPartitions: updatesPartitions,
	}), nil
}

func encodeCursor(startKey, endKey string) []byte {
	if startKey == "" && endKey == "" {
		return nil
	}
	data, _ := json.Marshal(map[string]string{"s": startKey, "e": endKey})
	return data
}

func decodeCursor(cursor []byte) (startKey, endKey string) {
	if len(cursor) == 0 {
		return "", ""
	}
	var m map[string]string
	if err := json.Unmarshal(cursor, &m); err != nil {
		return "", ""
	}
	return m["s"], m["e"]
}

func (c *conn) GetNamespaceMetadata(ctx context.Context, r *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	db := c.client.DB(r.Msg.GetNamespace())
	stats, err := db.Stats(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to get database stats: %w", err))
	}

	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(stats.DocCount),
	}), nil
}

func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	partition := r.Msg.GetPartition()
	db := c.client.DB(partition.GetNamespace())

	pageSize := c.settings.MaxPageSize
	if pageSize == 0 {
		pageSize = 1000
	}

	params := map[string]interface{}{
		"include_docs": true,
		"limit":        pageSize + 1, // fetch one extra to detect if there's a next page
	}

	// Always apply partition end bound
	startKey, endKey := decodeCursor(partition.GetCursor())
	if endKey != "" {
		params["endkey"] = endKey
	}

	pageCursor := r.Msg.GetCursor()
	if len(pageCursor) > 0 {
		// Page cursor from previous call - start after this key
		params["startkey"] = string(pageCursor)
		params["skip"] = 1 // skip the document we already returned
	} else {
		// Initial call - use partition start key if present
		if startKey != "" {
			params["startkey"] = startKey
			params["skip"] = 1
		}
	}

	rows := db.AllDocs(ctx, kivik.Params(params))
	defer rows.Close()

	var data [][]byte
	var lastID string
	hasMore := false

	for rows.Next() {
		if len(data) >= pageSize {
			// We got one extra - there's more data
			hasMore = true
			break
		}

		var doc map[string]interface{}
		if err := rows.ScanDoc(&doc); err != nil {
			slog.Warn(fmt.Sprintf("Failed to scan document: %v", err))
			continue
		}

		delete(doc, "_rev")

		jsonBytes, err := json.Marshal(doc)
		if err != nil {
			slog.Warn(fmt.Sprintf("Failed to marshal document: %v", err))
			continue
		}

		if id, err := rows.ID(); err == nil {
			lastID = id
		}
		data = append(data, jsonBytes)
	}

	if err := rows.Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Error(fmt.Sprintf("Error iterating documents: %v", err))
		}
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var nextCursor []byte
	if hasMore && lastID != "" {
		nextCursor = []byte(lastID)
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       data,
		NextCursor: nextCursor,
	}), nil
}

func (c *conn) writeDataBatch(ctx context.Context, db *kivik.DB, docs []map[string]interface{}) error {
	if len(docs) == 0 {
		return nil
	}

	// Convert to interface slice for BulkDocs
	bulkDocs := make([]interface{}, len(docs))
	for i, doc := range docs {
		bulkDocs[i] = doc
	}

	// First attempt: optimistic insert
	results, err := db.BulkDocs(ctx, bulkDocs)
	if err != nil {
		return fmt.Errorf("failed to bulk insert documents: %w", err)
	}

	// Collect conflicts (409 errors)
	var conflictIDs []string
	conflictDocs := make(map[string]map[string]interface{})
	for i, result := range results {
		if result.Error != nil {
			if kivik.HTTPStatus(result.Error) == 409 {
				docID, ok := docs[i]["_id"].(string)
				if ok {
					conflictIDs = append(conflictIDs, docID)
					conflictDocs[docID] = docs[i]
				}
			} else {
				slog.Error(fmt.Sprintf("Failed to insert document %s: %v", result.ID, result.Error))
			}
		}
	}

	// Retry conflicts with revisions
	if len(conflictIDs) > 0 {
		rows := db.AllDocs(ctx, kivik.Params(map[string]interface{}{
			"keys": conflictIDs,
		}))
		for rows.Next() {
			id, err := rows.ID()
			if err != nil {
				continue
			}
			var value struct {
				Rev string `json:"rev"`
			}
			if err := rows.ScanValue(&value); err == nil && value.Rev != "" {
				if doc, exists := conflictDocs[id]; exists {
					doc["_rev"] = value.Rev
				}
			}
		}
		_ = rows.Close()

		// Retry with revisions
		var retryDocs []interface{}
		for _, doc := range conflictDocs {
			if _, hasRev := doc["_rev"]; hasRev {
				retryDocs = append(retryDocs, doc)
			}
		}

		if len(retryDocs) > 0 {
			retryResults, err := db.BulkDocs(ctx, retryDocs)
			if err != nil {
				return fmt.Errorf("failed to bulk upsert conflicting documents: %w", err)
			}
			for _, result := range retryResults {
				if result.Error != nil {
					slog.Error(fmt.Sprintf("Failed to upsert document %s: %v", result.ID, result.Error))
				}
			}
		}
	}

	return nil
}

func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	db := c.client.DB(r.Msg.GetNamespace())

	var docs []map[string]interface{}
	for _, data := range r.Msg.GetData() {
		var doc map[string]interface{}
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("failed to unmarshal document: %w", err))
		}
		delete(doc, "_rev")
		docs = append(docs, doc)

		if c.settings.WriterMaxBatchSize > 0 && len(docs) >= c.settings.WriterMaxBatchSize {
			if err := c.writeDataBatch(ctx, db, docs); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			docs = nil
		}
	}

	if err := c.writeDataBatch(ctx, db, docs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	db := c.client.DB(r.Msg.GetNamespace())
	updates := r.Msg.GetUpdates()

	if len(updates) == 0 {
		return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
	}

	// Collect all document IDs to fetch revisions in batch
	var docIDs []string
	docIDSet := make(map[string]struct{})
	for _, update := range updates {
		if len(update.GetId()) == 0 {
			continue
		}
		docID := string(update.GetId()[0].GetData())
		if _, exists := docIDSet[docID]; !exists {
			docIDs = append(docIDs, docID)
			docIDSet[docID] = struct{}{}
		}
	}

	// Batch fetch all existing revisions using _all_docs with keys
	revMap := make(map[string]string)
	if len(docIDs) > 0 {
		rows := db.AllDocs(ctx, kivik.Params(map[string]interface{}{
			"keys": docIDs,
		}))
		for rows.Next() {
			id, err := rows.ID()
			if err != nil {
				continue
			}
			var value struct {
				Rev string `json:"rev"`
			}
			if err := rows.ScanValue(&value); err == nil && value.Rev != "" {
				revMap[id] = value.Rev
			}
		}
		_ = rows.Close()
	}

	// Prepare batch operations
	var upsertDocs []interface{}
	for _, update := range updates {
		if len(update.GetId()) == 0 {
			continue
		}

		docID := string(update.GetId()[0].GetData())

		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			var doc map[string]interface{}
			if err := json.Unmarshal(update.GetData(), &doc); err != nil {
				slog.Error(fmt.Sprintf("Failed to unmarshal update data: %v", err))
				continue
			}

			if rev, exists := revMap[docID]; exists {
				doc["_rev"] = rev
			} else {
				delete(doc, "_rev")
			}
			upsertDocs = append(upsertDocs, doc)

		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			if rev, exists := revMap[docID]; exists {
				upsertDocs = append(upsertDocs, map[string]interface{}{
					"_id":      docID,
					"_rev":     rev,
					"_deleted": true,
				})
			}
		}
	}

	// Execute bulk operation
	if len(upsertDocs) > 0 {
		results, err := db.BulkDocs(ctx, upsertDocs)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to bulk write documents: %w", err))
		}
		// Log any individual document errors
		for _, result := range results {
			if result.Error != nil {
				slog.Error(fmt.Sprintf("Failed to write document %s: %v", result.ID, result.Error))
			}
		}
	}

	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	namespaces := r.Msg.GetNamespaces()
	since := string(r.Msg.GetCursor())

	if len(namespaces) != 1 {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected exactly 1 namespace for streaming"))
	}

	ns := namespaces[0]
	db := c.client.DB(ns)

	params := map[string]interface{}{
		"feed":         "continuous",
		"include_docs": true,
		"heartbeat":    1000, // 1 second heartbeat
	}

	// Only set 'since' if we have a valid cursor, otherwise start from now
	if since != "" {
		params["since"] = since
	} else {
		params["since"] = "now"
	}

	changes := db.Changes(ctx, kivik.Params(params))
	defer changes.Close()

	for changes.Next() {
		seq := changes.Seq()
		id := changes.ID()
		idBytes := []byte(id)

		var update *adiomv1.Update

		if changes.Deleted() {
			update = &adiomv1.Update{
				Id: []*adiomv1.BsonValue{{
					Data: idBytes,
					Type: 2, // string type
					Name: "_id",
				}},
				Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
			}
		} else {
			var doc map[string]interface{}
			if err := changes.ScanDoc(&doc); err != nil {
				slog.Error(fmt.Sprintf("Failed to scan change doc: %v", err))
				continue
			}

			delete(doc, "_rev")

			jsonBytes, err := json.Marshal(doc)
			if err != nil {
				slog.Error(fmt.Sprintf("Failed to marshal change doc: %v", err))
				continue
			}

			update = &adiomv1.Update{
				Id: []*adiomv1.BsonValue{{
					Data: idBytes,
					Type: 2,
					Name: "_id",
				}},
				Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
				Data: jsonBytes,
			}
		}

		// Send each update immediately for continuous feed
		if err := s.Send(&adiomv1.StreamUpdatesResponse{
			Updates:    []*adiomv1.Update{update},
			Namespace:  ns,
			NextCursor: []byte(seq),
		}); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return connect.NewError(connect.CodeInternal, err)
		}
	}

	if err := changes.Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeInternal, err)
		}
	}

	return nil
}

func (c *conn) StreamLSN(ctx context.Context, r *connect.Request[adiomv1.StreamLSNRequest], s *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	namespaces := r.Msg.GetNamespaces()
	since := string(r.Msg.GetCursor())

	if len(namespaces) != 1 {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected exactly 1 namespace for LSN streaming"))
	}

	ns := namespaces[0]
	db := c.client.DB(ns)

	params := map[string]interface{}{
		"feed":      "continuous",
		"heartbeat": 10000,
		"timeout":   60000,
	}

	if since != "" {
		params["since"] = since
	} else {
		params["since"] = "now"
	}

	changes := db.Changes(ctx, kivik.Params(params))
	defer changes.Close()

	var lsn uint64
	for changes.Next() {
		lsn++
		seq := changes.Seq()

		if err := s.Send(&adiomv1.StreamLSNResponse{
			Lsn:        lsn,
			NextCursor: []byte(seq),
		}); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return connect.NewError(connect.CodeInternal, err)
		}
	}

	if err := changes.Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			return connect.NewError(connect.CodeInternal, err)
		}
	}

	return nil
}

func (c *conn) Teardown() {
	if c.client != nil {
		_ = c.client.Close()
	}
}

func NewConn(settings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	setDefault(&settings.ServerConnectTimeout, 10*time.Second)
	setDefault(&settings.PingTimeout, 5*time.Second)
	setDefault(&settings.TargetDocCountPerPartition, 50000)
	setDefault(&settings.MaxPageSize, 1000)

	dsn, err := convertUriToDSN(settings.Uri)
	if err != nil {
		return nil, fmt.Errorf("failed to convert URI to DSN: %w", err)
	}

	// In Kivik v4, authentication is handled via the DSN URL itself
	// (username:password in the URL) - no separate Authenticate call needed
	client, err := kivik.New("couch", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create CouchDB client: %w", err)
	}

	pingCtx, pingCancel := context.WithTimeout(context.Background(), settings.PingTimeout)
	defer pingCancel()

	if _, err := client.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("failed to ping CouchDB server: %w", err)
	}

	parsedURL, _ := url.Parse(dsn)
	baseURL := fmt.Sprintf("%s://%s", parsedURL.Scheme, parsedURL.Host)
	slog.Info(fmt.Sprintf("Connected to CouchDB at %s", baseURL))

	return &conn{
		client:   client,
		settings: settings,
		dsn:      dsn,
	}, nil
}
