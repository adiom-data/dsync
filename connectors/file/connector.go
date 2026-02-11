/*
 * Copyright (C) 2025 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

//gosec:disable G304 -- This is a false positive

package file

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	ErrPathRequired      = errors.New("file path is required in connection URI (e.g., file:///path/to/dir)")
	ErrUnsupportedType   = errors.New("unsupported data type: file connector only supports JSON_ID and MONGO_BSON")
	ErrUnsupportedFormat = errors.New("unsupported format: file connector currently only supports 'csv' format")
	ErrMissingIDColumn   = errors.New("CSV file must have an 'id' column as the document identifier")
	ErrInvalidDelimiter  = errors.New("invalid delimiter: must be a single character (e.g., ',' or ';')")
)

const (
	DefaultDelimiter = ','
	DefaultFormat    = "csv"
	DefaultBatchSize = 10000
)

type ConnectorSettings struct {
	Uri       string
	Path      string
	Format    string
	Delimiter rune
	BatchSize int
}

type connector struct {
	adiomv1connect.UnimplementedConnectorServiceHandler

	settings ConnectorSettings
	isDir    bool

	writeMutex sync.Mutex
	writers    map[string]*csvFileWriter
}

type csvFileWriter struct {
	file      *os.File
	writer    *csv.Writer
	header    []string
	headerSet map[string]bool
}

func (c *connector) fileExtension() string {
	return "." + strings.ToLower(c.settings.Format)
}

func NewConn(settings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	path, err := parseFileConnectionString(settings.Uri)
	if err != nil {
		return nil, fmt.Errorf("bad uri format %v (%v)", settings.Uri, err)
	}
	settings.Path = path

	if settings.Path == "" {
		return nil, ErrPathRequired
	}

	if settings.Format == "" {
		settings.Format = DefaultFormat
	}
	if !strings.EqualFold(settings.Format, "csv") {
		return nil, fmt.Errorf("%w: got %q", ErrUnsupportedFormat, settings.Format)
	}

	if settings.Delimiter == 0 {
		settings.Delimiter = DefaultDelimiter
	}

	if settings.BatchSize <= 0 {
		settings.BatchSize = DefaultBatchSize
	}

	info, err := os.Stat(settings.Path)
	isDir := false
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to access path %q: %w", settings.Path, err)
		}
		// Path doesn't exist - treat as directory for sink operations
		isDir = true
	} else {
		isDir = info.IsDir()
	}

	return &connector{
		settings: settings,
		isDir:    isDir,
		writers:  make(map[string]*csvFileWriter),
	}, nil
}

func (c *connector) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		DbType: "file",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{
					adiomv1.DataType_DATA_TYPE_JSON_ID,
					adiomv1.DataType_DATA_TYPE_MONGO_BSON,
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

func (c *connector) GeneratePlan(ctx context.Context, req *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
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

	if c.isDir {
		err := filepath.WalkDir(c.settings.Path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return fmt.Errorf("failed to access %q: %w", path, err)
			}
			if d.IsDir() {
				slog.Debug("skipping directory", "path", path, "dir", d)
				return nil
			}
			if !strings.HasSuffix(strings.ToLower(path), c.fileExtension()) {
				slog.Debug("skipping non-matching file", "path", path, "dir", d, "expectedExt", c.fileExtension())
				return nil
			}

			namespace := pathToNamespace(c.settings.Path, path, c.fileExtension())

			if filterByNamespace {
				if _, ok := requestedNamespaces[namespace]; !ok {
					return nil
				}
			}

			count, err := c.countRecords(path)
			if err != nil {
				slog.Warn("failed to count records in file", "path", path, "err", err)
			}

			partitions = append(partitions, &adiomv1.Partition{
				Namespace:      namespace,
				Cursor:         []byte(path),
				EstimatedCount: uint64(count),
			})

			return nil
		})
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to scan directory %q for CSV files: %w", c.settings.Path, err))
		}
	} else {
		namespace := pathToNamespace(filepath.Dir(c.settings.Path), c.settings.Path, c.fileExtension())

		if filterByNamespace {
			if _, ok := requestedNamespaces[namespace]; !ok {
				return connect.NewResponse(&adiomv1.GeneratePlanResponse{
					Partitions: nil,
				}), nil
			}
		}

		count, err := c.countRecords(c.settings.Path)
		if err != nil {
			slog.Warn("failed to count records in file", "path", c.settings.Path, "err", err)
		}

		partitions = append(partitions, &adiomv1.Partition{
			Namespace:      namespace,
			Cursor:         []byte(c.settings.Path),
			EstimatedCount: uint64(count),
		})
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        partitions,
		UpdatesPartitions: nil,
	}), nil
}

func (c *connector) countRecords(path string) (int, error) {
	file, err := os.Open(path) //nolint:gosec // G304: path is intentionally user-provided for file connector
	if err != nil {
		return 0, err
	}
	defer file.Close()

	count, err := countCSVRows(file)
	if err != nil {
		return 0, err
	}

	if count <= 1 {
		return 0, nil
	}
	return count - 1, nil // subtract header
}

func (c *connector) GetNamespaceMetadata(ctx context.Context, req *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	namespace := req.Msg.GetNamespace()
	if namespace == "" {
		return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
			Count: 0,
		}), nil
	}

	var path string
	if c.isDir {
		path = namespaceToPath(c.settings.Path, namespace, c.fileExtension())
	} else {
		path = c.settings.Path
	}

	count, err := c.countRecords(path)
	if err != nil {
		slog.Warn("failed to count records in file", "path", path, "err", err)
		return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
			Count: 0,
		}), nil
	}

	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(count),
	}), nil
}

type listDataCursor struct {
	Path       string   `json:"path"`
	ByteOffset int64    `json:"byteOffset"`
	Header     []string `json:"header"`
}

func (c *connector) ListData(ctx context.Context, req *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	part := req.Msg.GetPartition()
	if part == nil || len(part.GetCursor()) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partition cursor is required: must contain the file path"))
	}

	// Parse cursor - it's either a plain path (initial) or JSON with path+offset (pagination)
	var cursor listDataCursor
	if err := json.Unmarshal(req.Msg.GetCursor(), &cursor); err != nil || cursor.Path == "" {
		// Initial request - cursor from partition is just the path
		cursor = listDataCursor{
			Path:       string(part.GetCursor()),
			ByteOffset: 0,
			Header:     nil,
		}
	}

	file, err := os.Open(cursor.Path) //nolint:gosec // G304: path is intentionally user-provided for file connector
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to open CSV file %q: %w", cursor.Path, err))
	}
	defer file.Close()

	// Seek to byte offset for pagination
	if cursor.ByteOffset > 0 {
		if _, err := file.Seek(cursor.ByteOffset, io.SeekStart); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to seek to offset %d in %q: %w", cursor.ByteOffset, cursor.Path, err))
		}
	}

	reader := csv.NewReader(file)
	reader.Comma = c.settings.Delimiter

	// For initial request, read header from file
	// For pagination, use cached header (file is already positioned past header)
	header := cursor.Header
	if header == nil {
		var err error
		header, err = reader.Read()
		if err != nil {
			if err == io.EOF {
				return connect.NewResponse(&adiomv1.ListDataResponse{
					Data:       nil,
					NextCursor: nil,
				}), nil
			}
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to read CSV header from %q: %w", cursor.Path, err))
		}
	}

	// Read batch
	var data [][]byte
	rowsRead := 0
	hitEOF := false
	for rowsRead < c.settings.BatchSize {
		row, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				hitEOF = true
				break
			}
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to read CSV row from %q: %w", cursor.Path, err))
		}
		rowsRead++

		doc := make(map[string]interface{})
		for i, col := range header {
			if i < len(row) {
				doc[col] = row[i]
			}
		}

		switch req.Msg.GetType() {
		case adiomv1.DataType_DATA_TYPE_JSON_ID:
			jsonDoc, err := json.Marshal(doc)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to convert CSV row to JSON in file %q: %w", cursor.Path, err))
			}
			data = append(data, jsonDoc)
		case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
			bsonDoc, err := bson.Marshal(doc)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to convert CSV row to BSON in file %q: %w", cursor.Path, err))
			}
			data = append(data, bsonDoc)
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%w: requested type %v", ErrUnsupportedType, req.Msg.GetType()))
		}
	}

	// Build next cursor if there's more data
	// Use InputOffset() to get precise byte position after the last read row
	var nextCursor []byte
	if !hitEOF {
		nextCursor, _ = json.Marshal(listDataCursor{
			Path:       cursor.Path,
			ByteOffset: cursor.ByteOffset + reader.InputOffset(),
			Header:     header,
		})
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       data,
		NextCursor: nextCursor,
	}), nil
}

func (c *connector) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func (c *connector) StreamUpdates(context.Context, *connect.Request[adiomv1.StreamUpdatesRequest], *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	return connect.NewError(connect.CodeUnimplemented, errors.ErrUnsupported)
}

func (c *connector) WriteData(ctx context.Context, req *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	slog.Debug("received write data request", "namespace", req.Msg.GetNamespace(), "numDocs", len(req.Msg.GetData()))

	if len(req.Msg.GetData()) == 0 {
		return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
	}

	namespace := req.Msg.GetNamespace()
	if namespace == "" {
		namespace = "default"
	}

	docs := make([]map[string]interface{}, 0, len(req.Msg.GetData()))
	for _, data := range req.Msg.GetData() {
		doc, err := convertFromData(data, req.Msg.GetType())
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		docs = append(docs, doc)
	}

	if err := c.writeCSV(namespace, docs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

func (c *connector) writeCSV(namespace string, docs []map[string]interface{}) error {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	writer, ok := c.writers[namespace]
	if !ok {
		path := namespaceToPath(c.settings.Path, namespace, c.fileExtension())

		if err := os.MkdirAll(filepath.Dir(path), 0750); err != nil {
			return fmt.Errorf("failed to create directory for namespace %q: %w", namespace, err)
		}

		file, err := os.Create(path) //nolint:gosec // G304: path is intentionally user-provided for file connector
		if err != nil {
			return fmt.Errorf("failed to create CSV file %q for namespace %q: %w", path, namespace, err)
		}

		csvWriter := csv.NewWriter(file)
		csvWriter.Comma = c.settings.Delimiter

		writer = &csvFileWriter{
			file:   file,
			writer: csvWriter,
			header: nil,
		}
		c.writers[namespace] = writer
	}

	for _, doc := range docs {
		if writer.header == nil {
			header := make([]string, 0, len(doc))
			hasID := false
			for k := range doc {
				if k == "id" || k == "_id" {
					hasID = true
					continue
				}
				header = append(header, k)
			}
			sort.Strings(header)
			if hasID {
				writer.header = append([]string{"id"}, header...)
			} else {
				writer.header = header
			}
			// Build header set for fast lookup
			writer.headerSet = make(map[string]bool, len(writer.header))
			for _, col := range writer.header {
				writer.headerSet[col] = true
			}
			if err := writer.writer.Write(writer.header); err != nil {
				return fmt.Errorf("failed to write CSV header for namespace %q: %w", namespace, err)
			}
		}

		// Check for extra keys not in header
		for k := range doc {
			normalizedKey := k
			if k == "_id" {
				normalizedKey = "id"
			}
			if writer.headerSet[normalizedKey] == false {
				slog.Warn("document has key not in CSV header, data will be lost",
					"namespace", namespace,
					"key", k,
					"header", writer.header)
			}
		}

		row := make([]string, len(writer.header))
		for i, col := range writer.header {
			var val interface{}
			if col == "id" {
				if v, ok := doc["id"]; ok {
					val = v
				} else if v, ok := doc["_id"]; ok {
					val = v
				}
			} else {
				val = doc[col]
			}
			if val != nil {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		if err := writer.writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row for namespace %q: %w", namespace, err)
		}
	}

	writer.writer.Flush()
	return writer.writer.Error()
}
func (c *connector) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("updates not supported by file connector"))
}

func (c *connector) Teardown() {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	for ns, writer := range c.writers {
		writer.writer.Flush()
		if err := writer.file.Close(); err != nil {
			slog.Warn("failed to close CSV file", "namespace", ns, "err", err)
		}
	}
	c.writers = make(map[string]*csvFileWriter)
}
