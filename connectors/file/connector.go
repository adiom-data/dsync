/*
 * Copyright (C) 2025 Adiom, Inc.
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

package file

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"connectrpc.com/connect"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"go.mongodb.org/mongo-driver/bson"
)

var (
	ErrPathRequired      = errors.New("path is required")
	ErrUnsupportedType   = errors.New("unsupported data type for file connector")
	ErrUnsupportedFormat = errors.New("unsupported format (only csv is supported)")
	ErrMissingIDColumn   = errors.New("csv file must have an 'id' column")
	ErrInvalidDelimiter  = errors.New("delimiter must be a single character")
)

const (
	DefaultDelimiter = ','
	DefaultFormat    = "csv"
)

type ConnectorSettings struct {
	Uri       string
	Path      string
	Format    string
	Delimiter rune
}

type connector struct {
	adiomv1connect.UnimplementedConnectorServiceHandler

	settings ConnectorSettings
	isDir    bool

	writeMutex sync.Mutex
	writers    map[string]*csvFileWriter
}

type csvFileWriter struct {
	file   *os.File
	writer *csv.Writer
	header []string
}

func parseFileConnectionString(raw string) (string, error) {
	const prefix = "file://"
	if !strings.HasPrefix(strings.ToLower(raw), prefix) {
		return "", fmt.Errorf("invalid file connection string %q", raw)
	}
	path := raw[len(prefix):]
	if path == "" {
		return "", fmt.Errorf("missing path in %q", raw)
	}
	return path, nil
}

func (c *connector) fileExtension() string {
	return "." + strings.ToLower(c.settings.Format)
}

func (c *connector) pathToNamespace(basePath, filePath string) string {
	relPath, err := filepath.Rel(basePath, filePath)
	if err != nil {
		relPath = filepath.Base(filePath)
	}
	// Remove file extension
	relPath = strings.TrimSuffix(relPath, c.fileExtension())
	// Convert path separators to dots for namespace
	relPath = strings.ReplaceAll(relPath, string(filepath.Separator), ".")
	return relPath
}

func (c *connector) namespaceToPath(basePath, namespace string) string {
	relPath := strings.ReplaceAll(namespace, ".", string(filepath.Separator))
	return filepath.Join(basePath, relPath+c.fileExtension())
}

func NewConn(settings ConnectorSettings) (adiomv1connect.ConnectorServiceHandler, error) {
	path, err := parseFileConnectionString(settings.Uri)
	if err != nil {
		return nil, fmt.Errorf("bad uri format %v", settings.Uri)
	}
	settings.Path = path

	if settings.Path == "" {
		return nil, ErrPathRequired
	}

	if settings.Format == "" {
		settings.Format = DefaultFormat
	}
	if !strings.EqualFold(settings.Format, "csv") {
		return nil, ErrUnsupportedFormat
	}

	if settings.Delimiter == 0 {
		settings.Delimiter = DefaultDelimiter
	}

	info, err := os.Stat(settings.Path)
	isDir := false
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat path: %w", err)
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
				return err
			}
			if d.IsDir() {
				slog.Debug("skipping directory", "path", path, "dir", d)
				return nil
			}
			if !strings.HasSuffix(strings.ToLower(path), c.fileExtension()) {
				slog.Debug("skipping non-matching file", "path", path, "dir", d, "expectedExt", c.fileExtension())
				return nil
			}

			namespace := c.pathToNamespace(c.settings.Path, path)

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
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("walk directory: %w", err))
		}
	} else {
		namespace := c.pathToNamespace(filepath.Dir(c.settings.Path), c.settings.Path)

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
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = c.settings.Delimiter

	records, err := reader.ReadAll()
	if err != nil {
		return 0, err
	}

	if len(records) <= 1 {
		return 0, nil
	}
	return len(records) - 1, nil // subtract header
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
		path = c.namespaceToPath(c.settings.Path, namespace)
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

func (c *connector) ListData(ctx context.Context, req *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	part := req.Msg.GetPartition()
	if part == nil || len(part.GetCursor()) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partition cursor (file path) is required"))
	}

	path := string(part.GetCursor())

	file, err := os.Open(path)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("open file %s: %w", path, err))
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = c.settings.Delimiter

	records, err := reader.ReadAll()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("read csv from %s: %w", path, err))
	}

	if len(records) == 0 {
		return connect.NewResponse(&adiomv1.ListDataResponse{
			Data:       nil,
			NextCursor: nil,
		}), nil
	}

	header := records[0]

	var data [][]byte
	for _, row := range records[1:] {
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
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("marshal json in %s: %w", path, err))
			}
			data = append(data, jsonDoc)
		case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
			bsonDoc, err := bson.Marshal(doc)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("marshal bson in %s: %w", path, err))
			}
			data = append(data, bsonDoc)
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, ErrUnsupportedType)
		}
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       data,
		NextCursor: nil,
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
		path := c.namespaceToPath(c.settings.Path, namespace)

		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return fmt.Errorf("create directory: %w", err)
		}

		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create file %s: %w", path, err)
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
			if hasID {
				writer.header = append([]string{"id"}, header...)
			} else {
				writer.header = header
			}
			if err := writer.writer.Write(writer.header); err != nil {
				return fmt.Errorf("write header: %w", err)
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
			return fmt.Errorf("write row: %w", err)
		}
	}

	writer.writer.Flush()
	return writer.writer.Error()
}

func convertFromData(data []byte, dataType adiomv1.DataType) (map[string]interface{}, error) {
	switch dataType {
	case adiomv1.DataType_DATA_TYPE_JSON_ID:
		var doc map[string]interface{}
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal json: %w", err)
		}
		return doc, nil
	case adiomv1.DataType_DATA_TYPE_MONGO_BSON:
		var doc map[string]interface{}
		if err := bson.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("unmarshal bson: %w", err)
		}
		if idVal, ok := doc["_id"]; ok {
			doc["id"] = idVal
			delete(doc, "_id")
		}
		return doc, nil
	default:
		return nil, ErrUnsupportedType
	}
}

func (c *connector) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("updates not supported by file connector"))
}

func (c *connector) Teardown() {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	for _, writer := range c.writers {
		writer.writer.Flush()
		writer.file.Close()
	}
	c.writers = make(map[string]*csvFileWriter)
}
