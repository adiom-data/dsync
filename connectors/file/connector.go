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
)

var (
	ErrPathRequired      = errors.New("file path is required in connection URI (e.g., file:///path/to/dir)")
	ErrUnsupportedType   = errors.New("unsupported data type: file connector only supports JSON_ID and MONGO_BSON")
	ErrUnsupportedFormat = errors.New("unsupported format: file connector currently only supports 'csv' format")
	ErrMissingIDColumn   = errors.New("CSV file must have an 'id' column as the document identifier")
	ErrInvalidDelimiter  = errors.New("invalid delimiter: must be a single character (e.g., ',' or ';')")
	ErrDuplicateColumn   = errors.New("CSV header has a duplicate column name")
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
	// DisableTypeInference loads every CSV value as a string instead of
	// inferring numbers, booleans, JSON values, ObjectIDs and dates.
	DisableTypeInference bool
}

type connector struct {
	adiomv1connect.UnimplementedConnectorServiceHandler

	settings ConnectorSettings
	isDir    bool

	writeMutex sync.Mutex
	writers    map[string]*csvFileWriter
}

type csvFileWriter struct {
	path      string
	file      *os.File
	writer    *csv.Writer
	header    []string
	headerSet map[string]bool
	// writtenHeaderLen is the number of columns in the header line at the top of
	// the file. When later documents add columns, the file is rewritten with the
	// full header on Teardown.
	writtenHeaderLen int
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
	// Row widths are checked against the header below (not against the first row
	// of the page): short rows are padded, longer rows are rejected.
	reader.FieldsPerRecord = -1

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
		if len(header) > 0 {
			header[0] = strings.TrimPrefix(header[0], "\ufeff") // UTF-8 BOM, e.g. from Excel
		}
	}

	decoder, err := newRowDecoder(header, req.Msg.GetType(), !c.settings.DisableTypeInference)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%w (file %q)", err, cursor.Path))
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
		if len(row) > len(header) {
			line, _ := reader.FieldPos(0)
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("CSV row in %q (line %d of this read) has %d fields but the header has %d", cursor.Path, line, len(row), len(header)))
		}

		doc, err := decoder.decode(row)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to convert CSV row in file %q: %w", cursor.Path, err))
		}
		data = append(data, doc)
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

	docs := make([]encodedDoc, 0, len(req.Msg.GetData()))
	for _, data := range req.Msg.GetData() {
		doc, err := encodeDocument(data, req.Msg.GetType())
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err)
		}
		docs = append(docs, doc)
	}

	if err := c.writeCSV(namespace, idColumnFor(req.Msg.GetType()), docs); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// writeCSV appends documents to the namespace's CSV file. The identifier column
// comes first, followed by the first document's keys in sorted order. Keys that
// first appear in later documents become new columns at the end; rows written
// before that are padded when the file is finalized on Teardown.
func (c *connector) writeCSV(namespace string, idColumn string, docs []encodedDoc) error {
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
			path:      path,
			file:      file,
			writer:    csvWriter,
			headerSet: map[string]bool{},
		}
		c.writers[namespace] = writer
	}

	for _, doc := range docs {
		var newKeys []string
		for k := range doc {
			if !writer.headerSet[k] && k != idColumn {
				newKeys = append(newKeys, k)
			}
		}
		sort.Strings(newKeys)
		if _, hasID := doc[idColumn]; hasID && !writer.headerSet[idColumn] {
			newKeys = append([]string{idColumn}, newKeys...)
		}
		for _, k := range newKeys {
			writer.header = append(writer.header, k)
			writer.headerSet[k] = true
		}

		if len(writer.header) == 0 {
			// An empty document has no CSV representation (blank lines are skipped by readers).
			slog.Warn("skipping empty document: it has no fields to write as a CSV row", "namespace", namespace)
			continue
		}

		if writer.writtenHeaderLen == 0 {
			if err := writeRecord(writer.writer, writer.file, writer.header); err != nil {
				return fmt.Errorf("failed to write CSV header for namespace %q: %w", namespace, err)
			}
			writer.writtenHeaderLen = len(writer.header)
		} else if len(newKeys) > 0 {
			slog.Debug("new CSV columns found; the file will be rewritten with the full header at the end",
				"namespace", namespace, "columns", newKeys)
		}

		row := make([]string, len(writer.header))
		for i, col := range writer.header {
			row[i] = doc[col]
		}
		if err := writeRecord(writer.writer, writer.file, row); err != nil {
			return fmt.Errorf("failed to write CSV row for namespace %q: %w", namespace, err)
		}
	}

	writer.writer.Flush()
	return writer.writer.Error()
}

// writeRecord writes one CSV record. csv.Writer writes a record made of a single
// empty field as a blank line, which readers skip; write it as "" instead.
func writeRecord(w *csv.Writer, raw io.Writer, rec []string) error {
	if len(rec) == 1 && rec[0] == "" {
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
		_, err := io.WriteString(raw, "\"\"\n")
		return err
	}
	return w.Write(rec)
}

// finalize flushes and closes the file, rewriting it when columns were added
// after the header line was written so that every row has the full header.
func (w *csvFileWriter) finalize(delimiter rune) error {
	w.writer.Flush()
	flushErr := w.writer.Error()
	closeErr := w.file.Close()
	if flushErr != nil {
		return flushErr
	}
	if closeErr != nil {
		return closeErr
	}
	if len(w.header) == w.writtenHeaderLen {
		return nil
	}

	slog.Info("rewriting CSV file with the full header", "path", w.path,
		"columns", len(w.header), "addedColumns", w.header[w.writtenHeaderLen:])

	src, err := os.Open(w.path) //nolint:gosec // G304: path is intentionally user-provided for file connector
	if err != nil {
		return err
	}
	defer src.Close()

	info, err := src.Stat()
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(w.path), filepath.Base(w.path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		if tmpPath != "" {
			_ = tmp.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	reader := csv.NewReader(src)
	reader.Comma = delimiter
	reader.FieldsPerRecord = -1 // rows written before a column was added are shorter
	reader.ReuseRecord = true
	out := csv.NewWriter(tmp)
	out.Comma = delimiter

	if _, err := reader.Read(); err != nil { // old header
		return fmt.Errorf("failed to read CSV header: %w", err)
	}
	if err := writeRecord(out, tmp, w.header); err != nil {
		return err
	}
	row := make([]string, len(w.header))
	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV row: %w", err)
		}
		if len(rec) > len(w.header) {
			return fmt.Errorf("CSV row has %d fields, more than the %d header columns", len(rec), len(w.header))
		}
		n := copy(row, rec)
		clear(row[n:])
		if err := writeRecord(out, tmp, row); err != nil {
			return err
		}
	}
	out.Flush()
	if err := out.Error(); err != nil {
		return err
	}
	if err := tmp.Chmod(info.Mode().Perm()); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		return err
	}
	tmpPath = ""
	return nil
}

func (c *connector) WriteUpdates(context.Context, *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	return nil, connect.NewError(connect.CodeUnimplemented, fmt.Errorf("updates not supported by file connector"))
}

func (c *connector) Teardown() {
	c.writeMutex.Lock()
	defer c.writeMutex.Unlock()

	for ns, writer := range c.writers {
		if err := writer.finalize(c.settings.Delimiter); err != nil {
			slog.Error("failed to finalize CSV file; it may be missing columns in the header or have rows of uneven length",
				"namespace", ns, "path", writer.path, "err", err)
		}
	}
	c.writers = make(map[string]*csvFileWriter)
}
