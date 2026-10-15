package sqlbatch

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/sqlbatch/dialects"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/internal/gobregister"
	"github.com/microsoft/go-mssqldb/msdsn"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"
)

func init() {
	gobregister.RegisterCursorTypes()
}

type ChangeMapping struct {
	InitialCursorQuery string        // query to get the initial cursor
	Query              string        // query to get cols and cursor and update type
	Interval           time.Duration // how long to wait once no more items
}

type Mapping struct {
	Namespace      string
	CountQuery     string
	Query          string
	PartitionQuery string
	Limit          int
	DecodeJSON     []string
	decodeJSON     map[string]struct{}
	NoCount        bool

	Changes  []ChangeMapping
	Cols     []string
	Fetchers int
}

type SinkConfig struct {
	// WriteBatchSize is the connector-wide maximum number of compatible writes
	// combined into one statement. Values less than one use the built-in default.
	WriteBatchSize int
	// Tables optionally overrides WriteBatchSize for individual namespaces.
	Tables map[string]SinkTableConfig
}

type SinkTableConfig struct {
	WriteBatchSize int
}

func (c SinkConfig) writeBatchSize(namespace string) int {
	if table, ok := c.Tables[namespace]; ok && table.WriteBatchSize > 0 {
		return table.WriteBatchSize
	}
	return c.WriteBatchSize
}

type Config struct {
	ID               string
	Driver           string
	ConnectionString string
	Mappings         map[string]Mapping
	Sink             SinkConfig
}

type ConfigYml struct {
	ID               string
	Driver           string
	ConnectionString string
	Mappings         []Mapping
	Sink             SinkConfig
}

type SQLDialect interface {
	ListDataTemplate(cols []string, query string) string
	ListDataApplyTemplate(cols []string, template string, limit int, low []any, high []any) (string, []any)
	FetchTemplate(cols []string, query string) string
	Placeholder(i int) string
	KeySub(cols []string, count int) string
	InspectTable(context.Context, *sql.DB, string) (dialects.TableMetadata, error)
	ConvertJSONValue(dialects.ColumnMetadata, any) (any, error)
	ConvertDriverValue(dialects.ColumnMetadata, any) (any, error)
	UpsertQuery(namespace string, keys, columns, columnTypes, defaultColumns []string, count int, overrideIdentity bool) (string, error)
	DeleteQuery(namespace string, keys []string, count int) string
	MaxQueryParameters() int
}

func parseConfigFromFile(file string) (Config, error) {
	f, err := os.ReadFile(file) // #nosec G304
	if err != nil {
		return Config{}, err
	}
	var m ConfigYml
	if err := yaml.Unmarshal(f, &m); err != nil {
		return Config{}, fmt.Errorf("err parsing yml %w", err)
	}
	if m.Sink.WriteBatchSize < 0 {
		return Config{}, fmt.Errorf("sink write batch size cannot be negative: %v", m.Sink.WriteBatchSize)
	}
	for namespace, table := range m.Sink.Tables {
		if namespace == "" {
			return Config{}, fmt.Errorf("sink table namespace cannot be empty")
		}
		if table.WriteBatchSize < 1 {
			return Config{}, fmt.Errorf("sink table %v has invalid write batch size: %v", namespace, table.WriteBatchSize)
		}
	}
	res := map[string]Mapping{}
	for _, mapping := range m.Mappings {
		if mapping.Limit < 1 {
			return Config{}, fmt.Errorf("source mapping for %v has invalid or unspecified limit: %v", mapping.Namespace, mapping.Limit)
		}
		if len(mapping.DecodeJSON) > 0 {
			mapping.decodeJSON = map[string]struct{}{}
			for _, v := range mapping.DecodeJSON {
				mapping.decodeJSON[v] = struct{}{}
			}
		}
		if len(mapping.CountQuery) == 0 {
			mapping.CountQuery = "WITH QUERY AS (" + mapping.Query + ") SELECT COUNT(*) from QUERY"
		}
		res[mapping.Namespace] = mapping
	}
	return Config{
		ID:               m.ID,
		Driver:           m.Driver,
		ConnectionString: m.ConnectionString,
		Mappings:         res,
		Sink:             m.Sink,
	}, nil
}

func (c *conn) sourceMapping(namespace string) (Mapping, error) {
	mapping, ok := c.config.Mappings[namespace]
	if !ok {
		return Mapping{}, fmt.Errorf("no source mapping for %v", namespace)
	}
	if mapping.Limit < 1 {
		return Mapping{}, fmt.Errorf("source mapping for %v has invalid or unspecified limit: %v", namespace, mapping.Limit)
	}
	return mapping, nil
}

type updateCursor struct {
	Cursor []any
}

func (c *updateCursor) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeUpdateCursor(in []byte) (*updateCursor, error) {
	if len(in) == 0 {
		return &updateCursor{}, nil
	}
	var c updateCursor
	br := bytes.NewReader(in)
	dec := gob.NewDecoder(br)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

type partitionCursor struct {
	Cols []string
	Low  []any
	High []any
}

func (c *partitionCursor) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodePartitionCursor(in []byte) (*partitionCursor, error) {
	if len(in) == 0 {
		return &partitionCursor{}, nil
	}
	var c partitionCursor
	br := bytes.NewReader(in)
	dec := gob.NewDecoder(br)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

type conn struct {
	c            *sql.DB
	config       Config
	dialect      SQLDialect
	metadataMu   sync.Mutex
	sinkMetadata map[string]dialects.TableMetadata
}

// GetByIds implements [adiomv1connect.ConnectorServiceHandler].
func (c *conn) GetByIds(context.Context, *connect.Request[adiomv1.GetByIdsRequest]) (*connect.Response[adiomv1.GetByIdsResponse], error) {
	panic("unimplemented")
}

func ScanRow(rows *sql.Rows, numCols int) ([]any, error) {
	values := make([]any, numCols)
	valuePtrs := make([]any, numCols)
	for i := range values {
		valuePtrs[i] = &values[i]
	}
	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}
	return values, nil
}

func convertDriverValues(cols []string, columnTypes []*sql.ColumnType, values []any, dialect SQLDialect) error {
	if len(columnTypes) != len(cols) || len(values) != len(cols) {
		return fmt.Errorf("column metadata has %d entries and row has %d values for %d columns", len(columnTypes), len(values), len(cols))
	}
	for i, col := range cols {
		converted, err := dialect.ConvertDriverValue(dialects.ColumnMetadata{
			Name:         col,
			DatabaseType: columnTypes[i].DatabaseTypeName(),
		}, values[i])
		if err != nil {
			return fmt.Errorf("convert column %q: %w", col, err)
		}
		values[i] = converted
	}
	return nil
}

func scanRowWithTypes(cols []string, columnTypes []*sql.ColumnType, rows *sql.Rows, dialect SQLDialect) ([]any, error) {
	values, err := ScanRow(rows, len(cols))
	if err != nil {
		return nil, err
	}
	if err := convertDriverValues(cols, columnTypes, values, dialect); err != nil {
		return nil, err
	}
	return values, nil
}

func ParseRow(cols []string, columnTypes []*sql.ColumnType, rows *sql.Rows, mapping *Mapping, dialect SQLDialect) (map[string]any, error) {
	m := make(map[string]any, len(cols))
	parsedRow, err := scanRowWithTypes(cols, columnTypes, rows, dialect)
	if err != nil {
		return nil, fmt.Errorf("err in scan row: %w", err)
	}
	for i, col := range cols {
		if _, ok := mapping.decodeJSON[col]; ok {
			if parsedRow[i] == nil {
				m[col] = parsedRow[i]
				continue
			}
			var reader io.Reader
			switch rowV := parsedRow[i].(type) {
			case string:
				reader = strings.NewReader(rowV)
			case []byte:
				reader = bytes.NewReader(rowV)
			default:
				return nil, fmt.Errorf("decodejson column %v not supported, got %T", col, parsedRow[i])
			}
			decoder := json.NewDecoder(reader)
			decoder.UseNumber()
			var m2 any
			if err := decoder.Decode(&m2); err != nil {
				return nil, fmt.Errorf("decodejson column %v could not be parsed %w", col, err)
			}
			m[col] = m2
		} else {
			m[col] = parsedRow[i]
		}
	}
	return m, nil
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	var allPartitions []*adiomv1.Partition
	namespaces := r.Msg.GetNamespaces()
	if len(namespaces) == 0 {
		namespaces = []string{}
		for namespace := range c.config.Mappings {
			namespaces = append(namespaces, namespace)
		}
	}
	if r.Msg.InitialSync {
		for _, namespace := range namespaces {
			var partitions []*adiomv1.Partition
			metadataResp, err := c.GetNamespaceMetadata(ctx, connect.NewRequest(&adiomv1.GetNamespaceMetadataRequest{Namespace: namespace}))
			if err != nil {
				return nil, err
			}
			estimatedCount := metadataResp.Msg.GetCount()
			mapping, err := c.sourceMapping(namespace)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
			rows, err := c.c.QueryContext(ctx, mapping.PartitionQuery)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err partition query: %w %v", err, mapping.PartitionQuery))
			}
			cols, err := rows.Columns()
			if err != nil {
				_ = rows.Close()
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err columns: %w", err))
			}
			columnTypes, err := rows.ColumnTypes()
			if err != nil {
				_ = rows.Close()
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err column types: %w", err))
			}
			var low []any
			for rows.Next() {
				parsedRow, err := scanRowWithTypes(cols, columnTypes, rows, c.dialect)
				if err != nil {
					_ = rows.Close()
					return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err scanning row: %w", err))
				}
				cursor := partitionCursor{
					Cols: cols,
					Low:  low,
					High: parsedRow,
				}
				encodedCursor, err := cursor.Encode()
				if err != nil {
					_ = rows.Close()
					return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err encoding cursor: %w", err))
				}
				slog.Debug("Partition", "namespace", namespace, "cursor", cursor)
				partitions = append(partitions, &adiomv1.Partition{
					Namespace:      namespace,
					Cursor:         encodedCursor,
					EstimatedCount: 0,
				})
				low = parsedRow
			}
			if rows.Err() != nil {
				_ = rows.Close()
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err in generate plan: %w", rows.Err()))
			}
			cursor := partitionCursor{
				Cols: cols,
				Low:  low,
			}
			encodedCursor, err := cursor.Encode()
			if err != nil {
				_ = rows.Close()
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err encoding cursor: %w", err))
			}
			slog.Debug("Partition", "namespace", namespace, "cursor", cursor)
			partitions = append(partitions, &adiomv1.Partition{
				Namespace:      namespace,
				Cursor:         encodedCursor,
				EstimatedCount: 0,
			})
			_ = rows.Close()
			if estimatedCount > 0 {
				for _, partition := range partitions {
					partition.EstimatedCount = estimatedCount / uint64(len(partitions))
					if partition.EstimatedCount == 0 {
						partition.EstimatedCount = 1
					}
				}
			}
			tmpl := c.dialect.ListDataTemplate(cols, mapping.Query)
			query, subs := c.dialect.ListDataApplyTemplate(cols, tmpl, 0, nil, nil)
			slog.Debug("Checking query", "query", query, "subs", subs)
			rows2, err := c.c.QueryContext(ctx, query, subs...)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err checking query: %w", err))
			}
			cols2, err := rows2.Columns()
			if err != nil {
				_ = rows2.Close()
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err getting columns on query: %w", err))
			}
			_ = rows2.Close()
			for _, col := range cols {
				// We expect cols to be small
				if !slices.Contains(cols2, col) {
					return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("query column missing %v, (partition columns: %v , query columns: %v)", col, cols, cols2))
				}
			}
			allPartitions = append(allPartitions, partitions...)
		}
	}
	var updatePartitions []*adiomv1.UpdatesPartition
	if r.Msg.Updates {
		for _, namespace := range namespaces {
			mapping, err := c.sourceMapping(namespace)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			}
			if len(mapping.Changes) == 0 {
				continue
			}
			updateCursor, err := GetUpdateCursor(ctx, c.c, &mapping, c.dialect)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to get update cursor for %v", namespace))
			}
			cursor, err := updateCursor.Encode()
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to encode update cursor for %v", namespace))
			}
			updatePartitions = append(updatePartitions, &adiomv1.UpdatesPartition{
				Namespaces: []string{namespace},
				Cursor:     cursor,
			})
		}
		// DummyPartition
		if len(updatePartitions) == 0 {
			updatePartitions = append(updatePartitions, &adiomv1.UpdatesPartition{})
		}
	}
	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        allPartitions,
		UpdatesPartitions: updatePartitions,
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	id := c.config.ID
	if id == "" {
		id = "sql"
	}
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     id,
		DbType: "sql",
		Capabilities: &adiomv1.Capabilities{
			Source: &adiomv1.Capabilities_Source{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_JSON_ID},
				MultiNamespacePlan: true,
				DefaultPlan:        true,
			},
			Sink: &adiomv1.Capabilities_Sink{
				SupportedDataTypes: []adiomv1.DataType{adiomv1.DataType_DATA_TYPE_JSON_ID},
			},
		},
	}), nil
}

// GetNamespaceMetadata implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetNamespaceMetadata(ctx context.Context, r *connect.Request[adiomv1.GetNamespaceMetadataRequest]) (*connect.Response[adiomv1.GetNamespaceMetadataResponse], error) {
	mapping, err := c.sourceMapping(r.Msg.GetNamespace())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	if mapping.NoCount {
		return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{}), nil
	}

	rows, err := c.c.QueryContext(ctx, mapping.CountQuery)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer rows.Close()
	var count uint64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	} else {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err couldn't get a count"))
	}
	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{Count: count}), nil
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	partition := r.Msg.GetPartition()
	namespace := partition.GetNamespace()
	rawCursor := r.Msg.GetCursor()
	if rawCursor == nil {
		rawCursor = partition.GetCursor()
	}
	cursor, err := DecodePartitionCursor(rawCursor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error decoding cursor"))
	}
	mapping, err := c.sourceMapping(namespace)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	template := c.dialect.ListDataTemplate(cursor.Cols, mapping.Query)
	query, subs := c.dialect.ListDataApplyTemplate(cursor.Cols, template, mapping.Limit+1, cursor.Low, cursor.High)

	slog.Debug("query", "query", query, "subs", subs)
	rows, err := c.c.QueryContext(ctx, query, subs...)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err querying: %v ; %w", query, err))
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err getting columns: %w", err))
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err getting column types: %w", err))
	}
	var data [][]byte
	var count int
	var nextLow []any
	for rows.Next() {
		count += 1
		m, err := ParseRow(cols, columnTypes, rows, &mapping, c.dialect)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err in parse row: %w", err))
		}
		slog.Debug("row", "data", m, "count", count)

		if count > mapping.Limit {
			for _, col := range cursor.Cols {
				nextLow = append(nextLow, m[col])
			}
			break
		}
		newData, err := json.Marshal(m)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err in json marshal: %w", err))
		}
		data = append(data, newData)
	}
	if rows.Err() != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("err in list data: %w", rows.Err()))
	}

	var nextCursor []byte
	var next partitionCursor
	if nextLow != nil {
		next = *cursor
		next.Low = nextLow
		nextCursor, err = next.Encode()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
	slog.Debug("list data", "data-size", len(data), "cursor", cursor, "next-cursor", next)

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       data,
		NextCursor: nextCursor,
	}), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, r *connect.Request[adiomv1.StreamUpdatesRequest], s *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	if len(r.Msg.GetNamespaces()) != 1 {
		return nil
	}
	cursor, err := DecodeUpdateCursor(r.Msg.GetCursor())
	if err != nil {
		return connect.NewError(connect.CodeInternal, fmt.Errorf("err decoding cursor: %w", err))
	}
	mapping, err := c.sourceMapping(r.Msg.GetNamespaces()[0])
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, err)
	}
	ch := make(chan *adiomv1.StreamUpdatesResponse)

	if len(mapping.Changes) == 0 {
		slog.Warn("No change stream configuration", "namespace", r.Msg.GetNamespaces()[0])
		return nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(ch)
		return Poller(ctx, *cursor, &mapping, c.c, c.dialect, ch)
	})
	eg.Go(func() error {
		for {
			select {
			case result, ok := <-ch:
				if !ok {
					return nil
				}
				if err := s.Send(result); err != nil {
					return fmt.Errorf("err in send: %w", err)
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	if err := eg.Wait(); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	return nil
}

func (c *conn) Teardown() {
	_ = c.c.Close()
}

func NewConn(cfgFile string) (*conn, error) {
	config, err := parseConfigFromFile(cfgFile)
	if err != nil {
		return nil, err
	}
	var dialect SQLDialect
	switch config.Driver {
	case "sqlserver":
		dsn, err := msdsn.Parse(config.ConnectionString)
		if err != nil {
			return nil, fmt.Errorf("parse SQL Server connection string: %w", err)
		}
		dialect = dialects.SQLServer{GuidConversion: dsn.Encoding.GuidConversion}
	case "postgres", "pgx":
		config.Driver = "pgx"
		dialect = dialects.Postgres{}
	case "oracle":
		dialect = dialects.Oracle{}
	default:
		return nil, fmt.Errorf("unsupported dialect %v", config.Driver)
	}

	db, err := sql.Open(config.Driver, config.ConnectionString)
	if err != nil {
		return nil, err
	}

	return &conn{
		c:       db,
		config:  config,
		dialect: dialect,
	}, nil
}

var _ adiomv1connect.ConnectorServiceHandler = &conn{}
