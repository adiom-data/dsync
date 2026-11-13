package postgres

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/adiom-data/dsync/internal/gobregister"
	"github.com/cespare/xxhash"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/sync/errgroup"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
)

type PostgresSettings struct {
	URL string

	Force           bool
	SlotName        string
	PublicationName string

	Limit int

	StreamMaxBatchWait time.Duration // (soft) force a stream flush of the batch after this time has passed
	StreamMaxBatchSize int           // (soft) force a stream flush when reaching this batch size
	StreamFlushDelay   time.Duration

	EstimatedCountThreshold    int64
	TargetDocCountPerPartition int64

	// When true, sets session_replication_role = 'replica' for sink operations
	// This disables triggers and rules, useful when replicating data
	EnableReplicaMode bool
}

func init() {
	gobregister.RegisterCursorTypes()
}

type conn struct {
	adiomv1connect.UnimplementedConnectorServiceHandler
	replicationUrl string
	id             uint64
	c              *pgxpool.Pool

	pkeys map[string][]string
	// sinkMetadata is a connection-time snapshot. Schemas are expected to stay
	// unchanged for the lifetime of the connector.
	sinkMetadata map[string]sinkTableMetadata
	hasIDColumn  map[string]bool
	idAliases    map[string]string
	settings     PostgresSettings
	bsonRegistry *bson.Registry
}

type batchSender interface {
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

func (c *conn) sendSinkBatch(ctx context.Context, batch *pgx.Batch, count int, onError func(int, error)) error {
	var sender batchSender = c.c
	var tx pgx.Tx
	if c.settings.EnableReplicaMode {
		var err error
		tx, err = c.c.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, "SET LOCAL session_replication_role = 'replica'"); err != nil {
			return fmt.Errorf("failed to set session_replication_role: %w", err)
		}
		sender = tx
	}

	results := sender.SendBatch(ctx, batch)
	for i := 0; i < count; i++ {
		if _, err := results.Exec(); err != nil {
			onError(i, err)
			_ = results.Close()
			return err
		}
	}
	if err := results.Close(); err != nil {
		return err
	}
	if tx != nil {
		return tx.Commit(ctx)
	}
	return nil
}

func (c *conn) Teardown() {
	c.c.Close()
}

type streamCursor struct {
	LSN             uint64
	SlotName        string
	PublicationName string
}

func (c *streamCursor) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeStreamCursor(in []byte) (*streamCursor, error) {
	if len(in) == 0 {
		return &streamCursor{}, nil
	}
	var c streamCursor
	br := bytes.NewReader(in)
	dec := gob.NewDecoder(br)
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

type partitionCursor struct {
	Low  []interface{}
	High []interface{}
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

type pkeyRow struct {
	TableSchema string
	TableName   string
	ColumnName  string
}

type columnRow struct {
	TableSchema string
	TableName   string
	ColumnName  string
}

type sinkColumnMetadata struct {
	TableSchema        string
	TableName          string
	Name               string
	DatabaseType       string
	Nullable           bool
	HasDefault         bool
	Generated          bool
	Identity           bool
	IdentityGeneration string
}

type sinkTableMetadata struct {
	Columns    []sinkColumnMetadata
	PrimaryKey []string
}

func getPrimaryKey(primaryKeys []string, data map[string]interface{}) []interface{} {
	var res []interface{}
	for _, col := range primaryKeys {
		res = append(res, data[col])
	}
	return res
}

func getTablePrimaryKeySchema(ctx context.Context, c *pgxpool.Pool) (map[string][]string, error) {
	rows, err := c.Query(ctx, `
		SELECT t.table_schema, t.table_name, c.column_name
		FROM information_schema.table_constraints AS t
		JOIN information_schema.key_column_usage AS c
		  ON c.constraint_catalog = t.constraint_catalog
		 AND c.constraint_schema = t.constraint_schema
		 AND c.constraint_name = t.constraint_name
		 AND c.table_schema = t.table_schema
		 AND c.table_name = t.table_name
		WHERE t.constraint_type = 'PRIMARY KEY'
		  AND t.table_schema != 'pg_catalog'
		ORDER BY t.table_schema, t.table_name, c.ordinal_position`)
	if err != nil {
		return nil, err
	}
	pkRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[pkeyRow])
	if err != nil {
		return nil, err
	}
	pkeys := map[string][]string{}
	for _, pkRow := range pkRows {
		k := pkRow.TableSchema + "." + pkRow.TableName
		pkeys[k] = append(pkeys[k], pkRow.ColumnName)
	}
	return pkeys, nil
}

func getTableColumns(ctx context.Context, c *pgxpool.Pool) (map[string]map[string]struct{}, error) {
	rows, err := c.Query(ctx, `
		SELECT n.nspname AS table_schema, c.relname AS table_name, a.attname AS column_name
		FROM pg_catalog.pg_attribute AS a
		JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
		JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		WHERE a.attnum > 0
		  AND NOT a.attisdropped
		  AND c.relkind IN ('r', 'p')
		  AND n.nspname != 'pg_catalog'`)
	if err != nil {
		return nil, err
	}
	columnRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[columnRow])
	if err != nil {
		return nil, err
	}
	result := map[string]map[string]struct{}{}
	for _, row := range columnRows {
		namespace := row.TableSchema + "." + row.TableName
		if result[namespace] == nil {
			result[namespace] = map[string]struct{}{}
		}
		result[namespace][row.ColumnName] = struct{}{}
	}
	return result, nil
}

func getSinkTableMetadata(ctx context.Context, c *pgxpool.Pool, pkeys map[string][]string) (map[string]sinkTableMetadata, error) {
	rows, err := c.Query(ctx, `
		SELECT table_schema,
		       table_name,
		       column_name AS name,
		       udt_name AS database_type,
		       is_nullable = 'YES' AS nullable,
		       column_default IS NOT NULL AS has_default,
		       is_generated <> 'NEVER' AS generated,
		       is_identity = 'YES' AS identity,
		       COALESCE(identity_generation, '') AS identity_generation
		FROM information_schema.columns
		WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY table_schema, table_name, ordinal_position`)
	if err != nil {
		return nil, err
	}
	columnRows, err := pgx.CollectRows(rows, pgx.RowToStructByName[sinkColumnMetadata])
	if err != nil {
		return nil, err
	}
	result := map[string]sinkTableMetadata{}
	for _, row := range columnRows {
		namespace := row.TableSchema + "." + row.TableName
		metadata := result[namespace]
		metadata.Columns = append(metadata.Columns, row)
		metadata.PrimaryKey = pkeys[namespace]
		result[namespace] = metadata
	}
	return result, nil
}

func idColumnMetadata(columns map[string]map[string]struct{}, pkeys map[string][]string) (map[string]bool, map[string]string) {
	hasIDColumn := map[string]bool{}
	idAliases := map[string]string{}
	for namespace, tableColumns := range columns {
		if _, exists := tableColumns["_id"]; !exists {
			continue
		}
		hasIDColumn[namespace] = true
		if slices.Contains(pkeys[namespace], "_id") {
			continue
		}
		for suffix := 2; ; suffix++ {
			alias := "_id" + strconv.Itoa(suffix)
			if _, exists := tableColumns[alias]; !exists {
				idAliases[namespace] = alias
				break
			}
		}
	}
	return hasIDColumn, idAliases
}

func NewConn(ctx context.Context, settings PostgresSettings) (*conn, error) {
	url := settings.URL
	// Set some defaults
	if settings.Limit == 0 {
		settings.Limit = 1000
	}
	if settings.StreamFlushDelay == 0 {
		settings.StreamFlushDelay = time.Minute * 3
	}

	rUrl, err := createReplicationUrl(url)
	if err != nil {
		return nil, err
	}
	hasher := xxhash.New()
	if _, err := hasher.Write([]byte(url)); err != nil {
		return nil, err
	}
	id := hasher.Sum64()

	afterConnect := func(ctx context.Context, conn *pgx.Conn) error {
		pgxdecimal.Register(conn.TypeMap())
		return nil
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	config.AfterConnect = afterConnect
	c, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	pkeys, err := getTablePrimaryKeySchema(ctx, c)
	if err != nil {
		c.Close()
		return nil, err
	}
	columns, err := getTableColumns(ctx, c)
	if err != nil {
		c.Close()
		return nil, err
	}
	sinkMetadata, err := getSinkTableMetadata(ctx, c, pkeys)
	if err != nil {
		c.Close()
		return nil, err
	}
	hasIDColumn, idAliases := idColumnMetadata(columns, pkeys)

	// Create BSON registry with decimal support
	bsonRegistry := NewBSONRegistry()

	return &conn{
		id:             id,
		replicationUrl: rUrl,
		c:              c,
		pkeys:          pkeys,
		sinkMetadata:   sinkMetadata,
		hasIDColumn:    hasIDColumn,
		idAliases:      idAliases,
		settings:       settings,
		bsonRegistry:   bsonRegistry,
	}, nil
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	namespaces := r.Msg.GetNamespaces()
	if len(namespaces) < 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("must specify at least one namespace"))
	}
	for _, namespace := range namespaces {
		if len(c.pkeys[namespace]) == 0 {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no primary key found for namespace %s", namespace))
		}
	}

	var partitions []*adiomv1.Partition
	var updatesPartitions []*adiomv1.UpdatesPartition

	if r.Msg.GetInitialSync() {
		for _, namespace := range namespaces {
			sanitizedNamespace := SanitizeNamespace(namespace)
			var sanitizedKeys []string
			keys := c.pkeys[namespace]
			for _, k := range keys {
				sanitizedKeys = append(sanitizedKeys, pgx.Identifier([]string{k}).Sanitize())
			}
			primaryKeysPartial := strings.Join(sanitizedKeys, ", ")

			percentage := 100.0 / float64(c.settings.TargetDocCountPerPartition+1)

			sampleQuery := fmt.Sprintf("SELECT %v FROM %v TABLESAMPLE BERNOULLI (%v) ORDER BY %v", primaryKeysPartial, sanitizedNamespace, percentage, primaryKeysPartial)
			rows, err := c.c.Query(ctx, sampleQuery)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			ms, err := pgx.CollectRows(rows, pgx.RowToMap)
			if err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			for _, m := range ms {
				getPrimaryKey(keys, m)
			}

			var last []interface{}
			ms = append(ms, nil)
			for _, m := range ms {
				var k []interface{}
				if m != nil {
					k = getPrimaryKey(keys, m)
				}
				cursor := &partitionCursor{
					Low:  last,
					High: k,
				}
				encodedCursor, err := cursor.Encode()
				if err != nil {
					return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to set initial cursor"))
				}
				partitions = append(partitions, &adiomv1.Partition{
					Namespace:      namespace,
					Cursor:         encodedCursor,
					EstimatedCount: uint64(c.settings.TargetDocCountPerPartition),
				})
				last = k
			}
		}
	}

	if r.Msg.GetUpdates() {
		var sanitizedNamespaces []string
		for _, n := range namespaces {
			sanitizedNamespaces = append(sanitizedNamespaces, SanitizeNamespace(n))
		}

		sanitizedPublication := pgx.Identifier([]string{c.settings.PublicationName}).Sanitize()

		if c.settings.Force {
			if _, err := c.c.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %v", sanitizedPublication)); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

			createPublicationQ := fmt.Sprintf("CREATE PUBLICATION %v FOR TABLE %v;", sanitizedPublication, strings.Join(sanitizedNamespaces, ", "))
			if _, err := c.c.Exec(ctx, createPublicationQ); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}

			var count int
			if err := c.c.QueryRow(ctx, "SELECT COUNT(*) from pg_replication_slots WHERE slot_name=$1", c.settings.SlotName).Scan(&count); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
			if count > 0 {
				if _, err := c.c.Exec(ctx, "SELECT pg_drop_replication_slot($1)", c.settings.SlotName); err != nil {
					return nil, connect.NewError(connect.CodeInternal, err)
				}
			}

			if _, err := c.c.Exec(ctx, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", c.settings.SlotName); err != nil {
				return nil, connect.NewError(connect.CodeInternal, err)
			}
		}

		sc := &streamCursor{
			SlotName:        c.settings.SlotName,
			PublicationName: c.settings.PublicationName,
		}

		encodedSc, err := sc.Encode()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("unable to set initial cursor"))
		}

		updatesPartitions = []*adiomv1.UpdatesPartition{{
			Namespaces: r.Msg.GetNamespaces(),
			Cursor:     encodedSc,
		}}
	}

	return connect.NewResponse(&adiomv1.GeneratePlanResponse{
		Partitions:        partitions,
		UpdatesPartitions: updatesPartitions,
	}), nil
}

// GetInfo implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GetInfo(context.Context, *connect.Request[adiomv1.GetInfoRequest]) (*connect.Response[adiomv1.GetInfoResponse], error) {
	return connect.NewResponse(&adiomv1.GetInfoResponse{
		Id:     strconv.FormatUint(c.id, 10),
		DbType: "postgres",
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
	sanitizedNamespace := SanitizeNamespace(r.Msg.GetNamespace())
	var count int64
	if err := c.c.QueryRow(ctx, "SELECT reltuples::bigint AS estimate FROM pg_class WHERE oid = $1::regclass", r.Msg.GetNamespace()).Scan(&count); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if count < c.settings.EstimatedCountThreshold {
		row := c.c.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %v", sanitizedNamespace))
		err := row.Scan(&count)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	return connect.NewResponse(&adiomv1.GetNamespaceMetadataResponse{
		Count: uint64(count),
	}), nil
}

func SanitizeNamespace(s string) string {
	return pgx.Identifier(strings.Split(s, ".")).Sanitize()
}

// ListData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) ListData(ctx context.Context, r *connect.Request[adiomv1.ListDataRequest]) (*connect.Response[adiomv1.ListDataResponse], error) {
	limit := c.settings.Limit
	partition := r.Msg.GetPartition()
	namespace := partition.GetNamespace()
	keys := c.pkeys[namespace]
	if len(keys) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("no primary key found for namespace %s", namespace))
	}
	rawCursor := r.Msg.GetCursor()
	if rawCursor == nil {
		rawCursor = partition.GetCursor()
	}
	cursor, err := DecodePartitionCursor(rawCursor)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error decoding cursor"))
	}

	var args []any
	var whereQueries []string
	var sanitizedKeys []string
	for _, k := range keys {
		sanitizedKeys = append(sanitizedKeys, pgx.Identifier([]string{k}).Sanitize())
	}
	primaryKeysPartial := strings.Join(sanitizedKeys, ", ")

	if len(cursor.Low) > 0 {
		if len(cursor.Low) != len(keys) {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error mismatched cursor and primary key lengths"))
		}
		var values []string
		for i, v := range cursor.Low {
			values = append(values, fmt.Sprintf("$%v", i+1))
			args = append(args, v)
		}
		whereQueries = append(whereQueries, fmt.Sprintf("(%v) >= (%v)", primaryKeysPartial, strings.Join(values, ", ")))
	}
	if len(cursor.High) > 0 {
		if len(cursor.High) != len(keys) {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error mismatched cursor and primary key lengths"))
		}
		offset := 1 + len(cursor.Low)
		var values []string
		for i, v := range cursor.High {
			values = append(values, fmt.Sprintf("$%v", i+offset))
			args = append(args, v)
		}
		whereQueries = append(whereQueries, fmt.Sprintf("(%v) < (%v)", primaryKeysPartial, strings.Join(values, ", ")))
	}
	var where string
	if len(whereQueries) > 0 {
		where = " WHERE "
	}

	q := fmt.Sprintf("SELECT * from %v%v%v ORDER BY %v LIMIT %v", SanitizeNamespace(namespace), where, strings.Join(whereQueries, " AND "), primaryKeysPartial, limit+1)
	rows, err := c.c.Query(ctx, q, args...)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	ms, err := pgx.CollectRows(rows, pgx.RowToMap)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	var data [][]byte

	for i, m := range ms {
		if i >= limit {
			break
		}
		if err := c.prepareSourceDocument(namespace, m); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		marshalled, err := marshalWithRegistry(c.bsonRegistry, m)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		data = append(data, marshalled)
	}

	var nextCursor []byte
	if len(ms) > limit {
		last := ms[limit]

		next := *cursor
		next.Low = nil
		for _, k := range keys {
			next.Low = append(next.Low, last[k])
		}
		nextCursor, err = next.Encode()
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("error encoding next cursor"))
		}
	}

	return connect.NewResponse(&adiomv1.ListDataResponse{
		Data:       data,
		NextCursor: nextCursor,
	}), nil
}

// StreamLSN implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamLSN(context.Context, *connect.Request[adiomv1.StreamLSNRequest], *connect.ServerStream[adiomv1.StreamLSNResponse]) error {
	return nil
}

func toMongoID(reg *bson.Registry, in []interface{}) ([]*adiomv1.BsonValue, interface{}, error) {
	if len(in) == 1 {
		typ, data, err := marshalValueWithRegistry(reg, in[0])
		if err != nil {
			return nil, nil, err
		}
		return []*adiomv1.BsonValue{{
			Name: "_id",
			Data: data,
			Type: uint32(typ),
		}}, in[0], nil
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(in); err != nil {
		return nil, bson.Binary{}, err
	}
	b := bson.Binary{
		Subtype: bson.TypeBinaryGeneric,
		Data:    buf.Bytes(),
	}
	typ, data, err := marshalValueWithRegistry(reg, b)
	if err != nil {
		return nil, bson.Binary{}, err
	}
	return []*adiomv1.BsonValue{{
		Name: "_id",
		Data: data,
		Type: uint32(typ),
	}}, b, nil
}

func toNamedID(reg *bson.Registry, names []string, values map[string]interface{}) ([]*adiomv1.BsonValue, error) {
	result := make([]*adiomv1.BsonValue, 0, len(names))
	for _, name := range names {
		typ, data, err := marshalValueWithRegistry(reg, values[name])
		if err != nil {
			return nil, err
		}
		result = append(result, &adiomv1.BsonValue{Name: name, Type: uint32(typ), Data: data})
	}
	return result, nil
}

func idLast(primaryKeys []string) []string {
	result := make([]string, 0, len(primaryKeys))
	for _, key := range primaryKeys {
		if key != "_id" {
			result = append(result, key)
		}
	}
	if slices.Contains(primaryKeys, "_id") {
		result = append(result, "_id")
	}
	return result
}

func (c *conn) sourceUpdateID(namespace string, values map[string]interface{}) ([]*adiomv1.BsonValue, error) {
	primaryKeys := c.pkeys[namespace]
	if c.hasIDColumn[namespace] && slices.Contains(primaryKeys, "_id") {
		return toNamedID(c.bsonRegistry, idLast(primaryKeys), values)
	}
	keys, _, err := toMongoID(c.bsonRegistry, getPrimaryKey(primaryKeys, values))
	return keys, err
}

func (c *conn) prepareSourceDocument(namespace string, values map[string]interface{}) error {
	primaryKeys := c.pkeys[namespace]
	if alias := c.idAliases[namespace]; alias != "" {
		values[alias] = values["_id"]
		delete(values, "_id")
	}
	if c.hasIDColumn[namespace] && slices.Contains(primaryKeys, "_id") {
		return nil
	}
	_, id, err := toMongoID(c.bsonRegistry, getPrimaryKey(primaryKeys, values))
	if err != nil {
		return err
	}
	values["_id"] = id
	return nil
}

func (c *conn) toBsonAdiomUpdate(update pglib.Update) (*adiomv1.Update, error) {
	ns := update.Namespace + "." + update.TableName
	switch update.Type {
	case pglib.UpdateTypeInsert:
		keys, err := c.sourceUpdateID(ns, update.New)
		if err != nil {
			return nil, err
		}
		if err := c.prepareSourceDocument(ns, update.New); err != nil {
			return nil, err
		}
		marshalled, err := marshalWithRegistry(c.bsonRegistry, update.New)
		if err != nil {
			return nil, err
		}
		return &adiomv1.Update{
			Id:   keys,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: marshalled,
		}, nil
	case pglib.UpdateTypeUpdate:
		keys, err := c.sourceUpdateID(ns, update.New)
		if err != nil {
			return nil, err
		}
		if err := c.prepareSourceDocument(ns, update.New); err != nil {
			return nil, err
		}
		marshalled, err := marshalWithRegistry(c.bsonRegistry, update.New)
		if err != nil {
			return nil, err
		}
		return &adiomv1.Update{
			Id:   keys,
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: marshalled,
		}, nil
	case pglib.UpdateTypeDelete:
		keys, err := c.sourceUpdateID(ns, update.Old)
		if err != nil {
			return nil, err
		}
		return &adiomv1.Update{
			Id:   keys,
			Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE,
		}, nil
	}
	return nil, nil
}

// StreamUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) StreamUpdates(ctx context.Context, req *connect.Request[adiomv1.StreamUpdatesRequest], res *connect.ServerStream[adiomv1.StreamUpdatesResponse]) error {
	pgConn, err := pgconn.Connect(ctx, c.replicationUrl)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}
	defer pgConn.Close(ctx)

	rawCursor := req.Msg.GetCursor()
	cursor, err := DecodeStreamCursor(rawCursor)
	if err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	cs := &pglib.ChangeStream{
		ReplicationURL:      c.replicationUrl,
		ReplicationSlotName: cursor.SlotName,
		PublicationName:     cursor.PublicationName,
		StartLSN:            cursor.LSN,
		MinFlushDelay:       c.settings.StreamFlushDelay,
	}

	eg, ctx := errgroup.WithContext(ctx)
	maxBatchSize := c.settings.StreamMaxBatchSize
	maxBatchWait := c.settings.StreamMaxBatchWait

	ch := make(chan pglib.UpdateBatch, 10)
	eg.Go(func() error {
		defer close(ch)
		return cs.Run(ctx, ch)
	})

	eg.Go(func() error {
		var batch []*adiomv1.Update
		var ns string
		var lastSafeLSN uint64
		deadline := time.Now().Add(maxBatchWait)

		send := func() error {
			if len(batch) < 1 {
				return nil
			}
			cursor.LSN = max(cursor.LSN, lastSafeLSN)
			nextCursor, err := cursor.Encode()
			if err != nil {
				return err
			}
			if err := res.Send(&adiomv1.StreamUpdatesResponse{
				Updates:    batch,
				Namespace:  ns,
				NextCursor: nextCursor,
			}); err != nil {
				return err
			}
			batch = nil
			deadline = time.Now().Add(maxBatchWait)
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				return nil
			case updateBatch, ok := <-ch:
				if !ok {
					return nil
				}
				for _, update := range updateBatch.Updates {
					up, err := c.toBsonAdiomUpdate(update)
					if err != nil {
						return err
					}
					newNS := update.Namespace + "." + update.TableName
					if ns != newNS {
						if err := send(); err != nil {
							return err
						}
						ns = newNS
					}
					batch = append(batch, up)
				}
				lastSafeLSN = max(lastSafeLSN, updateBatch.LSN)
				if maxBatchSize > 0 && len(batch) > maxBatchSize {
					if err := send(); err != nil {
						return err
					}

				}
				if (maxBatchWait > 0 && time.Now().After(deadline)) || len(ch) == 0 || (maxBatchSize > 0 && len(batch) > maxBatchSize) {
					if err := send(); err != nil {
						return err
					}
				}
			}
		}
	})

	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}

type postgresWriteOperation struct {
	delete           bool
	overrideIdentity bool
	columns          []string
	defaultColumns   []string
	values           []interface{}
	requestIndex     int
	data             map[string]interface{}
}

func (c *conn) sinkTable(namespace string) (sinkTableMetadata, error) {
	metadata, ok := c.sinkMetadata[namespace]
	if !ok || len(metadata.Columns) == 0 {
		return sinkTableMetadata{}, fmt.Errorf("namespace %s was not found or has no visible columns", namespace)
	}
	if len(metadata.PrimaryKey) == 0 {
		return sinkTableMetadata{}, fmt.Errorf("no primary key found for namespace %s", namespace)
	}
	return metadata, nil
}

func planPostgresUpsert(metadata sinkTableMetadata, document map[string]interface{}, requestIndex int) (postgresWriteOperation, error) {
	columnsByName := make(map[string]sinkColumnMetadata, len(metadata.Columns))
	for _, column := range metadata.Columns {
		columnsByName[column.Name] = column
	}
	for name := range document {
		if _, ok := columnsByName[name]; !ok {
			return postgresWriteOperation{}, fmt.Errorf("column %q does not exist in destination table", name)
		}
	}

	columns := make([]string, 0, len(metadata.Columns))
	values := make([]interface{}, 0, len(metadata.Columns))
	defaultColumns := make([]string, 0, len(metadata.Columns))
	for _, key := range metadata.PrimaryKey {
		column, ok := columnsByName[key]
		if !ok {
			return postgresWriteOperation{}, fmt.Errorf("primary key field %q is not present in table metadata", key)
		}
		if column.Generated && !column.Identity {
			return postgresWriteOperation{}, fmt.Errorf("generated primary key field %q cannot be written explicitly", key)
		}
		value, exists := document[key]
		if !exists || value == nil {
			return postgresWriteOperation{}, fmt.Errorf("document is missing non-null key field %q", key)
		}
		columns = append(columns, key)
		values = append(values, value)
	}

	for _, column := range metadata.Columns {
		if slices.Contains(metadata.PrimaryKey, column.Name) {
			continue
		}
		if column.Generated || column.Identity {
			if _, exists := document[column.Name]; exists {
				return postgresWriteOperation{}, fmt.Errorf("column %q is generated by the destination and cannot be supplied", column.Name)
			}
			continue
		}
		value, exists := document[column.Name]
		if exists {
			if value == nil && !column.Nullable {
				return postgresWriteOperation{}, fmt.Errorf("column %q is required and cannot be null", column.Name)
			}
			columns = append(columns, column.Name)
			values = append(values, value)
			continue
		}
		if column.HasDefault {
			defaultColumns = append(defaultColumns, column.Name)
			continue
		}
		if !column.Nullable {
			return postgresWriteOperation{}, fmt.Errorf("document is missing required column %q", column.Name)
		}
		columns = append(columns, column.Name)
		values = append(values, nil)
	}

	overrideIdentity := false
	for _, key := range metadata.PrimaryKey {
		column := columnsByName[key]
		if column.Identity && strings.EqualFold(column.IdentityGeneration, "ALWAYS") {
			overrideIdentity = true
			break
		}
	}
	return postgresWriteOperation{
		overrideIdentity: overrideIdentity,
		columns:          columns,
		defaultColumns:   defaultColumns,
		values:           values,
		requestIndex:     requestIndex,
		data:             document,
	}, nil
}

// buildUpsertQuery creates an INSERT ... ON CONFLICT ... DO UPDATE query for postgres.
func buildUpsertQuery(namespace string, pkeys, columns, defaultColumns []string, overrideIdentity bool) string {
	sanitizedNamespace := SanitizeNamespace(namespace)

	// Build column list
	var sanitizedColumns []string
	for _, col := range columns {
		sanitizedColumns = append(sanitizedColumns, pgx.Identifier([]string{col}).Sanitize())
	}
	columnList := strings.Join(sanitizedColumns, ", ")

	// Build placeholders for VALUES
	var placeholders []string
	for i := range columns {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	valuesPlaceholder := fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))

	// Build conflict target (primary keys)
	var sanitizedPKeys []string
	for _, pk := range pkeys {
		sanitizedPKeys = append(sanitizedPKeys, pgx.Identifier([]string{pk}).Sanitize())
	}
	conflictTarget := strings.Join(sanitizedPKeys, ", ")

	// Build UPDATE SET clause (exclude primary keys).
	var updateSet []string
	for _, col := range columns {
		if !slices.Contains(pkeys, col) {
			sanitizedCol := pgx.Identifier([]string{col}).Sanitize()
			updateSet = append(updateSet, fmt.Sprintf("%s = EXCLUDED.%s", sanitizedCol, sanitizedCol))
		}
	}
	for _, col := range defaultColumns {
		updateSet = append(updateSet, fmt.Sprintf("%s = DEFAULT", pgx.Identifier([]string{col}).Sanitize()))
	}

	identityClause := ""
	if overrideIdentity {
		identityClause = " OVERRIDING SYSTEM VALUE"
	}

	if len(updateSet) == 0 {
		return fmt.Sprintf(
			"INSERT INTO %s (%s)%s VALUES %s ON CONFLICT (%s) DO NOTHING",
			sanitizedNamespace, columnList, identityClause, valuesPlaceholder, conflictTarget,
		)
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s)%s VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
		sanitizedNamespace, columnList, identityClause, valuesPlaceholder, conflictTarget, strings.Join(updateSet, ", "),
	)
}

func buildDeleteQuery(namespace string, pkeys []string) string {
	whereClauses := make([]string, 0, len(pkeys))
	for i, pk := range pkeys {
		whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", pgx.Identifier([]string{pk}).Sanitize(), i+1))
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", SanitizeNamespace(namespace), strings.Join(whereClauses, " AND "))
}

func (c *conn) executePostgresWrites(ctx context.Context, namespace string, pkeys []string, operations []postgresWriteOperation) error {
	if len(operations) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, operation := range operations {
		if operation.delete {
			batch.Queue(buildDeleteQuery(namespace, pkeys), operation.values...)
			continue
		}
		batch.Queue(buildUpsertQuery(namespace, pkeys, operation.columns, operation.defaultColumns, operation.overrideIdentity), operation.values...)
	}
	return c.sendSinkBatch(ctx, batch, len(operations), func(i int, err error) {
		if errors.Is(err, context.Canceled) {
			return
		}
		operation := operations[i]
		slog.Error("Failed to execute PostgreSQL sink operation", "index", operation.requestIndex, "error", err)
		if operation.delete {
			slog.Debug("Failed delete operation", "pkValues", operation.values)
			return
		}
		for key, value := range operation.data {
			slog.Debug("Operation field", "key", key, "value", value, "type", fmt.Sprintf("%T", value))
		}
	})
}

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, r *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	if r.Msg.GetPartialWriteResults() {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partial write results are not supported by postgres"))
	}
	namespace := r.Msg.GetNamespace()
	metadata, err := c.sinkTable(namespace)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}

	operations := make([]postgresWriteOperation, 0, len(r.Msg.GetData()))
	for i, data := range r.Msg.GetData() {
		document, err := bsonToMap(c.bsonRegistry, data, c.hasIDColumn[namespace])
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				slog.Error("Failed to unmarshal BSON document", "index", i, "error", err)
			}
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("decode document %d: %w", i, err))
		}
		operation, err := planPostgresUpsert(metadata, document, i)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("document %d: %w", i, err))
		}
		operations = append(operations, operation)
	}
	if err := c.executePostgresWrites(ctx, namespace, metadata.PrimaryKey, operations); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// fromMongoID extracts the original primary key values from BSON ID
func fromMongoID(reg *bson.Registry, id []*adiomv1.BsonValue, primaryKeys []string) ([]interface{}, error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("expected at least one ID value")
	}
	if len(id) > 1 {
		parts := make(map[string]*adiomv1.BsonValue, len(id))
		for _, part := range id {
			if part.GetName() != "" {
				parts[part.GetName()] = part
			}
		}
		values := make([]interface{}, 0, len(primaryKeys))
		for _, primaryKey := range primaryKeys {
			part, ok := parts[primaryKey]
			if !ok {
				return nil, fmt.Errorf("ID is missing primary key field %q", primaryKey)
			}
			var value interface{}
			rawValue := bson.RawValue{Type: bson.Type(part.GetType()), Value: part.GetData()}
			if err := rawValue.UnmarshalWithRegistry(reg, &value); err != nil {
				return nil, err
			}
			value, err := bsonValueForPostgres(primaryKey, value)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		return values, nil
	}

	var value interface{}
	rawValue := bson.RawValue{Type: bson.Type(id[0].GetType()), Value: id[0].GetData()}
	if err := rawValue.UnmarshalWithRegistry(reg, &value); err != nil {
		return nil, err
	}

	// If it's a binary (composite key), decode it
	if b, ok := value.(bson.Binary); ok && b.Subtype == bson.TypeBinaryGeneric {
		var compositeKey []interface{}
		br := bytes.NewReader(b.Data)
		dec := gob.NewDecoder(br)
		if err := dec.Decode(&compositeKey); err == nil {
			return compositeKey, nil
		}
		// Generic BSON binary is also how a scalar bytea key is represented.
		// Legacy composite IDs are gob-encoded, so only treat the value as a
		// composite when gob decoding succeeds.
		value = b.Data
	}

	// Single key
	value, err := bsonValueForPostgres(primaryKeys[0], value)
	if err != nil {
		return nil, err
	}
	return []interface{}{value}, nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, r *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	if r.Msg.GetPartialWriteResults() {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partial write results are not supported by postgres"))
	}
	namespace := r.Msg.GetNamespace()
	metadata, err := c.sinkTable(namespace)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	for _, update := range r.Msg.GetUpdates() {
		if update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_PARTIAL_UPDATE {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("partial updates are not supported by postgres"))
		}
	}

	operations := make([]postgresWriteOperation, 0, len(r.Msg.GetUpdates()))
	for i, update := range r.Msg.GetUpdates() {
		if update.GetType() == adiomv1.UpdateType_UPDATE_TYPE_UNKNOWN {
			continue
		}
		pkValues, err := fromMongoID(c.bsonRegistry, update.GetId(), metadata.PrimaryKey)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: failed to extract primary key: %w", i, err))
		}
		if len(pkValues) != len(metadata.PrimaryKey) {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: primary key length mismatch: expected %d, got %d", i, len(metadata.PrimaryKey), len(pkValues)))
		}
		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			document, err := bsonToMap(c.bsonRegistry, update.GetData(), c.hasIDColumn[namespace])
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					slog.Error("Failed to unmarshal BSON update", "index", i, "error", err)
				}
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("decode update %d: %w", i, err))
			}
			for keyIndex, key := range metadata.PrimaryKey {
				document[key] = pkValues[keyIndex]
			}
			operation, err := planPostgresUpsert(metadata, document, i)
			if err != nil {
				return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: %w", i, err))
			}
			operations = append(operations, operation)
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			operations = append(operations, postgresWriteOperation{delete: true, values: pkValues, requestIndex: i})
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("update %d: unsupported update type: %v", i, update.GetType()))
		}
	}
	if err := c.executePostgresWrites(ctx, namespace, metadata.PrimaryKey, operations); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func createReplicationUrl(urlString string) (string, error) {
	parsed, err := url.Parse(urlString)
	if err != nil {
		return "", err
	}
	query := parsed.Query()
	query.Add("replication", "database")
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

var _ adiomv1connect.ConnectorServiceHandler = &conn{}
