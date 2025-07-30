package postgres

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/postgres/pglib"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"github.com/adiom-data/dsync/gen/adiom/v1/adiomv1connect"
	"github.com/cespare/xxhash"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"golang.org/x/sync/errgroup"
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
}

type conn struct {
	replicationUrl string
	id             uint64
	c              *pgxpool.Pool

	pkeys    map[string][]string
	settings PostgresSettings
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

func getPrimaryKey(primaryKeys []string, data map[string]interface{}) []interface{} {
	var res []interface{}
	for _, col := range primaryKeys {
		res = append(res, data[col])
	}
	return res
}

func getTablePrimaryKeySchema(ctx context.Context, c *pgxpool.Pool) (map[string][]string, error) {
	rows, err := c.Query(ctx, "SELECT t.table_schema, t.table_name, c.column_name FROM information_schema.key_column_usage AS c LEFT JOIN information_schema.table_constraints AS t ON t.constraint_name = c.constraint_name WHERE t.constraint_type = 'PRIMARY KEY' AND t.table_schema != 'pg_catalog' order by t.table_schema, t.table_name, c.ordinal_position;")
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
	c, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	pkeys, err := getTablePrimaryKeySchema(ctx, c)
	if err != nil {
		return nil, err
	}

	return &conn{
		id:             id,
		replicationUrl: rUrl,
		c:              c,
		pkeys:          pkeys,
		settings:       settings,
	}, nil
}

// GeneratePlan implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) GeneratePlan(ctx context.Context, r *connect.Request[adiomv1.GeneratePlanRequest]) (*connect.Response[adiomv1.GeneratePlanResponse], error) {
	namespaces := r.Msg.GetNamespaces()
	if len(namespaces) < 1 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("must specify at least one namespace"))
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
					Namespace: namespace,
					Cursor:    encodedCursor,
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
		keys := getPrimaryKey(c.pkeys[namespace], m)
		_, b, err := toMongoID(keys)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		m["_id"] = b
		marshalled, err := bson.Marshal(m)
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

func toMongoID(in []interface{}) ([]*adiomv1.BsonValue, interface{}, error) {
	if len(in) == 1 {
		typ, data, err := bson.MarshalValue(in[0])
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
		return nil, primitive.Binary{}, err
	}
	b := primitive.Binary{
		Subtype: bson.TypeBinaryGeneric,
		Data:    buf.Bytes(),
	}
	typ, data, err := bson.MarshalValue(b)
	if err != nil {
		return nil, primitive.Binary{}, err
	}
	return []*adiomv1.BsonValue{{
		Name: "_id",
		Data: data,
		Type: uint32(typ),
	}}, b, nil
}

func (c *conn) toBsonAdiomUpdate(update pglib.Update) (*adiomv1.Update, error) {
	ns := update.Namespace + "." + update.TableName
	primaryKeys := c.pkeys[ns]
	switch update.Type {
	case pglib.UpdateTypeInsert:
		keys, b, err := toMongoID(getPrimaryKey(primaryKeys, update.New))
		if err != nil {
			return nil, err
		}
		update.New["_id"] = b
		marshalled, err := bson.Marshal(update.New)
		if err != nil {
			return nil, err
		}
		return &adiomv1.Update{
			Id:   keys,
			Type: adiomv1.UpdateType_UPDATE_TYPE_INSERT,
			Data: marshalled,
		}, nil
	case pglib.UpdateTypeUpdate:
		// Actually we'll ignore "old" and just accept breaking behavior
		// since in the mongo format you can't edit the _id
		keys, b, err := toMongoID(getPrimaryKey(primaryKeys, update.New))
		if err != nil {
			return nil, err
		}
		update.New["_id"] = b
		marshalled, err := bson.Marshal(update.New)
		if err != nil {
			return nil, err
		}
		return &adiomv1.Update{
			Id:   keys,
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: marshalled,
		}, nil
	case pglib.UpdateTypeDelete:
		keys, _, err := toMongoID(getPrimaryKey(primaryKeys, update.Old))
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

	ch := make(chan []pglib.Update, 10)
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
			case updates, ok := <-ch:
				if !ok {
					return nil
				}
				for _, update := range updates {
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
				lastSafeLSN = updates[len(updates)-1].LSN
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

// WriteData implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteData(ctx context.Context, req *connect.Request[adiomv1.WriteDataRequest]) (*connect.Response[adiomv1.WriteDataResponse], error) {
	namespace := req.Msg.GetNamespace()
	data := req.Msg.GetData()
	if len(data) == 0 {
		return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
	}
	keys := c.pkeys[namespace]
	if len(keys) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown primary key for namespace: %s", namespace))
	}
	sanitizedNamespace := SanitizeNamespace(namespace)
	batchSize := 1000 // reasonable default, can be tuned or made configurable
	var batchCols []string
	var batchVals [][]interface{}
	for i, raw := range data {
		var m map[string]interface{}
		if err := bson.Unmarshal(raw, &m); err != nil {
			return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to unmarshal BSON: %w", err))
		}
		if len(batchCols) == 0 {
			for k := range m {
				batchCols = append(batchCols, pgx.Identifier([]string{k}).Sanitize())
			}
		}
		vals := make([]interface{}, len(batchCols))
		for j, col := range batchCols {
			origCol := strings.Trim(col, `"`)
			if v, ok := m[origCol]; ok {
				vals[j] = v
			} else {
				vals[j] = nil
			}
		}
		batchVals = append(batchVals, vals)
		if len(batchVals) == batchSize || i == len(data)-1 {
			var placeholders []string
			flatVals := []interface{}{}
			for _, vals := range batchVals {
				rowPlaceholders := []string{}
				for k := range vals {
					rowPlaceholders = append(rowPlaceholders, fmt.Sprintf("$%d", len(flatVals)+k+1))
				}
				placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.Join(rowPlaceholders, ", ")))
				flatVals = append(flatVals, vals...)
			}
			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", sanitizedNamespace, strings.Join(batchCols, ", "), strings.Join(placeholders, ", "))
			if _, err := c.c.Exec(ctx, query, flatVals...); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("batch insert failed: %w", err))
			}
			batchVals = nil
		}
	}
	return connect.NewResponse(&adiomv1.WriteDataResponse{}), nil
}

// WriteUpdates implements adiomv1connect.ConnectorServiceHandler.
func (c *conn) WriteUpdates(ctx context.Context, req *connect.Request[adiomv1.WriteUpdatesRequest]) (*connect.Response[adiomv1.WriteUpdatesResponse], error) {
	namespace := req.Msg.GetNamespace()
	updates := req.Msg.GetUpdates()
	keys := c.pkeys[namespace]
	if len(keys) == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("unknown primary key for namespace: %s", namespace))
	}
	sanitizedNamespace := SanitizeNamespace(namespace)
	for _, update := range updates {
		switch update.GetType() {
		case adiomv1.UpdateType_UPDATE_TYPE_INSERT, adiomv1.UpdateType_UPDATE_TYPE_UPDATE:
			var m map[string]interface{}
			if err := bson.Unmarshal(update.GetData(), &m); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("failed to unmarshal BSON: %w", err))
			}
			var cols []string
			var vals []interface{}
			var placeholders []string
			for k, v := range m {
				cols = append(cols, pgx.Identifier([]string{k}).Sanitize())
				if k == "_id" {
					vals = append(vals, pgx.Identifier([]string{string(update.GetId()[0].GetData())}).Sanitize())
				} else {
					vals = append(vals, v)
				}
				placeholders = append(placeholders, fmt.Sprintf("$%d", len(vals)))
			}
			conflictCols := make([]string, len(keys))
			for i, k := range keys {
				conflictCols[i] = pgx.Identifier([]string{k}).Sanitize()
			}
			setUpdates := []string{}
			for _, col := range cols {
				if !contains(conflictCols, col) {
					setUpdates = append(setUpdates, fmt.Sprintf("%s=EXCLUDED.%s", col, col))
				}
			}
			query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s", sanitizedNamespace, strings.Join(cols, ", "), strings.Join(placeholders, ", "), strings.Join(conflictCols, ", "), strings.Join(setUpdates, ", "))
			slog.Info(fmt.Sprintf("Executing upsert query: %s with values: %v\n", query, vals))
			if _, err := c.c.Exec(ctx, query, vals...); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("upsert failed: %w", err))
			}
		case adiomv1.UpdateType_UPDATE_TYPE_DELETE:
			idVals := make([]interface{}, len(keys))
			for i, k := range keys {
				if k == "_id" {
					idVals[i] = pgx.Identifier([]string{string(update.GetId()[0].GetData())}).Sanitize()
				} else {
					idVals[i] = extractBsonValue(update.GetId(), k)
				}
			}
			where := []string{}
			for i, k := range keys {
				where = append(where, fmt.Sprintf("%s=$%d", pgx.Identifier([]string{k}).Sanitize(), i+1))
			}
			query := fmt.Sprintf("DELETE FROM %s WHERE %s", sanitizedNamespace, strings.Join(where, " AND "))
			slog.Info(fmt.Sprintf("Executing delete query: %s with values: %v\n", query, idVals))
			if _, err := c.c.Exec(ctx, query, idVals...); err != nil {
				return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("delete failed: %w", err))
			}
		}
	}
	return connect.NewResponse(&adiomv1.WriteUpdatesResponse{}), nil
}

func contains(arr []string, s string) bool {
	for _, a := range arr {
		if a == s {
			return true
		}
	}
	return false
}

func extractBsonValue(bsonVals []*adiomv1.BsonValue, key string) interface{} {
	for _, v := range bsonVals {
		if v.GetName() == "_id" || v.GetName() == key {
			var out interface{}
			_ = bson.Unmarshal(v.GetData(), &out)
			return out
		}
	}
	return nil
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
