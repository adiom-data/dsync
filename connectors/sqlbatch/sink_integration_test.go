package sqlbatch

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/adiom-data/dsync/connectors/sqlbatch/dialects"
	adiomv1 "github.com/adiom-data/dsync/gen/adiom/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func assertRFC3339SourceRow(t *testing.T, ctx context.Context, db *sql.DB, dialect SQLDialect, query string, temporalColumns ...string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		t.Fatalf("query returned no temporal row: %s", query)
	}
	row, err := ParseRow(columns, columnTypes, rows, &Mapping{}, dialect)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	for _, column := range temporalColumns {
		value, ok := document[column].(string)
		if !ok {
			t.Fatalf("source column %q encoded as %T, want RFC3339 string; JSON=%s", column, document[column], encoded)
		}
		if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
			t.Fatalf("source column %q value %q is not RFC3339: %v", column, value, err)
		}
	}
}

func TestPostgresSinkLive(t *testing.T) {
	connectionURL := os.Getenv("POSTGRES_TEST_URL")
	if connectionURL == "" {
		t.Skip("POSTGRES_TEST_URL is not set")
	}
	ctx := context.Background()
	db, err := sql.Open("pgx", connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`DROP SCHEMA IF EXISTS sqlbatch_live CASCADE`,
		`CREATE SCHEMA sqlbatch_live`,
		`CREATE TABLE sqlbatch_live.items (tenant text, id bigint, name text NOT NULL CHECK (name <> 'forbidden'), amount numeric DEFAULT 42, payload bytea, PRIMARY KEY (tenant, id))`,
		`CREATE TABLE sqlbatch_live.identity_items (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, name text NOT NULL)`,
		`CREATE TABLE sqlbatch_live.uuid_items (id uuid PRIMARY KEY, occurred_at timestamptz NOT NULL)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	c := &conn{
		c:       db,
		dialect: dialects.Postgres{},
	}

	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.items",
		Data:      [][]byte{[]byte(`{"tenant":"a","id":1,"name":"first","amount":1234567890.0123456789,"payload":"AAH+/w=="}`)},
	})); err != nil {
		t.Fatal(err)
	}
	var bytesMatch bool
	if err := db.QueryRowContext(ctx, `SELECT payload = decode('0001feff', 'hex') FROM sqlbatch_live.items WHERE tenant = 'a' AND id = 1`).Scan(&bytesMatch); err != nil || !bytesMatch {
		t.Fatalf("bytea round trip: match=%v err=%v", bytesMatch, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.items",
		Data:      [][]byte{[]byte(`{"tenant":"a","id":1,"name":"second"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	var name, amount string
	if err := db.QueryRowContext(ctx, `SELECT name, amount::text FROM sqlbatch_live.items WHERE tenant = 'a' AND id = 1`).Scan(&name, &amount); err != nil {
		t.Fatal(err)
	}
	if name != "second" || amount != "42" {
		t.Fatalf("initial upsert row: name=%q amount=%q", name, amount)
	}

	id, _, err := ToID([]string{"tenant", "id"}, []any{"a", int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE sqlbatch_live.items SET amount = 99 WHERE tenant = 'a' AND id = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "sqlbatch_live.items",
		Updates: []*adiomv1.Update{{
			Id: id, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: []byte(`{"tenant":"wrong","id":99,"name":"updated"}`),
		}},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT name, amount::text FROM sqlbatch_live.items WHERE tenant = 'a' AND id = 1`).Scan(&name, &amount); err != nil || name != "updated" || amount != "42" {
		t.Fatalf("full update row: name=%q amount=%q err=%v", name, amount, err)
	}

	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.items",
		Data: [][]byte{
			[]byte(`{"tenant":"b","id":2,"name":"batched-1"}`),
			[]byte(`{"tenant":"c","id":3,"name":"continued"}`),
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT name FROM sqlbatch_live.items WHERE tenant = 'c' AND id = 3`).Scan(&name); err != nil || name != "continued" {
		t.Fatalf("continued row: name=%q err=%v", name, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.items",
		Data: [][]byte{
			[]byte(`{"tenant":"f","id":6,"name":"duplicate-first"}`),
			[]byte(`{"tenant":"f","id":6,"name":"duplicate-last"}`),
		},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT name FROM sqlbatch_live.items WHERE tenant = 'f' AND id = 6`).Scan(&name); err != nil || name != "duplicate-last" {
		t.Fatalf("duplicate-key ordering: name=%q err=%v", name, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace:           "sqlbatch_live.items",
		PartialWriteResults: true,
		Data:                [][]byte{[]byte(`{"tenant":"d","id":4,"name":"unsupported"}`)},
	})); err == nil {
		t.Fatal("expected partial-result request to be rejected")
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.items",
		Data: [][]byte{
			[]byte(`{"tenant":"d","id":4,"name":"forbidden"}`),
			[]byte(`{"tenant":"e","id":5,"name":"same-batch"}`),
		},
	})); err == nil {
		t.Fatal("expected whole-batch database error")
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlbatch_live.items WHERE tenant IN ('d', 'e')`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed batch count=%d err=%v", count, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.items",
		Data: [][]byte{
			[]byte(`{"tenant":"g","id":7,"name":"would-be-valid"}`),
			[]byte(`{"tenant":"h","id":8}`),
		},
	})); err == nil {
		t.Fatal("expected missing required column error")
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlbatch_live.items WHERE tenant IN ('g', 'h')`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("prevalidated request count=%d err=%v", count, err)
	}

	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "sqlbatch_live.items",
		Updates:   []*adiomv1.Update{{Id: id, Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE}},
	})); err != nil {
		t.Fatal(err)
	}
	count = 0
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlbatch_live.items WHERE tenant = 'a' AND id = 1`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("delete count=%d err=%v", count, err)
	}

	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.identity_items",
		Data:      [][]byte{[]byte(`{"id":42,"name":"preserved"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT name FROM sqlbatch_live.identity_items WHERE id = 42`).Scan(&name); err != nil || name != "preserved" {
		t.Fatalf("generated identity row: name=%q err=%v", name, err)
	}

	const uuidText = "00010203-0405-0607-0809-0a0b0c0d0e0f"
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "sqlbatch_live.uuid_items",
		Data:      [][]byte{[]byte(`{"id":"` + uuidText + `","occurred_at":"2026-10-15T12:34:56.123456-07:00"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	uuidType, uuidData, err := bson.MarshalValue(bson.Binary{
		Subtype: bson.TypeBinaryUUID,
		Data:    []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "sqlbatch_live.uuid_items",
		Updates: []*adiomv1.Update{{
			Id:   []*adiomv1.BsonValue{{Name: "id", Type: uint32(uuidType), Data: uuidData}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: []byte(`{"occurred_at":"2026-10-16T01:02:03.654321Z"}`),
		}},
	})); err != nil {
		t.Fatal(err)
	}
	var occurredAt time.Time
	if err := db.QueryRowContext(ctx, `SELECT occurred_at FROM sqlbatch_live.uuid_items WHERE id = $1`, uuidText).Scan(&occurredAt); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 10, 16, 1, 2, 3, 654321000, time.UTC); !occurredAt.Equal(want) {
		t.Fatalf("PostgreSQL timestamp = %v, want %v", occurredAt, want)
	}
	assertRFC3339SourceRow(t, ctx, db, c.dialect, `SELECT occurred_at FROM sqlbatch_live.uuid_items WHERE id = '`+uuidText+`'`, "occurred_at")
}

func TestSQLServerSinkLive(t *testing.T) {
	connectionURL := os.Getenv("SQLSERVER_TEST_URL")
	if connectionURL == "" {
		t.Skip("SQLSERVER_TEST_URL is not set")
	}
	ctx := context.Background()
	db, err := sql.Open("sqlserver", connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range []string{
		`DROP TABLE IF EXISTS dbo.sqlbatch_live_items`,
		`DROP TABLE IF EXISTS dbo.sqlbatch_live_identity_items`,
		`DROP TABLE IF EXISTS dbo.sqlbatch_live_guid_items`,
		`DROP TABLE IF EXISTS dbo.sqlbatch_live_nonkey_identity`,
		`DROP TABLE IF EXISTS dbo.sqlbatch_live_decimal_keys`,
		`DROP TABLE IF EXISTS dbo.sqlbatch_live_temporal`,
		`CREATE TABLE dbo.sqlbatch_live_items (tenant nvarchar(20) NOT NULL, id bigint NOT NULL, name nvarchar(100) NOT NULL CHECK (name <> 'forbidden'), amount decimal(20,4) CONSTRAINT sqlbatch_live_amount_default DEFAULT 42, payload varbinary(max), CONSTRAINT sqlbatch_live_items_pk PRIMARY KEY (tenant, id))`,
		`CREATE TABLE dbo.sqlbatch_live_identity_items (id bigint IDENTITY(1,1) PRIMARY KEY, name nvarchar(100) NOT NULL)`,
		`CREATE TABLE dbo.sqlbatch_live_nonkey_identity (key_id bigint PRIMARY KEY, generated_id bigint IDENTITY(1,1) NOT NULL, name nvarchar(100) NOT NULL, computed AS (key_id + generated_id))`,
		`CREATE TABLE dbo.sqlbatch_live_decimal_keys (id decimal(20,4) PRIMARY KEY, name nvarchar(100) NOT NULL)`,
		`CREATE TABLE dbo.sqlbatch_live_temporal (id bigint PRIMARY KEY, occurred_at datetime2, event_date date, zoned_at datetimeoffset, at_time time)`,
		`CREATE TABLE dbo.sqlbatch_live_guid_items (id uniqueidentifier PRIMARY KEY, occurred_at datetimeoffset(7) NOT NULL, amount decimal(20,10) NOT NULL, payload varbinary(max))`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	c := &conn{c: db, dialect: dialects.SQLServer{}}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_items",
		Data: [][]byte{
			[]byte(`{"tenant":"a","id":1,"name":"first","payload":"AAH+/w=="}`),
			[]byte(`{"tenant":"b","id":2,"name":"second"}`),
		},
	})); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.sqlbatch_live_items WHERE amount = 42`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("defaulted row count=%d err=%v", count, err)
	}
	var bytesMatch bool
	if err := db.QueryRowContext(ctx, `SELECT CASE WHEN payload = 0x0001FEFF THEN 1 ELSE 0 END FROM dbo.sqlbatch_live_items WHERE tenant = 'a' AND id = 1`).Scan(&bytesMatch); err != nil || !bytesMatch {
		t.Fatalf("varbinary round trip: match=%v err=%v", bytesMatch, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_items",
		Data: [][]byte{
			[]byte(`{"tenant":"c","id":3,"name":"whole","amount":1}`),
			[]byte(`{"tenant":"d","id":4,"name":"fractional","amount":1.5}`),
		},
	})); err != nil {
		t.Fatalf("mixed decimal insert batch: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.sqlbatch_live_items WHERE (tenant = 'c' AND amount = 1) OR (tenant = 'd' AND amount = 1.5)`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("mixed decimal insert count=%d err=%v", count, err)
	}

	id, _, err := ToID([]string{"tenant", "id"}, []any{"a", int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dbo.sqlbatch_live_items",
		Updates: []*adiomv1.Update{{
			Id: id, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: []byte(`{"name":"updated"}`),
		}},
	})); err != nil {
		t.Fatal(err)
	}
	var name string
	if err := db.QueryRowContext(ctx, `SELECT name FROM dbo.sqlbatch_live_items WHERE tenant = 'a' AND id = 1`).Scan(&name); err != nil || name != "updated" {
		t.Fatalf("updated row: name=%q err=%v", name, err)
	}
	mixedUpdates := make([]*adiomv1.Update, 0, 2)
	for _, item := range []struct {
		tenant string
		id     int64
		data   string
	}{
		{tenant: "c", id: 3, data: `{"name":"whole-updated","amount":2}`},
		{tenant: "d", id: 4, data: `{"name":"fractional-updated","amount":2.5}`},
	} {
		itemID, _, err := ToID([]string{"tenant", "id"}, []any{item.tenant, item.id})
		if err != nil {
			t.Fatal(err)
		}
		mixedUpdates = append(mixedUpdates, &adiomv1.Update{Id: itemID, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: []byte(item.data)})
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dbo.sqlbatch_live_items",
		Updates:   mixedUpdates,
	})); err != nil {
		t.Fatalf("mixed decimal update batch: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.sqlbatch_live_items WHERE (tenant = 'c' AND amount = 2) OR (tenant = 'd' AND amount = 2.5)`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("mixed decimal update count=%d err=%v", count, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_decimal_keys",
		Data: [][]byte{
			[]byte(`{"id":1,"name":"whole"}`),
			[]byte(`{"id":1.5,"name":"fractional"}`),
		},
	})); err != nil {
		t.Fatalf("mixed decimal key insert batch: %v", err)
	}
	decimalID, err := bson.ParseDecimal128("1.5")
	if err != nil {
		t.Fatal(err)
	}
	decimalKeyUpdates := make([]*adiomv1.Update, 0, 2)
	for _, item := range []struct {
		id   any
		name string
	}{
		{id: int64(1), name: "whole-updated"},
		{id: decimalID, name: "fractional-updated"},
	} {
		itemID, _, err := ToID([]string{"id"}, []any{item.id})
		if err != nil {
			t.Fatal(err)
		}
		decimalKeyUpdates = append(decimalKeyUpdates, &adiomv1.Update{Id: itemID, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: []byte(`{"name":"` + item.name + `"}`)})
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dbo.sqlbatch_live_decimal_keys",
		Updates:   decimalKeyUpdates,
	})); err != nil {
		t.Fatalf("mixed decimal key update batch: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.sqlbatch_live_decimal_keys WHERE name LIKE '%-updated'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("mixed decimal key update count=%d err=%v", count, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_decimal_keys",
		Data: [][]byte{
			[]byte(`{"id":1,"name":"equivalent-first"}`),
			[]byte(`{"id":1.0,"name":"equivalent-last"}`),
		},
	})); err != nil {
		t.Fatalf("equivalent decimal keys: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT name FROM dbo.sqlbatch_live_decimal_keys WHERE id = 1`).Scan(&name); err != nil || name != "equivalent-last" {
		t.Fatalf("equivalent decimal key ordering: name=%q err=%v", name, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_temporal",
		Data: [][]byte{
			[]byte(`{"id":1,"occurred_at":"2026-10-19T12:34:56+05:30","event_date":"2026-10-19","zoned_at":"2026-10-19T12:34:56+05:30","at_time":"12:34:56+05:30"}`),
			[]byte(`{"id":2}`),
		},
	})); err != nil {
		t.Fatalf("temporal insert batch: %v", err)
	}
	var occurredAt, eventDate, zonedAt, atTime string
	var zonedOffset int
	if err := db.QueryRowContext(ctx, `SELECT CONVERT(varchar(33), occurred_at, 126), CONVERT(varchar(10), event_date, 23), CONVERT(varchar(19), zoned_at, 126), DATEPART(TZOFFSET, zoned_at), CONVERT(varchar(8), at_time, 108) FROM dbo.sqlbatch_live_temporal WHERE id = 1`).Scan(&occurredAt, &eventDate, &zonedAt, &zonedOffset, &atTime); err != nil {
		t.Fatal(err)
	}
	if occurredAt != "2026-10-19T07:04:56" || eventDate != "2026-10-19" || zonedAt != "2026-10-19T12:34:56" || zonedOffset != 330 || atTime != "07:04:56" {
		t.Fatalf("temporal values: occurred=%q date=%q zoned=%q offset=%d time=%q", occurredAt, eventDate, zonedAt, zonedOffset, atTime)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_temporal",
		Data: [][]byte{
			[]byte(`{"id":3}`),
			[]byte(`{"id":4,"occurred_at":"not-a-timestamp"}`),
		},
	})); err == nil {
		t.Fatal("expected invalid SQL Server temporal value to be rejected")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.sqlbatch_live_temporal`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("prevalidated temporal request count=%d err=%v", count, err)
	}

	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_identity_items",
		Data:      [][]byte{[]byte(`{"id":42,"name":"preserved"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT name FROM dbo.sqlbatch_live_identity_items WHERE id = 42`).Scan(&name); err != nil || name != "preserved" {
		t.Fatalf("identity row: name=%q err=%v", name, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_nonkey_identity",
		Data:      [][]byte{[]byte(`{"key_id":1,"generated_id":42,"name":"rejected","computed":43}`)},
	})); err == nil {
		t.Fatal("expected supplied computed column to be rejected")
	}
	for _, data := range []string{
		`{"key_id":1,"generated_id":42,"name":"inserted"}`,
		`{"key_id":1,"generated_id":42,"name":"updated"}`,
	} {
		if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
			Namespace: "dbo.sqlbatch_live_nonkey_identity",
			Data:      [][]byte{[]byte(data)},
		})); err != nil {
			t.Fatalf("non-key identity write: %v", err)
		}
	}
	var generatedID, computed int64
	if err := db.QueryRowContext(ctx, `SELECT generated_id, computed, name FROM dbo.sqlbatch_live_nonkey_identity WHERE key_id = 1`).Scan(&generatedID, &computed, &name); err != nil || generatedID != 42 || computed != 43 || name != "updated" {
		t.Fatalf("non-key identity row: generated=%d computed=%d name=%q err=%v", generatedID, computed, name, err)
	}

	const guidText = "00010203-0405-0607-0809-0a0b0c0d0e0f"
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "dbo.sqlbatch_live_guid_items",
		Data:      [][]byte{[]byte(`{"id":"` + guidText + `","occurred_at":"2026-10-15T12:34:56.123456-07:00","amount":1234567890.0123456789,"payload":"AAH+/w=="}`)},
	})); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT id, amount, payload FROM dbo.sqlbatch_live_guid_items WHERE id = '`+guidText+`'`)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		t.Fatal(err)
	}
	if !rows.Next() {
		rows.Close()
		t.Fatal("SQL Server source query returned no row")
	}
	sourceRow, err := ParseRow(columns, columnTypes, rows, &Mapping{}, c.dialect)
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if sourceRow["id"] != "00010203-0405-0607-0809-0A0B0C0D0E0F" {
		t.Fatalf("source uniqueidentifier = %#v", sourceRow["id"])
	}
	if sourceRow["amount"] != "1234567890.0123456789" {
		t.Fatalf("source decimal = %#v", sourceRow["amount"])
	}
	sourceJSON, err := json.Marshal(sourceRow)
	if err != nil {
		t.Fatal(err)
	}
	var sourceDocument map[string]any
	if err := json.Unmarshal(sourceJSON, &sourceDocument); err != nil {
		t.Fatal(err)
	}
	if sourceDocument["payload"] != "AAH+/w==" {
		t.Fatalf("source binary = %#v", sourceDocument["payload"])
	}
	c.config.Mappings = map[string]Mapping{
		"dbo.sqlbatch_live_guid_items": {
			Namespace:      "dbo.sqlbatch_live_guid_items",
			CountQuery:     `SELECT COUNT(*) FROM dbo.sqlbatch_live_guid_items`,
			Query:          `SELECT id, occurred_at, amount, payload FROM dbo.sqlbatch_live_guid_items`,
			PartitionQuery: `SELECT id FROM dbo.sqlbatch_live_guid_items`,
			Limit:          100,
			Cols:           []string{"id"},
			Changes: []ChangeMapping{{
				InitialCursorQuery: `SELECT TOP (1) id FROM dbo.sqlbatch_live_guid_items ORDER BY id DESC`,
			}},
		},
	}
	plan, err := c.GeneratePlan(ctx, connect.NewRequest(&adiomv1.GeneratePlanRequest{
		Namespaces:  []string{"dbo.sqlbatch_live_guid_items"},
		InitialSync: true,
		Updates:     true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Msg.GetPartitions()) == 0 {
		t.Fatal("SQL Server plan returned no initial-sync partitions")
	}
	partition, err := DecodePartitionCursor(plan.Msg.GetPartitions()[0].GetCursor())
	if err != nil {
		t.Fatal(err)
	}
	if len(partition.High) != 1 || partition.High[0] != "00010203-0405-0607-0809-0A0B0C0D0E0F" {
		t.Fatalf("partition GUID boundary = %#v", partition.High)
	}
	if len(plan.Msg.GetUpdatesPartitions()) != 1 {
		t.Fatalf("update partitions = %d", len(plan.Msg.GetUpdatesPartitions()))
	}
	updateCursor, err := DecodeUpdateCursor(plan.Msg.GetUpdatesPartitions()[0].GetCursor())
	if err != nil {
		t.Fatal(err)
	}
	if len(updateCursor.Cursor) != 1 || updateCursor.Cursor[0] != "00010203-0405-0607-0809-0A0B0C0D0E0F" {
		t.Fatalf("update GUID cursor = %#v", updateCursor.Cursor)
	}
	guidType, guidData, err := bson.MarshalValue(bson.Binary{
		Subtype: bson.TypeBinaryUUID,
		Data:    []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dbo.sqlbatch_live_guid_items",
		Updates: []*adiomv1.Update{{
			Id:   []*adiomv1.BsonValue{{Name: "id", Type: uint32(guidType), Data: guidData}},
			Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: []byte(`{"occurred_at":"2026-10-16T01:02:03.654321Z","amount":1234567890.0123456789,"payload":"AAH+/w=="}`),
		}},
	})); err != nil {
		t.Fatal(err)
	}
	assertRFC3339SourceRow(t, ctx, db, c.dialect, `SELECT occurred_at FROM dbo.sqlbatch_live_guid_items WHERE id = '`+guidText+`'`, "occurred_at")

	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "dbo.sqlbatch_live_items",
		Updates:   []*adiomv1.Update{{Id: id, Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE}},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM dbo.sqlbatch_live_items WHERE tenant = 'a' AND id = 1`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("delete count=%d err=%v", count, err)
	}
}

func TestOracleSinkLive(t *testing.T) {
	connectionURL := os.Getenv("ORACLE_TEST_URL")
	if connectionURL == "" {
		t.Skip("ORACLE_TEST_URL is not set")
	}
	ctx := context.Background()
	db, err := sql.Open("oracle", connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, table := range []string{"SQLBATCH_LIVE_ITEMS", "SQLBATCH_LIVE_IDENTITY", "SQLBATCH_LIVE_ALWAYS", "SQLBATCH_LIVE_DECIMAL_KEYS", "SQLBATCH_LIVE_TEMPORAL"} {
		statement := `BEGIN EXECUTE IMMEDIATE 'DROP TABLE ` + table + ` PURGE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -942 THEN RAISE; END IF; END;`
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	for _, statement := range []string{
		`CREATE TABLE SQLBATCH_LIVE_ITEMS (TENANT VARCHAR2(20) NOT NULL, ID NUMBER(19) NOT NULL, NAME VARCHAR2(100) NOT NULL CHECK (NAME <> 'forbidden'), AMOUNT NUMBER(20,4) DEFAULT 42, PAYLOAD BLOB, RAWPAYLOAD RAW(16), CONSTRAINT SQLBATCH_LIVE_ITEMS_PK PRIMARY KEY (TENANT, ID))`,
		`CREATE TABLE SQLBATCH_LIVE_IDENTITY (ID NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, NAME VARCHAR2(100) NOT NULL)`,
		`CREATE TABLE SQLBATCH_LIVE_ALWAYS (ID NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, NAME VARCHAR2(100) NOT NULL)`,
		`CREATE TABLE SQLBATCH_LIVE_DECIMAL_KEYS (ID NUMBER(20,4) PRIMARY KEY, NAME VARCHAR2(100) NOT NULL)`,
		`CREATE TABLE SQLBATCH_LIVE_TEMPORAL (ID NUMBER PRIMARY KEY, OCCURRED_AT TIMESTAMP(9), EVENT_DATE DATE, ZONED_AT TIMESTAMP(9) WITH TIME ZONE, LOCAL_ZONED_AT TIMESTAMP(9) WITH LOCAL TIME ZONE)`,
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}

	c := &conn{c: db, dialect: dialects.Oracle{}}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_ITEMS",
		Data: [][]byte{
			[]byte(`{"TENANT":"a","ID":1,"NAME":"first","PAYLOAD":"AAH+/w==","RAWPAYLOAD":"AAE="}`),
			[]byte(`{"TENANT":"b","ID":2,"NAME":"second"}`),
		},
	})); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM SQLBATCH_LIVE_ITEMS WHERE AMOUNT = 42`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("defaulted row count=%d err=%v", count, err)
	}
	var payloadHex string
	if err := db.QueryRowContext(ctx, `SELECT RAWTOHEX(DBMS_LOB.SUBSTR(PAYLOAD, 4, 1)) FROM SQLBATCH_LIVE_ITEMS WHERE TENANT = 'a' AND ID = 1`).Scan(&payloadHex); err != nil || payloadHex != "0001FEFF" {
		t.Fatalf("blob round trip: value=%q err=%v", payloadHex, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT RAWTOHEX(RAWPAYLOAD) FROM SQLBATCH_LIVE_ITEMS WHERE TENANT = 'a' AND ID = 1`).Scan(&payloadHex); err != nil || payloadHex != "0001" {
		t.Fatalf("raw round trip: value=%q err=%v", payloadHex, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_ITEMS",
		Data: [][]byte{
			[]byte(`{"TENANT":"c","ID":3,"NAME":"whole","AMOUNT":1}`),
			[]byte(`{"TENANT":"d","ID":4,"NAME":"fractional","AMOUNT":1.5}`),
		},
	})); err != nil {
		t.Fatalf("mixed decimal insert batch: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM SQLBATCH_LIVE_ITEMS WHERE (TENANT = 'c' AND AMOUNT = 1) OR (TENANT = 'd' AND AMOUNT = 1.5)`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("mixed decimal insert count=%d err=%v", count, err)
	}

	id, _, err := ToID([]string{"TENANT", "ID"}, []any{"a", int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE SQLBATCH_LIVE_ITEMS SET AMOUNT = 99 WHERE TENANT = 'a' AND ID = 1`); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "SQLBATCH_LIVE_ITEMS",
		Updates: []*adiomv1.Update{{
			Id: id, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE,
			Data: []byte(`{"NAME":"updated"}`),
		}},
	})); err != nil {
		t.Fatal(err)
	}
	var name, amount string
	if err := db.QueryRowContext(ctx, `SELECT NAME, TO_CHAR(AMOUNT, 'FM9999999999999990D0000', 'NLS_NUMERIC_CHARACTERS=''.,''') FROM SQLBATCH_LIVE_ITEMS WHERE TENANT = 'a' AND ID = 1`).Scan(&name, &amount); err != nil || name != "updated" || amount != "42.0000" {
		t.Fatalf("updated row: name=%q amount=%q err=%v", name, amount, err)
	}
	mixedUpdates := make([]*adiomv1.Update, 0, 2)
	for _, item := range []struct {
		tenant string
		id     int64
		data   string
	}{
		{tenant: "c", id: 3, data: `{"NAME":"whole-updated","AMOUNT":2}`},
		{tenant: "d", id: 4, data: `{"NAME":"fractional-updated","AMOUNT":2.5}`},
	} {
		itemID, _, err := ToID([]string{"TENANT", "ID"}, []any{item.tenant, item.id})
		if err != nil {
			t.Fatal(err)
		}
		mixedUpdates = append(mixedUpdates, &adiomv1.Update{Id: itemID, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: []byte(item.data)})
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "SQLBATCH_LIVE_ITEMS",
		Updates:   mixedUpdates,
	})); err != nil {
		t.Fatalf("mixed decimal update batch: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM SQLBATCH_LIVE_ITEMS WHERE (TENANT = 'c' AND AMOUNT = 2) OR (TENANT = 'd' AND AMOUNT = 2.5)`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("mixed decimal update count=%d err=%v", count, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_DECIMAL_KEYS",
		Data: [][]byte{
			[]byte(`{"ID":1,"NAME":"whole"}`),
			[]byte(`{"ID":1.5,"NAME":"fractional"}`),
		},
	})); err != nil {
		t.Fatalf("mixed decimal key insert batch: %v", err)
	}
	decimalID, err := bson.ParseDecimal128("1.5")
	if err != nil {
		t.Fatal(err)
	}
	decimalKeyUpdates := make([]*adiomv1.Update, 0, 2)
	for _, item := range []struct {
		id   any
		name string
	}{
		{id: int64(1), name: "whole-updated"},
		{id: decimalID, name: "fractional-updated"},
	} {
		itemID, _, err := ToID([]string{"ID"}, []any{item.id})
		if err != nil {
			t.Fatal(err)
		}
		decimalKeyUpdates = append(decimalKeyUpdates, &adiomv1.Update{Id: itemID, Type: adiomv1.UpdateType_UPDATE_TYPE_UPDATE, Data: []byte(`{"NAME":"` + item.name + `"}`)})
	}
	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "SQLBATCH_LIVE_DECIMAL_KEYS",
		Updates:   decimalKeyUpdates,
	})); err != nil {
		t.Fatalf("mixed decimal key update batch: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM SQLBATCH_LIVE_DECIMAL_KEYS WHERE NAME LIKE '%-updated'`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("mixed decimal key update count=%d err=%v", count, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_TEMPORAL",
		Data: [][]byte{
			[]byte(`{"ID":1,"OCCURRED_AT":"2026-10-19T12:34:56.123456789+05:30","EVENT_DATE":"2026-10-19T12:34:56+05:30","ZONED_AT":"2026-10-19T12:34:56.123456789+05:30","LOCAL_ZONED_AT":"2026-10-19T12:34:56.123456789+05:30"}`),
			[]byte(`{"ID":2}`),
		},
	})); err != nil {
		t.Fatalf("temporal insert batch: %v", err)
	}
	var occurredAt, eventDate, zonedOffset, localZonedUTC string
	if err := db.QueryRowContext(ctx, `SELECT TO_CHAR(OCCURRED_AT, 'YYYY-MM-DD HH24:MI:SS.FF9'), TO_CHAR(EVENT_DATE, 'YYYY-MM-DD HH24:MI:SS'), TO_CHAR(ZONED_AT, 'TZH:TZM'), TO_CHAR(SYS_EXTRACT_UTC(LOCAL_ZONED_AT), 'YYYY-MM-DD HH24:MI:SS.FF9') FROM SQLBATCH_LIVE_TEMPORAL WHERE ID = 1`).Scan(&occurredAt, &eventDate, &zonedOffset, &localZonedUTC); err != nil {
		t.Fatal(err)
	}
	if occurredAt != "2026-10-19 07:04:56.123456789" || eventDate != "2026-10-19 07:04:56" || zonedOffset != "+05:30" || localZonedUTC != "2026-10-19 07:04:56.123456789" {
		t.Fatalf("temporal values: occurred=%q date=%q zoned offset=%q local zoned UTC=%q", occurredAt, eventDate, zonedOffset, localZonedUTC)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_TEMPORAL",
		Data: [][]byte{
			[]byte(`{"ID":3}`),
			[]byte(`{"ID":4,"OCCURRED_AT":"not-a-timestamp"}`),
		},
	})); err == nil {
		t.Fatal("expected invalid Oracle temporal value to be rejected")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM SQLBATCH_LIVE_TEMPORAL`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("prevalidated temporal request count=%d err=%v", count, err)
	}

	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_IDENTITY",
		Data:      [][]byte{[]byte(`{"ID":42,"NAME":"preserved"}`)},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT NAME FROM SQLBATCH_LIVE_IDENTITY WHERE ID = 42`).Scan(&name); err != nil || name != "preserved" {
		t.Fatalf("identity row: name=%q err=%v", name, err)
	}
	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_ALWAYS",
		Data:      [][]byte{[]byte(`{"ID":42,"NAME":"rejected"}`)},
	})); err == nil || connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected GENERATED ALWAYS identity write to fail as invalid argument, got %v", err)
	}

	if _, err := c.WriteData(ctx, connect.NewRequest(&adiomv1.WriteDataRequest{
		Namespace: "SQLBATCH_LIVE_TEMPORAL",
		Data: [][]byte{[]byte(
			`{"ID":1,"OCCURRED_AT":"2026-10-15T12:34:56.123456-07:00","EVENT_DATE":"2026-10-15T00:00:00Z"}`,
		)},
	})); err != nil {
		t.Fatal(err)
	}
	var sourceOccurredAt, sourceEventDate time.Time
	if err := db.QueryRowContext(ctx, `SELECT OCCURRED_AT, EVENT_DATE FROM SQLBATCH_LIVE_TEMPORAL WHERE ID = 1`).Scan(&sourceOccurredAt, &sourceEventDate); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 10, 15, 19, 34, 56, 123456000, time.UTC); !sourceOccurredAt.Equal(want) {
		t.Fatalf("Oracle timestamp = %v, want instant %v", sourceOccurredAt, want)
	}
	if sourceEventDate.Year() != 2026 || sourceEventDate.Month() != time.October || sourceEventDate.Day() != 15 {
		t.Fatalf("Oracle date = %v", sourceEventDate)
	}
	assertRFC3339SourceRow(t, ctx, db, c.dialect, `SELECT OCCURRED_AT, EVENT_DATE FROM SQLBATCH_LIVE_TEMPORAL WHERE ID = 1`, "OCCURRED_AT", "EVENT_DATE")

	if _, err := c.WriteUpdates(ctx, connect.NewRequest(&adiomv1.WriteUpdatesRequest{
		Namespace: "SQLBATCH_LIVE_ITEMS",
		Updates:   []*adiomv1.Update{{Id: id, Type: adiomv1.UpdateType_UPDATE_TYPE_DELETE}},
	})); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM SQLBATCH_LIVE_ITEMS WHERE TENANT = 'a' AND ID = 1`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("delete count=%d err=%v", count, err)
	}
}
