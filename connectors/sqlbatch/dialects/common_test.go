package dialects_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/adiom-data/dsync/connectors/sqlbatch/dialects"
	mssql "github.com/microsoft/go-mssqldb"
	"github.com/stretchr/testify/assert"
)

func TestTemporalValuesUseRFC3339(t *testing.T) {
	want := time.Date(2026, 10, 15, 12, 34, 56, 123456000, time.FixedZone("-0700", -7*60*60))
	tests := []struct {
		name    string
		dialect interface {
			ConvertJSONValue(dialects.ColumnMetadata, any) (any, error)
		}
		databaseType string
	}{
		{name: "postgres", dialect: dialects.Postgres{}, databaseType: "timestamptz"},
		{name: "sqlserver", dialect: dialects.SQLServer{}, databaseType: "datetimeoffset"},
		{name: "oracle", dialect: dialects.Oracle{}, databaseType: "TIMESTAMP WITH TIME ZONE"},
		{name: "oracle precision", dialect: dialects.Oracle{}, databaseType: "TIMESTAMP(6) WITH TIME ZONE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "occurred_at", DatabaseType: test.databaseType}, "2026-10-15T12:34:56.123456-07:00")
			if err != nil {
				t.Fatal(err)
			}
			if !got.(time.Time).Equal(want) {
				t.Fatalf("time = %v, want %v", got, want)
			}
		})
	}
}

func TestTemporalValuesRejectNonRFC3339(t *testing.T) {
	_, err := (dialects.Oracle{}).ConvertJSONValue(
		dialects.ColumnMetadata{Name: "occurred_at", DatabaseType: "DATE"},
		"15-OCT-26",
	)
	if err == nil {
		t.Fatal("expected invalid RFC3339 value to fail")
	}
}

func TestSQLServerUniqueIdentifierFromBinaryValue(t *testing.T) {
	got, err := (dialects.SQLServer{}).ConvertJSONValue(
		dialects.ColumnMetadata{Name: "id", DatabaseType: "uniqueidentifier"},
		[]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := mssql.UniqueIdentifier{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if got != want {
		t.Fatalf("uniqueidentifier = %#v, want %#v", got, want)
	}
}

func TestSQLServerDriverValuesUseUnambiguousJSONTypes(t *testing.T) {
	column := dialects.ColumnMetadata{Name: "id", DatabaseType: "uniqueidentifier"}
	rawTDS := []byte{3, 2, 1, 0, 5, 4, 7, 6, 8, 9, 10, 11, 12, 13, 14, 15}
	got, err := (dialects.SQLServer{}).ConvertDriverValue(column, rawTDS)
	if err != nil {
		t.Fatal(err)
	}
	const want = "00010203-0405-0607-0809-0A0B0C0D0E0F"
	if got != want {
		t.Fatalf("uniqueidentifier = %#v, want %q", got, want)
	}

	canonical := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	got, err = (dialects.SQLServer{GuidConversion: true}).ConvertDriverValue(column, canonical)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("converted uniqueidentifier = %#v, want %q", got, want)
	}

	got, err = (dialects.SQLServer{}).ConvertDriverValue(
		dialects.ColumnMetadata{Name: "amount", DatabaseType: "decimal"},
		[]byte("1234567890.0123456789"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got != "1234567890.0123456789" {
		t.Fatalf("decimal = %#v", got)
	}

	binary := []byte{0, 1, 254, 255}
	got, err = (dialects.SQLServer{}).ConvertDriverValue(
		dialects.ColumnMetadata{Name: "payload", DatabaseType: "varbinary"},
		binary,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.([]byte), binary) {
		t.Fatalf("binary = %#v", got)
	}
}

func upsertQuery(t *testing.T, dialect interface {
	UpsertQuery(string, []string, []string, []string, []string, []string, int, bool) (string, error)
}, namespace string, keys, columns, defaultColumns []string, count int, overrideIdentity bool) string {
	t.Helper()
	columnTypes := make([]string, len(columns))
	query, err := dialect.UpsertQuery(namespace, keys, columns, columnTypes, defaultColumns, nil, count, overrideIdentity)
	assert.NoError(t, err)
	return query
}

func TestKeySub(t *testing.T) {
	p := func(i int) string { return fmt.Sprintf("X%v", i) }
	assert.Equal(t, "(X0)", dialects.KeySub(1, 1, p))
	assert.Equal(t, "(X0), (X1), (X2)", dialects.KeySub(1, 3, p))
	assert.Equal(t, "(X0, X1, X2), (X3, X4, X5)", dialects.KeySub(3, 2, p))
	assert.Equal(t, "(X0, X1, X2)", dialects.KeySub(3, 1, p))
}

func TestPostgresWriteQueries(t *testing.T) {
	d := dialects.Postgres{}
	assert.Equal(t,
		`INSERT INTO "public"."users" ("tenant", "id", "name") VALUES ($1, $2, $3) ON CONFLICT ("tenant", "id") DO UPDATE SET "name" = EXCLUDED."name"`,
		upsertQuery(t, d, "public.users", []string{"tenant", "id"}, []string{"tenant", "id", "name"}, nil, 1, false),
	)
	assert.Equal(t,
		`DELETE FROM "public"."users" WHERE ("tenant" = $1 AND "id" = $2)`,
		d.DeleteQuery("public.users", []string{"tenant", "id"}, 1),
	)
}

func TestPostgresKeyOnlyUpsert(t *testing.T) {
	d := dialects.Postgres{}
	assert.Equal(t,
		`INSERT INTO "items" ("id") VALUES ($1) ON CONFLICT ("id") DO NOTHING`,
		upsertQuery(t, d, "items", []string{"id"}, []string{"id"}, nil, 1, false),
	)
}

func TestSQLServerUpsertQuery(t *testing.T) {
	d := dialects.SQLServer{}
	assert.Equal(t,
		`MERGE INTO "dbo"."users" AS target USING (VALUES (@p1, @p2)) AS source ("id", "name") ON target."id" = source."id" WHEN MATCHED THEN UPDATE SET target."name" = source."name" WHEN NOT MATCHED THEN INSERT ("id", "name") VALUES (source."id", source."name");`,
		upsertQuery(t, d, "dbo.users", []string{"id"}, []string{"id", "name"}, nil, 1, false),
	)
}

func TestOracleUpsertQuery(t *testing.T) {
	d := dialects.Oracle{}
	assert.Equal(t,
		`MERGE INTO "APP"."USERS" target USING (SELECT :1 AS "ID", :2 AS "NAME" FROM DUAL) source ON (target."ID" = source."ID") WHEN MATCHED THEN UPDATE SET target."NAME" = source."NAME" WHEN NOT MATCHED THEN INSERT ("ID", "NAME") VALUES (source."ID", source."NAME")`,
		upsertQuery(t, d, "APP.USERS", []string{"ID"}, []string{"ID", "NAME"}, nil, 1, false),
	)
}

func TestMultiRowWriteQueries(t *testing.T) {
	assert.Equal(t,
		`INSERT INTO "items" ("id", "name") VALUES ($1, $2), ($3, $4) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"`,
		upsertQuery(t, dialects.Postgres{}, "items", []string{"id"}, []string{"id", "name"}, nil, 2, false),
	)
	assert.Equal(t,
		`DELETE FROM "items" WHERE ("id" = $1) OR ("id" = $2)`,
		(dialects.Postgres{}).DeleteQuery("items", []string{"id"}, 2),
	)
	assert.Contains(t,
		upsertQuery(t, dialects.SQLServer{}, "items", []string{"id"}, []string{"id", "name"}, nil, 2, false),
		`VALUES (@p1, @p2), (@p3, @p4)`,
	)
	assert.Contains(t,
		upsertQuery(t, dialects.Oracle{}, "ITEMS", []string{"ID"}, []string{"ID", "NAME"}, nil, 2, false),
		`SELECT :1 AS "ID", :2 AS "NAME" FROM DUAL UNION ALL SELECT :3 AS "ID", :4 AS "NAME" FROM DUAL`,
	)
}

func TestWriteQueriesResetOmittedColumnsToDefault(t *testing.T) {
	assert.Equal(t,
		`INSERT INTO "items" ("id", "name") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "status" = DEFAULT`,
		upsertQuery(t, dialects.Postgres{}, "items", []string{"id"}, []string{"id", "name"}, []string{"status"}, 1, false),
	)
	assert.Contains(t,
		upsertQuery(t, dialects.SQLServer{}, "items", []string{"id"}, []string{"id", "name"}, []string{"status"}, 1, false),
		`target."name" = source."name", target."status" = DEFAULT`,
	)
	assert.Contains(t,
		upsertQuery(t, dialects.Oracle{}, "ITEMS", []string{"ID"}, []string{"ID", "NAME"}, []string{"STATUS"}, 1, false),
		`target."NAME" = source."NAME", target."STATUS" = DEFAULT`,
	)
}

func TestGeneratedIdentityWriteQueries(t *testing.T) {
	assert.Contains(t,
		upsertQuery(t, dialects.Postgres{}, "items", []string{"id"}, []string{"id", "name"}, nil, 1, true),
		`) OVERRIDING SYSTEM VALUE VALUES (`,
	)
	assert.Contains(t,
		upsertQuery(t, dialects.SQLServer{}, "items", []string{"id"}, []string{"id", "name"}, nil, 1, true),
		`SET IDENTITY_INSERT "items" ON; BEGIN TRY MERGE`,
	)
	_, err := (dialects.Oracle{}).UpsertQuery("ITEMS", []string{"ID"}, []string{"ID"}, []string{"NUMBER"}, nil, []string{"ID"}, 1, true)
	assert.ErrorContains(t, err, "cannot be written explicitly")
}

func TestIdentityColumnsAreInsertOnly(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect interface {
			UpsertQuery(string, []string, []string, []string, []string, []string, int, bool) (string, error)
		}
	}{
		{name: "Postgres", dialect: dialects.Postgres{}},
		{name: "SQL Server", dialect: dialects.SQLServer{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			query, err := test.dialect.UpsertQuery(
				"items",
				[]string{"id"},
				[]string{"id", "sequence", "name"},
				[]string{"bigint", "bigint", "text"},
				nil,
				[]string{"sequence"},
				1,
				true,
			)
			assert.NoError(t, err)
			assert.NotContains(t, query, `target."sequence" = source."sequence"`)
			assert.NotContains(t, query, `"sequence" = EXCLUDED."sequence"`)
			assert.Contains(t, query, `"sequence"`)
		})
	}
}

func TestSQLServerCastsBinaryParameters(t *testing.T) {
	query, err := (dialects.SQLServer{}).UpsertQuery(
		"items",
		[]string{"id"},
		[]string{"id", "payload"},
		[]string{"bigint", "varbinary"},
		nil,
		nil,
		2,
		false,
	)
	assert.NoError(t, err)
	assert.Contains(t, query, `VALUES (@p1, CAST(@p2 AS varbinary(max))), (@p3, CAST(@p4 AS varbinary(max)))`)
}

func TestOracleConvertsBlobParameters(t *testing.T) {
	query, err := (dialects.Oracle{}).UpsertQuery(
		"ITEMS",
		[]string{"ID"},
		[]string{"ID", "PAYLOAD"},
		[]string{"NUMBER", "BLOB"},
		nil,
		nil,
		2,
		false,
	)
	assert.NoError(t, err)
	assert.Contains(t, query, `SELECT CAST(:1 AS NUMBER) AS "ID", TO_BLOB(:2) AS "PAYLOAD" FROM DUAL`)
	assert.Contains(t, query, `SELECT CAST(:3 AS NUMBER) AS "ID", TO_BLOB(:4) AS "PAYLOAD" FROM DUAL`)
}

func TestOracleCastsNumericParameters(t *testing.T) {
	query, err := (dialects.Oracle{}).UpsertQuery(
		"ITEMS",
		[]string{"ID"},
		[]string{"ID", "AMOUNT"},
		[]string{"NUMBER", "BINARY_DOUBLE"},
		nil,
		nil,
		2,
		false,
	)
	assert.NoError(t, err)
	assert.Contains(t, query, `SELECT CAST(:1 AS NUMBER) AS "ID", CAST(:2 AS BINARY_DOUBLE) AS "AMOUNT" FROM DUAL`)
	assert.Contains(t, query, `SELECT CAST(:3 AS NUMBER) AS "ID", CAST(:4 AS BINARY_DOUBLE) AS "AMOUNT" FROM DUAL`)
}

func TestOracleTemporalConversionAndDeferredTypes(t *testing.T) {
	dialect := dialects.Oracle{}
	value, err := dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "PAYLOAD", DatabaseType: "RAW"}, nil)
	assert.NoError(t, err)
	assert.IsType(t, []byte(nil), value)

	value, err = dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "CREATED_AT", DatabaseType: "TIMESTAMP(6)"}, "2026-10-19T12:34:56+05:30")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 19, 7, 4, 56, 0, time.UTC), value)
	value, err = dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "EVENT_AT", DatabaseType: "DATE"}, "2026-10-19T12:34:56+05:30")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 19, 7, 4, 56, 0, time.UTC), value)
	value, err = dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "EVENT_AT", DatabaseType: "DATE"}, "2026-10-19")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 19, 0, 0, 0, 0, time.UTC), value)
	value, err = dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "CREATED_AT", DatabaseType: "TIMESTAMP(6) WITH TIME ZONE"}, "2026-10-19T12:34:56+05:30")
	assert.NoError(t, err)
	assert.Equal(t, 5*60+30, offsetMinutes(value.(time.Time)))
	_, err = dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "LEGACY", DatabaseType: "LONG RAW"}, "AAE=")
	assert.ErrorContains(t, err, "does not support supplied values")
}

func TestOraclePreservesTimestampPrecisionInCasts(t *testing.T) {
	query, err := (dialects.Oracle{}).UpsertQuery(
		"ITEMS",
		[]string{"ID"},
		[]string{"ID", "PLAIN", "ZONED", "LOCAL_ZONED"},
		[]string{"NUMBER", "TIMESTAMP(9)", "TIMESTAMP(9) WITH TIME ZONE", "TIMESTAMP(9) WITH LOCAL TIME ZONE"},
		nil,
		nil,
		1,
		false,
	)
	assert.NoError(t, err)
	assert.Contains(t, query, `CAST(:2 AS TIMESTAMP(9)) AS "PLAIN"`)
	assert.Contains(t, query, `CAST(:3 AS TIMESTAMP(9) WITH TIME ZONE) AS "ZONED"`)
	assert.Contains(t, query, `CAST(:4 AS TIMESTAMP(9) WITH LOCAL TIME ZONE) AS "LOCAL_ZONED"`)
}

func offsetMinutes(value time.Time) int {
	_, offset := value.Zone()
	return offset / 60
}

func TestSQLServerTemporalConversion(t *testing.T) {
	dialect := dialects.SQLServer{}
	value, err := dialect.ConvertJSONValue(dialects.ColumnMetadata{Name: "created_at", DatabaseType: "datetime2"}, "2026-10-19T12:34:56+05:30")
	assert.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 19, 7, 4, 56, 0, time.UTC), value)

	query, err := dialect.UpsertQuery("items", []string{"id"}, []string{"id", "created_at"}, []string{"bigint", "datetime2"}, nil, nil, 2, false)
	assert.NoError(t, err)
	assert.Contains(t, query, `VALUES (@p1, CAST(@p2 AS datetime2)), (@p3, CAST(@p4 AS datetime2))`)
}

func TestQuoteNamespaceEscapesIdentifiers(t *testing.T) {
	assert.Equal(t, `"odd""schema"."table"`, dialects.QuoteNamespace(`odd"schema.table`))
}

func TestPostgresJSONBinaryConversion(t *testing.T) {
	d := dialects.Postgres{}
	got, err := d.ConvertJSONValue(dialects.ColumnMetadata{Name: "payload", DatabaseType: "bytea"}, "AAH+/w==")
	assert.NoError(t, err)
	assert.True(t, bytes.Equal([]byte{0, 1, 0xfe, 0xff}, got.([]byte)))

	got, err = d.ConvertJSONValue(dialects.ColumnMetadata{Name: "payload", DatabaseType: "text"}, "AAH+/w==")
	assert.NoError(t, err)
	assert.Equal(t, "AAH+/w==", got)
}

func TestSplitNamespace(t *testing.T) {
	schema, table, err := dialects.SplitNamespace("public.items")
	assert.NoError(t, err)
	assert.Equal(t, "public", schema)
	assert.Equal(t, "items", table)

	schema, table, err = dialects.SplitNamespace("items")
	assert.NoError(t, err)
	assert.Empty(t, schema)
	assert.Equal(t, "items", table)
}
