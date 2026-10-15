package dialects

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type Postgres struct{}

func (Postgres) ConvertDriverValue(column ColumnMetadata, value any) (any, error) {
	return ConvertDriverValue(column, value)
}

func (Postgres) MaxQueryParameters() int {
	return 65535
}

func (d Postgres) InspectTable(ctx context.Context, db *sql.DB, namespace string) (TableMetadata, error) {
	schema, table, err := SplitNamespace(namespace)
	if err != nil {
		return TableMetadata{}, err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.column_name,
		       c.udt_name,
		       c.is_nullable,
		       CASE WHEN c.column_default IS NULL THEN 'NO' ELSE 'YES' END,
		       CASE WHEN c.is_generated = 'NEVER' THEN 'NO' ELSE 'YES' END,
		       c.is_identity,
		       COALESCE(c.identity_generation, ''),
		       COALESCE(pk.ordinal_position, 0)
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT kcu.table_schema, kcu.table_name, kcu.column_name, kcu.ordinal_position
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu
			  ON kcu.constraint_catalog = tc.constraint_catalog
			 AND kcu.constraint_schema = tc.constraint_schema
			 AND kcu.constraint_name = tc.constraint_name
			 AND kcu.table_name = tc.table_name
			WHERE tc.constraint_type = 'PRIMARY KEY'
		) pk
		  ON pk.table_schema = c.table_schema
		 AND pk.table_name = c.table_name
		 AND pk.column_name = c.column_name
		WHERE c.table_schema = COALESCE(NULLIF($1, ''), current_schema())
		  AND c.table_name = $2
		ORDER BY c.ordinal_position`, schema, table)
	if err != nil {
		return TableMetadata{}, err
	}
	defer rows.Close()
	return ScanTableMetadata(rows)
}

func (Postgres) ConvertJSONValue(column ColumnMetadata, value any) (any, error) {
	value, err := ConvertJSONValue(column, value, "bytea")
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(column.DatabaseType, "uuid") {
		if data, ok := value.([]byte); ok {
			if len(data) != 16 {
				return nil, fmt.Errorf("UUID column %q has %d bytes, expected 16", column.Name, len(data))
			}
			var uuid [16]byte
			copy(uuid[:], data)
			value = pgtype.UUID{Bytes: uuid, Valid: true}
		}
	}
	return ConvertRFC3339Value(column, value, "date", "timestamp", "timestamptz")
}

func (d Postgres) UpsertQuery(namespace string, keys, columns, _ []string, defaultColumns []string, count int, overrideIdentity bool) (string, error) {
	nonKeys := make([]string, 0, len(columns))
	for _, column := range columns {
		if !slices.Contains(keys, column) {
			nonKeys = append(nonKeys, column)
		}
	}
	query := "INSERT INTO " + QuoteNamespace(namespace) + " (" + QuotedIdentifiers(columns) + ")"
	if overrideIdentity {
		query += " OVERRIDING SYSTEM VALUE"
	}
	query += " VALUES " + ValueRows(len(columns), count, d.Placeholder) + " ON CONFLICT (" + QuotedIdentifiers(keys) + ") "
	if len(nonKeys) == 0 && len(defaultColumns) == 0 {
		return query + "DO NOTHING", nil
	}
	assignments := make([]string, 0, len(nonKeys)+len(defaultColumns))
	for _, column := range nonKeys {
		quoted := QuoteIdentifier(column)
		assignments = append(assignments, quoted+" = EXCLUDED."+quoted)
	}
	if len(defaultColumns) > 0 {
		assignments = append(assignments, DefaultAssignments(defaultColumns, ""))
	}
	return query + "DO UPDATE SET " + strings.Join(assignments, ", "), nil
}

func (d Postgres) DeleteQuery(namespace string, keys []string, count int) string {
	return DeleteQuery(namespace, keys, count, d.Placeholder)
}

// KeySub implements sqlbatch.SQLDialect.
func (d Postgres) KeySub(cols []string, count int) string {
	return KeySub(len(cols), count, d.Placeholder)
}

// FetchTemplate implements sqlbatch.SQLDialect.
func (d Postgres) FetchTemplate(cols []string, query string) string {
	return FetchTemplate(cols, query, SelectKeysTemplate(cols))
}

// ListDataApplyTemplate implements sqlbatch.SQLDialect.
func (d Postgres) ListDataApplyTemplate(cols []string, template string, limit int, low []any, high []any) (string, []any) {
	var finalQuery string
	var subs []any
	if len(low) > 0 {
		if len(high) > 0 {
			where := fmt.Sprintf(" WHERE %v AND %v", TupleQuery(cols, d.Placeholder, 0, ">="), TupleQuery(cols, d.Placeholder, len(cols), "<"))
			finalQuery = fmt.Sprintf(template, where, limit)
			subs = append(subs, low...)
			subs = append(subs, high...)
		} else {
			finalQuery = fmt.Sprintf(template, " WHERE "+TupleQuery(cols, d.Placeholder, 0, ">="), limit)
			subs = low
		}
	} else {
		if len(high) > 0 {
			finalQuery = fmt.Sprintf(template, " WHERE "+TupleQuery(cols, d.Placeholder, 0, "<"), limit)
			subs = high
		} else {
			finalQuery = fmt.Sprintf(template, "", limit)
		}
	}
	return finalQuery, subs
}

// ListDataTemplate implements sqlbatch.SQLDialect.
func (d Postgres) ListDataTemplate(cols []string, query string) string {
	return "WITH QUERY AS (" + query + ") SELECT * FROM QUERY%v ORDER BY \"" + strings.Join(cols, "\", \"") + "\" LIMIT %v"
}

// Placeholder implements sqlbatch.SQLDialect.
func (p Postgres) Placeholder(i int) string {
	return "$" + strconv.Itoa(i+1)
}
