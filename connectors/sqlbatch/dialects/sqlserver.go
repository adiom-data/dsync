package dialects

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
)

type SQLServer struct {
	// GuidConversion mirrors go-mssqldb's connection setting. Without it the
	// driver exposes uniqueidentifier values in TDS byte order.
	GuidConversion bool
}

func (d SQLServer) ConvertDriverValue(column ColumnMetadata, value any) (any, error) {
	data, ok := value.([]byte)
	if !ok {
		return value, nil
	}
	switch strings.ToLower(column.DatabaseType) {
	case "decimal", "numeric", "money", "smallmoney":
		return string(data), nil
	case "uniqueidentifier":
		if len(data) != 16 {
			return nil, fmt.Errorf("uniqueidentifier column %q has %d bytes, expected 16", column.Name, len(data))
		}
		var identifier mssql.UniqueIdentifier
		if d.GuidConversion {
			copy(identifier[:], data)
		} else if err := identifier.Scan(data); err != nil {
			return nil, fmt.Errorf("convert uniqueidentifier column %q: %w", column.Name, err)
		}
		return identifier.String(), nil
	default:
		return value, nil
	}
}

func (SQLServer) MaxQueryParameters() int {
	// Leave room below SQL Server's 2,100 parameter limit for driver and
	// statement-level parameters.
	return 2000
}

func (d SQLServer) InspectTable(ctx context.Context, db *sql.DB, namespace string) (TableMetadata, error) {
	schema, table, err := SplitNamespace(namespace)
	if err != nil {
		return TableMetadata{}, err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.name,
		       bt.name,
		       CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END,
		       CASE WHEN dc.object_id IS NULL THEN 'NO' ELSE 'YES' END,
		       CASE WHEN c.is_computed = 1 OR c.generated_always_type <> 0 OR bt.name IN ('timestamp', 'rowversion') THEN 'YES' ELSE 'NO' END,
		       CASE WHEN c.is_identity = 1 THEN 'YES' ELSE 'NO' END,
		       CASE WHEN c.is_identity = 1 THEN 'ALWAYS' ELSE '' END,
		       COALESCE(pk.key_ordinal, 0)
		FROM sys.tables t
		JOIN sys.schemas s ON s.schema_id = t.schema_id
		JOIN sys.columns c ON c.object_id = t.object_id
		JOIN sys.types bt
		  ON bt.system_type_id = c.system_type_id
		 AND bt.user_type_id = bt.system_type_id
		LEFT JOIN sys.default_constraints dc ON dc.object_id = c.default_object_id
		LEFT JOIN (
			SELECT ic.object_id, ic.column_id, ic.key_ordinal
			FROM sys.indexes i
			JOIN sys.index_columns ic
			  ON ic.object_id = i.object_id
			 AND ic.index_id = i.index_id
			WHERE i.is_primary_key = 1
		) pk
		  ON pk.object_id = c.object_id
		 AND pk.column_id = c.column_id
		WHERE s.name = COALESCE(NULLIF(@p1, ''), SCHEMA_NAME())
		  AND t.name = @p2
		ORDER BY c.column_id`, schema, table)
	if err != nil {
		return TableMetadata{}, err
	}
	defer rows.Close()
	return ScanTableMetadata(rows)
}

func (SQLServer) ConvertJSONValue(column ColumnMetadata, value any) (any, error) {
	value, err := ConvertJSONValue(column, value, "binary", "varbinary", "image", "timestamp", "rowversion")
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(column.DatabaseType, "uniqueidentifier") {
		if data, ok := value.([]byte); ok {
			if len(data) != 16 {
				return nil, fmt.Errorf("uniqueidentifier column %q has %d bytes, expected 16", column.Name, len(data))
			}
			var identifier mssql.UniqueIdentifier
			copy(identifier[:], data)
			value = identifier
		}
	}
	return ConvertRFC3339Value(column, value, "date", "datetime", "datetime2", "smalldatetime", "datetimeoffset")
}

func (d SQLServer) UpsertQuery(namespace string, keys, columns, columnTypes, defaultColumns []string, count int, overrideIdentity bool) (string, error) {
	nonKeys := make([]string, 0, len(columns))
	for _, column := range columns {
		if !slices.Contains(keys, column) {
			nonKeys = append(nonKeys, column)
		}
	}
	var query strings.Builder
	query.WriteString("MERGE INTO " + QuoteNamespace(namespace) + " AS target USING (VALUES " + d.valueRows(columnTypes, count) + ") AS source (" + QuotedIdentifiers(columns) + ") ON " + JoinPredicate(keys, "target", "source"))
	if len(nonKeys) > 0 || len(defaultColumns) > 0 {
		assignments := make([]string, 0, 2)
		if len(nonKeys) > 0 {
			assignments = append(assignments, Assignments(nonKeys, "target", "source"))
		}
		if len(defaultColumns) > 0 {
			assignments = append(assignments, DefaultAssignments(defaultColumns, "target"))
		}
		query.WriteString(" WHEN MATCHED THEN UPDATE SET " + strings.Join(assignments, ", "))
	}
	query.WriteString(" WHEN NOT MATCHED THEN INSERT (" + QuotedIdentifiers(columns) + ") VALUES (" + SourceColumns(columns, "source") + ");")
	if !overrideIdentity {
		return query.String(), nil
	}
	table := QuoteNamespace(namespace)
	return "SET IDENTITY_INSERT " + table + " ON; BEGIN TRY " + query.String() + " SET IDENTITY_INSERT " + table + " OFF; END TRY BEGIN CATCH SET IDENTITY_INSERT " + table + " OFF; THROW; END CATCH", nil
}

func (d SQLServer) valueRows(columnTypes []string, count int) string {
	rows := make([]string, count)
	for row := range count {
		values := make([]string, len(columnTypes))
		for column, databaseType := range columnTypes {
			value := d.Placeholder(row*len(columnTypes) + column)
			switch strings.ToLower(databaseType) {
			case "binary", "varbinary", "image":
				value = "CAST(" + value + " AS varbinary(max))"
			}
			values[column] = value
		}
		rows[row] = "(" + strings.Join(values, ", ") + ")"
	}
	return strings.Join(rows, ", ")
}

func (d SQLServer) DeleteQuery(namespace string, keys []string, count int) string {
	return DeleteQuery(namespace, keys, count, d.Placeholder)
}

// KeySub implements sqlbatch.SQLDialect.
func (d SQLServer) KeySub(cols []string, count int) string {
	return KeySub(len(cols), count, d.Placeholder)
}

func (d SQLServer) ListDataTemplate(cols []string, query string) string {
	return "WITH QUERY AS (" + query + ") SELECT TOP %v * FROM QUERY%v ORDER BY \"" + strings.Join(cols, "\", \"") + "\""
}

func (d SQLServer) ListDataApplyTemplate(cols []string, template string, limit int, low []any, high []any) (string, []any) {
	var finalQuery string
	var subs []any
	if len(low) > 0 {
		if len(high) > 0 {
			where := fmt.Sprintf(" WHERE %v AND %v", LowQuery(cols, d.Placeholder, 0), HighQuery(cols, d.Placeholder, len(cols)))
			finalQuery = fmt.Sprintf(template, limit, where)
			subs = append(subs, low...)
			subs = append(subs, high...)
		} else {
			finalQuery = fmt.Sprintf(template, limit, " WHERE "+LowQuery(cols, d.Placeholder, 0))
			subs = low
		}
	} else {
		if len(high) > 0 {
			finalQuery = fmt.Sprintf(template, limit, " WHERE "+HighQuery(cols, d.Placeholder, 0))
			subs = high
		} else {
			finalQuery = fmt.Sprintf(template, limit, "")
		}
	}
	return finalQuery, subs
}

func (d SQLServer) FetchTemplate(cols []string, query string) string {
	return FetchTemplate(cols, query, SelectKeysTemplate(cols))
}

func (d SQLServer) Placeholder(i int) string {
	return "@p" + strconv.Itoa(i+1)
}
