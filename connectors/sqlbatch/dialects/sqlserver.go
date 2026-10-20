package dialects

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	mssql "github.com/microsoft/go-mssqldb"
	"github.com/shopspring/decimal"
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

func (SQLServer) SupportsIdentityOverride() bool {
	return true
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
	if sqlServerExactNumericType(column.DatabaseType) {
		var err error
		value, err = normalizeExactNumericExponent(value)
		if err != nil {
			return nil, fmt.Errorf("normalize exact numeric column %q: %w", column.Name, err)
		}
	}
	value, err := convertJSONValue(
		column,
		value,
		[]string{"binary", "varbinary", "image", "timestamp", "rowversion"},
		[]string{"decimal", "numeric", "money", "smallmoney", "float", "real"},
	)
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
	if temporalType, dateOnly, timeOnly, preserveOffset := sqlServerTemporalType(column.DatabaseType); temporalType != "" {
		kind := temporalTimestamp
		switch {
		case dateOnly:
			kind = temporalDate
		case timeOnly:
			kind = temporalTime
		}
		return convertTemporalValue(column, value, kind, preserveOffset)
	}
	return value, nil
}

func sqlServerExactNumericType(databaseType string) bool {
	switch strings.ToLower(databaseType) {
	case "decimal", "numeric", "money", "smallmoney":
		return true
	default:
		return false
	}
}

func normalizeExactNumericExponent(value any) (any, error) {
	var text string
	switch value := value.(type) {
	case json.Number:
		text = value.String()
	case string:
		text = value
	default:
		return value, nil
	}
	if !strings.ContainsAny(text, "eE") {
		return value, nil
	}

	parsed, err := decimal.NewFromString(text)
	if err != nil {
		return nil, fmt.Errorf("parse exponent-form number %q: %w", text, err)
	}
	if parsed.IsZero() {
		return json.Number("0"), nil
	}
	// Decimal.String materializes exponent padding, so bound the rendered form
	// before calling it. SQL Server exact numerics are at most 38 digits; this
	// leaves ample room while preventing a tiny input from causing a huge allocation.
	const maxExpandedDecimalLength = 4096
	if decimalExpansionLength(parsed) > maxExpandedDecimalLength {
		return nil, fmt.Errorf("exponent-form number %q is too large to expand", text)
	}
	return json.Number(parsed.String()), nil
}

func decimalExpansionLength(value decimal.Decimal) int64 {
	coefficient := value.Coefficient()
	negative := coefficient.Sign() < 0
	if negative {
		coefficient.Neg(coefficient)
	}
	digits := int64(len(coefficient.String()))
	exponent := int64(value.Exponent())

	length := digits
	if exponent >= 0 {
		length += exponent
	} else if scale := -exponent; digits > scale {
		length++ // decimal point
	} else {
		length = scale + 2 // "0.", leading fractional zeros, and digits
	}
	if negative {
		length++
	}
	return length
}

func sqlServerTemporalType(databaseType string) (sqlType string, dateOnly, timeOnly, preserveOffset bool) {
	switch strings.ToLower(databaseType) {
	case "date":
		return "date", true, false, false
	case "time":
		return "time", false, true, false
	case "datetime", "datetime2", "smalldatetime":
		return strings.ToLower(databaseType), false, false, false
	case "datetimeoffset":
		return "datetimeoffset", false, false, true
	default:
		return "", false, false, false
	}
}

func (d SQLServer) UpsertQuery(namespace string, keys, columns, columnTypes, defaultColumns, identityColumns []string, count int, overrideIdentity bool) (string, error) {
	nonKeys := make([]string, 0, len(columns))
	for _, column := range columns {
		if !slices.Contains(keys, column) && !slices.Contains(identityColumns, column) {
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
			if temporalType, _, _, _ := sqlServerTemporalType(databaseType); temporalType != "" {
				value = "CAST(" + value + " AS " + temporalType + ")"
			} else {
				switch strings.ToLower(databaseType) {
				case "binary", "varbinary", "image":
					value = "CAST(" + value + " AS varbinary(max))"
				}
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
