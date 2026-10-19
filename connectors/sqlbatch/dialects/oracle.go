package dialects

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	go_ora "github.com/sijms/go-ora/v2"
)

type Oracle struct{}

func (Oracle) ConvertDriverValue(column ColumnMetadata, value any) (any, error) {
	return ConvertDriverValue(column, value)
}

var oracleTimestampTypePattern = regexp.MustCompile(`(?i)^timestamp(?:\([0-9]+\))?(?: with (?:local )?time zone)?$`)

func (Oracle) MaxQueryParameters() int {
	return 65535
}

func (Oracle) SupportsIdentityOverride() bool {
	return false
}

func (d Oracle) InspectTable(ctx context.Context, db *sql.DB, namespace string) (TableMetadata, error) {
	schema, table, err := SplitNamespace(namespace)
	if err != nil {
		return TableMetadata{}, err
	}
	rows, err := db.QueryContext(ctx, `
		SELECT c.column_name,
		       c.data_type,
		       CASE WHEN c.nullable = 'Y' THEN 'YES' ELSE 'NO' END,
		       CASE WHEN c.default_length IS NULL OR c.default_length = 0 THEN 'NO' ELSE 'YES' END,
		       CASE WHEN c.virtual_column = 'YES' THEN 'YES' ELSE 'NO' END,
		       CASE WHEN c.identity_column = 'YES' THEN 'YES' ELSE 'NO' END,
		       NVL(identity.generation_type, 'NONE'),
		       NVL(pk.position, 0)
		FROM all_tab_cols c
		LEFT JOIN all_tab_identity_cols identity
		  ON identity.owner = c.owner
		 AND identity.table_name = c.table_name
		 AND identity.column_name = c.column_name
		LEFT JOIN (
			SELECT cc.owner, cc.table_name, cc.column_name, cc.position
			FROM all_constraints con
			JOIN all_cons_columns cc
			  ON cc.owner = con.owner
			 AND cc.constraint_name = con.constraint_name
			WHERE con.constraint_type = 'P'
		) pk
		  ON pk.owner = c.owner
		 AND pk.table_name = c.table_name
		 AND pk.column_name = c.column_name
		WHERE c.owner = COALESCE(NULLIF(:1, ''), SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA'))
		  AND c.table_name = :2
		  AND c.hidden_column = 'NO'
		ORDER BY c.column_id`, schema, table)
	if err != nil {
		return TableMetadata{}, err
	}
	defer rows.Close()
	return ScanTableMetadata(rows)
}

func (Oracle) ConvertJSONValue(column ColumnMetadata, value any) (any, error) {
	databaseType := strings.ToLower(column.DatabaseType)
	if temporalType, preserveOffset := oracleTemporalType(column.DatabaseType); temporalType != "" {
		kind := temporalTimestamp
		if temporalType == "DATE" {
			// Unlike a SQL DATE, Oracle DATE also stores hours, minutes, and seconds.
			// Accept a date-only value as midnight, but preserve a supplied timestamp.
			kind = temporalOracleDate
		}
		return convertTemporalValue(column, value, kind, preserveOffset)
	}
	if value != nil && strings.HasPrefix(databaseType, "interval") {
		return nil, fmt.Errorf("Oracle sink does not yet support supplied values for interval column %q of type %s", column.Name, column.DatabaseType)
	}
	if value != nil && (databaseType == "long" || databaseType == "long raw") {
		return nil, fmt.Errorf("Oracle sink does not support supplied values for column %q of type %s", column.Name, column.DatabaseType)
	}
	if value == nil && databaseType == "raw" {
		// go-ora otherwise describes an untyped nil as NCHAR, which cannot be
		// combined with non-null RAW binds in the UNION ALL row source.
		return []byte(nil), nil
	}
	if number, ok := value.(json.Number); ok && oracleNumericType(column.DatabaseType) != "" {
		return go_ora.NewNumberFromString(number.String())
	}
	return ConvertJSONValue(column, value, "raw", "long raw", "blob")
}

func oracleTemporalType(databaseType string) (string, bool) {
	databaseType = strings.TrimSpace(databaseType)
	lowerType := strings.ToLower(databaseType)
	switch {
	case lowerType == "date":
		return "DATE", false
	case oracleTimestampTypePattern.MatchString(databaseType):
		// DATA_TYPE comes from Oracle metadata. Validate its shape before using it
		// as a cast target, and retain any declared fractional-second precision.
		return strings.ToUpper(databaseType), strings.Contains(lowerType, "with time zone")
	default:
		return "", false
	}
}

func oracleNumericType(databaseType string) string {
	switch strings.ToLower(databaseType) {
	case "number", "decimal", "numeric":
		return "NUMBER"
	case "float":
		return "FLOAT"
	case "binary_float":
		return "BINARY_FLOAT"
	case "binary_double":
		return "BINARY_DOUBLE"
	default:
		return ""
	}
}

func (d Oracle) UpsertQuery(namespace string, keys, columns, columnTypes []string, defaultColumns, identityColumns []string, count int, overrideIdentity bool) (string, error) {
	if overrideIdentity {
		return "", fmt.Errorf("Oracle GENERATED ALWAYS identity columns cannot be written explicitly")
	}
	nonKeys := make([]string, 0, len(columns))
	for _, column := range columns {
		if !slices.Contains(keys, column) && !slices.Contains(identityColumns, column) {
			nonKeys = append(nonKeys, column)
		}
	}
	rows := make([]string, count)
	for row := range count {
		selects := make([]string, len(columns))
		for i, column := range columns {
			value := d.Placeholder(row*len(columns) + i)
			if i < len(columnTypes) {
				if temporalType, _ := oracleTemporalType(columnTypes[i]); temporalType != "" {
					value = "CAST(" + value + " AS " + temporalType + ")"
				} else if numericType := oracleNumericType(columnTypes[i]); numericType != "" {
					// Besides making every UNION ALL branch agree, the cast gives an
					// untyped nil bind the destination's numeric family.
					value = "CAST(" + value + " AS " + numericType + ")"
				} else if strings.EqualFold(columnTypes[i], "blob") {
					// Oracle otherwise infers a NULL BLOB bind as a character value,
					// making batched UNION ALL sources fail with ORA-01790.
					value = "TO_BLOB(" + value + ")"
				}
			}
			selects[i] = value + " AS " + QuoteIdentifier(column)
		}
		rows[row] = "SELECT " + strings.Join(selects, ", ") + " FROM DUAL"
	}
	var query strings.Builder
	query.WriteString("MERGE INTO " + QuoteNamespace(namespace) + " target USING (" + strings.Join(rows, " UNION ALL ") + ") source ON (" + JoinPredicate(keys, "target", "source") + ")")
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
	query.WriteString(" WHEN NOT MATCHED THEN INSERT (" + QuotedIdentifiers(columns) + ") VALUES (" + SourceColumns(columns, "source") + ")")
	return query.String(), nil
}

func (d Oracle) DeleteQuery(namespace string, keys []string, count int) string {
	return DeleteQuery(namespace, keys, count, d.Placeholder)
}

// KeySub implements sqlbatch.SQLDialect.
func (d Oracle) KeySub(cols []string, count int) string {
	numCols := len(cols)
	var sb strings.Builder
	for i := range count {
		sb.WriteString("SELECT ")
		for j, col := range cols {
			sb.WriteString(d.Placeholder(i*numCols + j))
			sb.WriteString(" as \"")
			sb.WriteString(col)
			sb.WriteString("\"")
			if j < numCols-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(" FROM DUAL")
		if i < count-1 {
			sb.WriteString(" UNION ALL ")
		}
	}
	return sb.String()
}

func (d Oracle) ListDataTemplate(cols []string, query string) string {
	return "WITH QUERY AS (" + query + ") SELECT * FROM QUERY%v ORDER BY \"" + strings.Join(cols, "\", \"") + "\" FETCH NEXT %v ROWS ONLY"
}

func (d Oracle) ListDataApplyTemplate(cols []string, template string, limit int, low []any, high []any) (string, []any) {
	var finalQuery string
	var subs []any
	if len(low) > 0 {
		if len(high) > 0 {
			where := fmt.Sprintf(" WHERE %v AND %v", LowQuery(cols, d.Placeholder, 0), HighQuery(cols, d.Placeholder, len(cols)))
			finalQuery = fmt.Sprintf(template, where, limit)
			subs = append(subs, low...)
			subs = append(subs, high...)
		} else {
			finalQuery = fmt.Sprintf(template, " WHERE "+LowQuery(cols, d.Placeholder, 0), limit)
			subs = low
		}
	} else {
		if len(high) > 0 {
			finalQuery = fmt.Sprintf(template, " WHERE "+HighQuery(cols, d.Placeholder, 0), limit)
			subs = high
		} else {
			finalQuery = fmt.Sprintf(template, "", limit)
		}
	}
	return finalQuery, subs
}

func (d Oracle) FetchTemplate(cols []string, query string) string {
	return FetchTemplate(cols, query, "%v")
}

func (d Oracle) Placeholder(i int) string {
	return ":" + strconv.Itoa(i+1)
}
