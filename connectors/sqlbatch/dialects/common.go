package dialects

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type ColumnMetadata struct {
	Name               string
	DatabaseType       string
	Nullable           bool
	HasDefault         bool
	Generated          bool
	Identity           bool
	IdentityGeneration string
}

type TableMetadata struct {
	Columns    []ColumnMetadata
	PrimaryKey []string
}

func ConvertDriverValue(_ ColumnMetadata, value any) (any, error) {
	return value, nil
}

func SplitNamespace(namespace string) (schema, table string, err error) {
	parts := strings.Split(namespace, ".")
	switch len(parts) {
	case 1:
		table = parts[0]
	case 2:
		schema, table = parts[0], parts[1]
	default:
		return "", "", fmt.Errorf("expected table or schema.table namespace, got %q", namespace)
	}
	if table == "" || (len(parts) == 2 && schema == "") {
		return "", "", fmt.Errorf("invalid table namespace %q", namespace)
	}
	return schema, table, nil
}

func ScanTableMetadata(rows *sql.Rows) (TableMetadata, error) {
	var metadata TableMetadata
	primaryKeys := map[int]string{}
	for rows.Next() {
		var column ColumnMetadata
		var nullable, hasDefault, generated, identity, identityGeneration string
		var primaryKeyOrdinal int
		if err := rows.Scan(
			&column.Name,
			&column.DatabaseType,
			&nullable,
			&hasDefault,
			&generated,
			&identity,
			&identityGeneration,
			&primaryKeyOrdinal,
		); err != nil {
			return TableMetadata{}, err
		}
		column.Nullable = strings.EqualFold(nullable, "YES")
		column.HasDefault = strings.EqualFold(hasDefault, "YES")
		column.Generated = strings.EqualFold(generated, "YES")
		column.Identity = strings.EqualFold(identity, "YES")
		column.IdentityGeneration = identityGeneration
		metadata.Columns = append(metadata.Columns, column)
		if primaryKeyOrdinal > 0 {
			primaryKeys[primaryKeyOrdinal] = column.Name
		}
	}
	if err := rows.Err(); err != nil {
		return TableMetadata{}, err
	}
	if len(metadata.Columns) == 0 {
		return TableMetadata{}, fmt.Errorf("table was not found or has no visible columns")
	}
	ordinals := make([]int, 0, len(primaryKeys))
	for ordinal := range primaryKeys {
		ordinals = append(ordinals, ordinal)
	}
	sort.Ints(ordinals)
	for _, ordinal := range ordinals {
		metadata.PrimaryKey = append(metadata.PrimaryKey, primaryKeys[ordinal])
	}
	return metadata, nil
}

func ConvertJSONValue(column ColumnMetadata, value any, binaryTypes ...string) (any, error) {
	switch value := value.(type) {
	case json.Number:
		if integer, err := value.Int64(); err == nil {
			return integer, nil
		}
		return value.String(), nil
	case map[string]any, []any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		return string(encoded), nil
	case string:
		for _, binaryType := range binaryTypes {
			if strings.EqualFold(column.DatabaseType, binaryType) {
				decoded, err := base64.StdEncoding.DecodeString(value)
				if err != nil {
					return nil, fmt.Errorf("decode base64 value for binary column %q: %w", column.Name, err)
				}
				return decoded, nil
			}
		}
	}
	return value, nil
}

// ConvertRFC3339Value converts the connector's temporal wire representation
// to time.Time before it reaches a database driver. SQLBatch emits time.Time
// values as RFC3339 JSON through encoding/json, so sinks use the same format.
func ConvertRFC3339Value(column ColumnMetadata, value any, temporalTypes ...string) (any, error) {
	text, ok := value.(string)
	if !ok {
		return value, nil
	}
	for _, temporalType := range temporalTypes {
		if !strings.EqualFold(column.DatabaseType, temporalType) {
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, text)
		if err != nil {
			return nil, fmt.Errorf("parse RFC3339 value for temporal column %q: %w", column.Name, err)
		}
		return parsed, nil
	}
	return value, nil
}

func QuoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func QuoteNamespace(namespace string) string {
	parts := strings.Split(namespace, ".")
	for i, part := range parts {
		parts[i] = QuoteIdentifier(part)
	}
	return strings.Join(parts, ".")
}

func QuotedIdentifiers(identifiers []string) string {
	quoted := make([]string, len(identifiers))
	for i, identifier := range identifiers {
		quoted[i] = QuoteIdentifier(identifier)
	}
	return strings.Join(quoted, ", ")
}

func Placeholders(count int, placeholder func(int) string) string {
	return PlaceholdersFrom(0, count, placeholder)
}

func PlaceholdersFrom(start, count int, placeholder func(int) string) string {
	values := make([]string, count)
	for i := range count {
		values[i] = placeholder(start + i)
	}
	return strings.Join(values, ", ")
}

func ValueRows(columns, count int, placeholder func(int) string) string {
	rows := make([]string, count)
	for i := range count {
		rows[i] = "(" + PlaceholdersFrom(i*columns, columns, placeholder) + ")"
	}
	return strings.Join(rows, ", ")
}

func DeleteQuery(namespace string, keys []string, count int, placeholder func(int) string) string {
	rows := make([]string, count)
	for row := range count {
		predicates := make([]string, len(keys))
		for i, key := range keys {
			predicates[i] = QuoteIdentifier(key) + " = " + placeholder(row*len(keys)+i)
		}
		rows[row] = "(" + strings.Join(predicates, " AND ") + ")"
	}
	return "DELETE FROM " + QuoteNamespace(namespace) + " WHERE " + strings.Join(rows, " OR ")
}

func SourceColumns(columns []string, qualifier string) string {
	values := make([]string, len(columns))
	for i, column := range columns {
		values[i] = qualifier + "." + QuoteIdentifier(column)
	}
	return strings.Join(values, ", ")
}

func Assignments(columns []string, target, source string) string {
	values := make([]string, len(columns))
	for i, column := range columns {
		values[i] = target + "." + QuoteIdentifier(column) + " = " + source + "." + QuoteIdentifier(column)
	}
	return strings.Join(values, ", ")
}

func DefaultAssignments(columns []string, target string) string {
	values := make([]string, len(columns))
	for i, column := range columns {
		prefix := ""
		if target != "" {
			prefix = target + "."
		}
		values[i] = prefix + QuoteIdentifier(column) + " = DEFAULT"
	}
	return strings.Join(values, ", ")
}

func JoinPredicate(keys []string, target, source string) string {
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = target + "." + QuoteIdentifier(key) + " = " + source + "." + QuoteIdentifier(key)
	}
	return strings.Join(values, " AND ")
}

func BuildQuery(cols []string, placeholder func(int) string, placeholderStart int, compare string, lastCompare string) string {
	var buf strings.Builder
	var inner strings.Builder
	if len(cols) > 0 {
		buf.WriteString("(")
	}
	prefix := "("
	for i, col := range cols {
		buf.WriteString(prefix)
		buf.WriteRune('"')
		buf.WriteString(col)
		buf.WriteRune('"')
		p := placeholder(i + placeholderStart)
		if i == len(cols)-1 {
			buf.WriteString(lastCompare)
			buf.WriteString(p)
			buf.WriteString(")")
		} else {
			buf.WriteString(compare)
			buf.WriteString(p)
			buf.WriteString(") OR (")
		}
		inner.WriteRune('"')
		inner.WriteString(col)
		inner.WriteRune('"')
		inner.WriteString("=")
		inner.WriteString(p)
		inner.WriteString(" AND ")

		prefix = inner.String()
	}
	if len(cols) > 0 {
		buf.WriteString(")")
	}
	return buf.String()
}

func LowQuery(cols []string, placeholder func(int) string, placeholderStart int) string {
	return BuildQuery(cols, placeholder, placeholderStart, ">", ">=")
}

func HighQuery(cols []string, placeholder func(int) string, placeholderStart int) string {
	return BuildQuery(cols, placeholder, placeholderStart, "<", "<")
}

func TupleQuery(cols []string, placeholder func(int) string, placeholderStart int, comparer string) string {
	var sb strings.Builder
	for i := range cols {
		p := placeholder(i + placeholderStart)
		sb.WriteString(p)
		if i < len(cols)-1 {
			sb.WriteString(", ")
		}
	}
	return "(\"" + strings.Join(cols, "\", \"") + "\") " + comparer + " (" + sb.String() + ")"
}

func KeySub(numCols int, count int, placeholder func(int) string) string {
	var sb strings.Builder
	for i := range count {
		sb.WriteRune('(')
		for j := range numCols {
			sb.WriteString(placeholder(numCols*i + j))
			if j < numCols-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteRune(')')
		if i < count-1 {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

func FetchTemplate(cols []string, query string, selectKeysTemplate string) string {
	var sb strings.Builder
	sb.WriteString("WITH KEYS AS (")
	sb.WriteString(selectKeysTemplate)
	sb.WriteString("), QUERY AS (")
	sb.WriteString(query)
	sb.WriteString(") SELECT QUERY.* FROM KEYS INNER JOIN QUERY ON ")
	for i := range cols {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		sb.WriteString("KEYS.\"")
		sb.WriteString(cols[i])
		sb.WriteString("\" = QUERY.\"")
		sb.WriteString(cols[i])
		sb.WriteRune('"')
	}
	return sb.String()
}

func SelectKeysTemplate(cols []string) string {
	var sb strings.Builder
	sb.WriteString("SELECT ")
	for i := range cols {
		sb.WriteString("T.\"")
		sb.WriteString(cols[i])
		sb.WriteString("\" as \"")
		sb.WriteString(cols[i])
		sb.WriteRune('"')
		if i < len(cols)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(" FROM (VALUES %v) AS T(")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(")")
	return sb.String()
}
