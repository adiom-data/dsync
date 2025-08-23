package dialects

import (
	"strings"
)

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
