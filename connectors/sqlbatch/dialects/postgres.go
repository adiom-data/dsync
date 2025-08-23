package dialects

import (
	"fmt"
	"strconv"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Postgres struct{}

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
