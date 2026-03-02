package dialects

import (
	"fmt"
	"strconv"
	"strings"

	_ "github.com/microsoft/go-mssqldb"
)

type SQLServer struct{}

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
