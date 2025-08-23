package dialects

import (
	"fmt"
	"strconv"
	"strings"

	_ "github.com/sijms/go-ora/v2"
)

type Oracle struct{}

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
