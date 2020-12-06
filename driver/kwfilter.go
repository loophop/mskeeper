package driver

import (
	// "gitlab.papegames.com/fringe/mskeeper/log"
	// syslog "log"
	"strings"
)

// 下列关键字打头的SQL语句不做分析
var keywords1 []string = []string{
	"EXPLAIN",
	"SHOW",
}

// 下列关键字打头的SQL语句不进队列
var keywords2 []string = []string{
	"EXPLAIN",
	"DROP",
	"ALTER",
	"CREATE",
	"RENAME",
	"TRUNCATE",
	"LOCK",
	"UNLOCK",
	"CALL",
	"USE",
	"SHOW",
	"SET",
}

func findIn(kw string, keywords []string) bool {
	upkw := strings.ToUpper(kw)
	for i := 0; i < len(keywords); i++ {
		if upkw == keywords[i] {
			return true
		}
	}
	return false
}

func parseKeyWordFromSQL(sql string) string {
	sql = strings.TrimSpace(sql)

	kws := strings.Split(sql, " ")
	if len(kws) > 1 { // strings.Split至少返回一个原串，且，keywords中的语句至少 len(kws) >= 2
		return kws[0]
	}
	return sql
}

func checkIfSQLExplainLike(sql string) bool {
	kw := parseKeyWordFromSQL(sql)
	// syslog.Printf("checkIfSQLHardcore sql %v", sql)
	return findIn(kw, keywords1)
}

func checkIfSQLHardcore(sql string) bool {
	kw := parseKeyWordFromSQL(sql)
	// syslog.Printf("checkIfSQLHardcore sql %v", sql)
	return findIn(kw, keywords2)
}
