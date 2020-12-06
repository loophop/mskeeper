package policy

import (
	// "bytes"
	"context"
	"database/sql"
	"fmt"
	"gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/misc"
	// syslog "log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

/*
策略：
前置：
1. 操作影响的行数 > 1/3 总行数（count(1))
2. 行数 rows > 1w

*/

// REF
// MySQL Doc: 8.0https://dev.mysql.com/doc/refman/8.0/en/explain-output.html
//       all: https://dev.mysql.com/doc/refman/5.5/en/alter-table-online-operations.html
// 中文解释:   https://www.cnblogs.com/danhuangpai/p/8475458.html
/*

SIMPLE
explain insert into test select * from test where test.value in (select value from test)

SUBQUERY
explain insert into test select * from test where test.id = (select id from test limit 1)

UNION
UNION RESULT
explain
select * from test where test.value_str = '111'
union
select * from test where test.value_str = '222';

DEPENDENT SUBQUERY
explain select * from test t1 where t1.value = (select id from test t2 where t2.value = t1.value limit 1)

DERIVED (5.7+中优化去掉了部分)
explain select * from (select * from test) b;

MATERIALIZED

UNCACHEABLE SUBQUERY
UNCACHEABLE UNION

关于filtered字段
filtered (JSON name: filtered)

The filtered column indicates an estimated percentage of table rows that will be filtered by the table condition.
The maximum value is 100, which means no filtering of rows occurred. Values decreasing from 100 indicate _
increasing amounts of filtering.
rows shows the estimated number of rows examined and rows × filtered shows the number of rows that will be joined _
with the following table. For example, if rows is 1000 and filtered is 50.00 (50%),
the number of rows to be joined with the following table is 1000 × 50% = 500.

5.5和5.7在同样类型不匹配的情况下，表现并不一样。

explain extended select * from test_policy where value = 72
5.7
1	SIMPLE	test_policy	NULL	ALL	NULL	NULL	NULL	NULL	10157	10.00	Using where

5.5
1	SIMPLE	test_policy	ALL	NULL	NULL	NULL	NULL	10443	100.00	Using where

目测是做了优化。例如, 对于类型不匹配（value1 is varchar[6]），5.7查询效率filtered = 10, 是5.5的十倍
select * from test where value1 = 5

*/

var (
	supportExtended bool = true // true表示支持 explain extended语法，8.0以上不支持

	delayVersion bool = false // 表示已经获取了mysql的版本，假设一个包只有一个版本的MySQL（假设很强吗？）

	notSupportContext bool = false // true表示当前mysql版本（5.7.X）在context的事务处理方面存在bug

	notSupportExplainInsert bool = false // true表示当前mysql版本 (5.5.x) 不支持explain insert 语句

	tableNameReg = regexp.MustCompile(`<.*?>`)

	// mysql的 context 超时被取消之后，可能会死锁。这里的时间大一些。
	// 避开 5.7.25版本
	MaxTimeoutOfExplain = 3 * time.Second
)

type PolicyCode int

var (
	ErrExplainRowsFormatErr = fmt.Errorf("rows field is invalid")
)

const (
	ErrPolicyCodeSafe          PolicyCode = 5200
	ErrPolicyCodeExeCost       PolicyCode = 5201
	ErrPolicyCodeRowsAbs       PolicyCode = 5202
	ErrPolicyCodeRowsInvolve   PolicyCode = 5203
	ErrPolicyCodeAllTableScan  PolicyCode = 5204
	ErrPolicyCodeDataTruncate  PolicyCode = 5205
	WarnPolicyCodeDataTruncate PolicyCode = 5206
)

func (pl PolicyCode) String() string {
	switch pl {
	case ErrPolicyCodeSafe:
		return "ErrPolicyCodeSafe"
	case ErrPolicyCodeExeCost:
		return "ErrPolicyCodeExeCost"
	case ErrPolicyCodeRowsAbs:
		return "ErrPolicyCodeRowsAbs"
	case ErrPolicyCodeRowsInvolve:
		return "ErrPolicyCodeRowsInvolve"
	case ErrPolicyCodeAllTableScan:
		return "ErrPolicyCodeAllTableScan"
	case ErrPolicyCodeDataTruncate:
		return "ErrPolicyCodeDataTruncate"
	case WarnPolicyCodeDataTruncate:
		return "WarnPolicyCodeDataTruncate"
	default:
		str := strconv.Itoa(int(pl))
		return str
	}
}

type PolicyError struct {
	Code PolicyCode `json:"code"`
	Msg  string     `json:"msg"`
}

func NewPolicyErrorSafe(rowsAffected int, cost time.Duration) *PolicyError {
	return &PolicyError{Code: ErrPolicyCodeSafe,
		Msg: fmt.Sprintf("safe sql, rows_affected:%v cost:%0.3vms", rowsAffected, float64(cost.Nanoseconds())/float64(1000000))}
}

func NewPolicyError(code PolicyCode, msg string) *PolicyError {
	return &PolicyError{Code: code, Msg: msg}
}

func (err *PolicyError) Error() string {
	return fmt.Sprintf("[policy_code=%v,policy_msg=%v]", err.Code, err.Msg)
}

type PolicyChecker interface {
	Check(db *sql.DB, er []ExplainRecord, query string, args []interface{}) error
}

type ExplainRecord struct {
	ID           sql.NullString
	SelectType   sql.NullString
	Table        sql.NullString
	Partitions   sql.NullString
	Type         sql.NullString
	PossibleKeys sql.NullString
	Key          sql.NullString
	KeyLen       sql.NullString
	Ref          sql.NullString
	Rows         sql.NullString
	Filtered     sql.NullString
	Extra        sql.NullString
}

// 根据 filtered 和 rows 的比例, 获取真正受影响的行数
func (er *ExplainRecord) GetExplainRealRows() (int, error) {

	if !er.Rows.Valid {
		return 0, ErrExplainRowsFormatErr
	} else {
		rowCnt, _ := strconv.Atoi(er.Rows.String)
		var rate float64 = 100.0
		if er.Filtered.Valid {
			rate, _ = strconv.ParseFloat(er.Filtered.String, 64)
		}
		realRowCnt := int(float64(rowCnt) * (rate / float64(100.0)))
		// syslog.Printf("GetExplainRealRows: rate %v, rowCnt %v realRowCnt %v", rate, rowCnt, realRowCnt)
		log.MSKLog().Debugf("GetExplainRealRows: rate %v, rowCnt %v realRowCnt %v", rate, rowCnt, realRowCnt)
		return realRowCnt, nil
	}
}

func NewExplainRecord() *ExplainRecord {
	return &ExplainRecord{}
}

// func MakeExplainRecords(db *sql.DB, query string, args ...interface{}) ([]ExplainRecord, error) {
// 	explainRecords := []ExplainRecord{}

// 	//	originQuery := query
// 	query = "explain " + query

// 	rows, err := db.Query(query, args...)
// 	if err != nil {
// 		return explainRecords, err
// 	}
// 	defer rows.Close()

// 	explainRecords, err = genExplainRecordsFromRows(rows)
// 	if err != nil {
// 		// log.Printf("MakeExplainRecords(%v, %v) GenExplainRecordsFromRows failed %v", query, args, err)
// 		return explainRecords, err
// 	}
// 	//log.Printf("MakeExplainRecords %v on origin query %v", explainRecords, originQuery)
// 	return explainRecords, nil
// }

func safeRollback(memo string, tx *sql.Tx) error {
	err := tx.Rollback()
	if err != sql.ErrTxDone && err != nil {
		log.MSKLog().Errorf("safeRollback(%v) rollback failed %v",
			memo, err)
		return err
	}
	return nil
}

// !!!!!!!!!!!!!!!!!!!
// mysql 5.7.25 实测，偶发的会出现死亡deadlock：
// show 不出来线程,
// BeginTx、QueryContext的时候不能通过ctx取消!!!!!否则会出现Begin了的事务无法结束，锁住整个表
func MakeExplainRecords(db *sql.DB, query string, timeout time.Duration, args []interface{}) ([]ExplainRecord, error) {
	explainRecords := []ExplainRecord{}

	ctx, cancel := context.WithCancel(context.Background())
	// 针对 mysql 5.7.x 版本在context方面的bug，workaround
	if notSupportContext {
		timeout = timeout * 100
	}
	defer time.AfterFunc(timeout, cancel).Stop()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.MSKLog().Errorf("MakeExplainRecords(%v, %v) BeginTx failed %v", query, args, err)
		return explainRecords, err
	}
	defer func() {
		_ = safeRollback(fmt.Sprintf("MakeExplainRecords() query of %v rollback", query), tx)
	}()

	if !delayVersion {
		supportExtended = true
		versionQuery := "SELECT version();"
		row := tx.QueryRowContext(ctx, versionQuery)
		var version string
		if err := row.Scan(&version); err != nil {
			log.MSKLog().Errorf("MakeExplainRecords(%v, %v) QueryRowContext failed %v", query, args, err)
			return explainRecords, err
		}

		vs := strings.Split(version, ".")
		if len(vs) >= 1 && vs[0] == "8" { // mysql 8.0.x+
			supportExtended = false
		}
		if len(vs) >= 2 && vs[0] == "5" && vs[1] == "7" { // mysql 5.7.x+
			notSupportContext = true
		}
		if len(vs) >= 2 && vs[0] == "5" && vs[1] == "5" {
			notSupportExplainInsert = true
		}
		delayVersion = true
	}
	// 已知 mysql 8.0 不支持 explain 的 extended关键字！！！！！
	if supportExtended {
		query = "explain extended " + query
	} else {
		query = "explain " + query
	}

	// 已知 mysql 5.5 不支持 explain insert select 句式，过滤insert
	if notSupportExplainInsert {
		query = misc.FilterInnerSelectFor55Minus(query)
	}

	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		log.MSKLog().Errorf("MakeExplainRecords(%v, %v) QueryContext failed %v", query, args, err)
		return explainRecords, err
	}
	defer rows.Close()

	explainRecords, err = genExplainRecordsFromRows(rows)
	if err != nil {
		log.MSKLog().Errorf("MakeExplainRecords(%v, %v) GenExplainRecordsFromRows failed %v", query, args, err)
		return explainRecords, err
	}

	err = tx.Commit()
	if err != nil {
		log.MSKLog().Errorf("MakeExplainRecords(%v, %v) Commit failed %v", query, args, err)
		return explainRecords, err
	}
	//log.Printf("MakeExplainRecords %v on origin query %v", explainRecords, originQuery)
	return explainRecords, nil
}

func genExplainRecordsFromRows(rows *sql.Rows) ([]ExplainRecord, error) {
	records := make([]ExplainRecord, 0)
	columns, _ := rows.Columns()

	for rows.Next() {
		record := NewExplainRecord()
		// mysql 5.5-, 5.6
		if len(columns) == 11 {
			if err := rows.Scan(
				&record.ID, &record.SelectType, &record.Table, &record.Type, &record.PossibleKeys,
				&record.Key, &record.KeyLen, &record.Ref, &record.Rows, &record.Filtered, &record.Extra); err != nil {
				log.MSKLog().Warnf("GenExplainRecordsFromRows(%v) failed %v", rows, err)
				return records, err
			}
		} else {
			// mysql 5.7,8.0+
			if err := rows.Scan(
				&record.ID, &record.SelectType, &record.Table, &record.Partitions, &record.Type,
				&record.PossibleKeys, &record.Key, &record.KeyLen, &record.Ref, &record.Rows, &record.Filtered, &record.Extra); err != nil {
				log.MSKLog().Warnf("GenExplainRecordsFromRows(%v) failed %v", rows, err)
				return records, err
			}
		}

		records = append(records, *record)
		//log.Printf("GenExplainRecords(%+v)", record)
	}
	return records, rows.Err()
}

func MaxRowsFromExplainRecords(explainRecords []ExplainRecord) int {
	maxRows := 0
	for i := 0; i < len(explainRecords); i++ {
		rowCnt, _ := strconv.Atoi(explainRecords[i].Rows.String)
		if maxRows < rowCnt {
			maxRows = rowCnt
		}
	}
	return maxRows
}

func isTableName(tn string) bool {

	if len(strings.TrimSpace(tn)) <= 0 {
		return false
	}
	//返回匹配到的结果
	result := tableNameReg.FindStringSubmatch(tn)
	// log.Printf("isTableName(%v): result %v", tn, result)

	return len(result) <= 0
}
