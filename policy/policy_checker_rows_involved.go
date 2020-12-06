package policy

import (
	"database/sql"
	// "database/sql/driver"
	"fmt"
	"gitlab.papegames.com/fringe/mskeeper/log"
	// syslog "log"
)

const DefaultRowsRate float32 = float32(0.3)
const RowsSafeLine int = 1000

// 操作影响的行数 > 1/3 总行数（count(1)) && 操作影响的行数 > 1000
type PolicyCheckerRowsInvolved struct {
	rate float32
}

func NewPolicyCheckerRowsInvolved() *PolicyCheckerRowsInvolved {

	return &PolicyCheckerRowsInvolved{rate: DefaultRowsRate}
}

func (pcri *PolicyCheckerRowsInvolved) Check(db *sql.DB, explainRecords []ExplainRecord, query string, args []interface{}) error {

	log.MSKLog().Infof("PolicyCheckerRowsInvolved:Check(%v, %v, %v) with %v", explainRecords, query, args, pcri)
	for i := 0; i < len(explainRecords); i++ {
		// syslog.Printf("[DEBUG] ----- explainRecords[i].Table.String %v explainRecords[i].Rows %v query %v, explainRecords[i] %v",
		// explainRecords[i].Table.String, explainRecords[i].Rows, query, explainRecords[i])
		var rowsAffected int
		var err error
		if rowsAffected, err = explainRecords[i].GetExplainRealRows(); err != nil {
			// syslog.Printf("[DEBUG] +++++ explainRecords[i].Rows %v query %v", rowsAffected, query)
			continue
		}
		rowCnt := rowsAffected
		if !isTableName(explainRecords[i].Table.String) {
			// syslog.Printf("[DEBUG] ^^^^^^^ explainRecords[i].Table.String %v skipped", explainRecords[i].Table.String)
			continue
		}
		subTableCountQuery := "select count(1) from " + explainRecords[i].Table.String
		subExplainRecords, err := MakeExplainRecords(db, subTableCountQuery, MaxTimeoutOfExplain, []interface{}{})
		if err != nil {
			// 没有行数的直接跳过，包括了
			// log.Printf("[DEBUG] +++++ continue explainRecords[i].Rows %v query %v", rowsAffected, query)
			continue
		}

		maxRows := MaxRowsFromExplainRecords(subExplainRecords)
		// log.Printf("[DEBUG] +++++ explainRecords[i].Rows %v query %v maxRows %v rowsAffected %v", rowCnt, query, maxRows, rowsAffected)
		if rowCnt > int(float32(maxRows)*pcri.rate) &&
			rowCnt > RowsSafeLine {
			return NewPolicyError(ErrPolicyCodeRowsInvolve, fmt.Sprintf("Too many rows will involve by target sql: rowcnt %v > maxrows %v * pcri.rate %v",
				rowCnt, maxRows, pcri.rate))
		}
	}

	return nil
}
