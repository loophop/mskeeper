package policy

import (
	"database/sql"
	// "database/sql/driver"
	"fmt"
	"gitlab.papegames.com/fringe/mskeeper/log"
)

// 操作影响的行数 > 1w
/*
	Mark for 0806
	SELECT 1 FROM redeem_consume WHERE clientid = '1008' and binary redeem = 'nAfewAA8ufddsAaBD'
*/

type PolicyCheckerRowsAbsolute struct {
	maxRowsAcceptable int
}

func NewPolicyCheckerRowsAbsolute(mra int) *PolicyCheckerRowsAbsolute {

	return &PolicyCheckerRowsAbsolute{maxRowsAcceptable: mra}
}

func (pcri *PolicyCheckerRowsAbsolute) Check(db *sql.DB, explainRecords []ExplainRecord, query string, args []interface{}) error {

	log.MSKLog().Infof("PolicyCheckerRowsAbsolute:Check(%v, %v, %v) with %v", explainRecords, query, args, pcri)
	for i := 0; i < len(explainRecords); i++ {
		var rowsAffected int
		var err error

		// log.MSKLog().Warnf("PolicyCheckerRowsAbsolute explainRecords[i] %v = %v", i, explainRecords[i])
		if rowsAffected, err = explainRecords[i].GetExplainRealRows(); err != nil {
			// log.Printf("[DEBUG] +++++ explainRecords[i].Rows %v query %v", rowsAffected, query)
			continue
		}
		rowCnt := rowsAffected
		if rowCnt > pcri.maxRowsAcceptable {
			return NewPolicyError(ErrPolicyCodeRowsAbs, fmt.Sprintf("Too many rows affected absolutely: rowcnt %v > pcri.maxRowsAcceptable %v",
				rowCnt, pcri.maxRowsAcceptable))
		}
	}
	return nil
}
