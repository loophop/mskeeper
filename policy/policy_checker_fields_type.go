package policy

import (
	"database/sql"
	// "database/sql/driver"
	"fmt"
	"gitlab.papegames.com/fringe/mskeeper/log"
	// syslog "log"
	"strings"
)

const (
	DefaultMaxLinesForTypeALL          = 1000 // 1. 1000行以下且没有使用where的全表扫描是安全的，针对配置表
	DefaultMaxLinesForTypeALLWithWhere = 100  // 2. 100行以下，使用了where是安全的

	ExtraKeyWordsUsingWhere = "USING WHERE"
)

// 操作数的类型检查，通过SELECT_TYPE = ALL的方式

type PolicyCheckerFieldsType struct {
}

func NewPolicyCheckerFieldsType() *PolicyCheckerFieldsType {

	return &PolicyCheckerFieldsType{}
}

func (pcri *PolicyCheckerFieldsType) Check(db *sql.DB, explainRecords []ExplainRecord, query string, args []interface{}) error {
	log.MSKLog().Infof("PolicyCheckerFieldsType:Check(%v, %v, %v) with %v", explainRecords, query, args, pcri)

	for i := 0; i < len(explainRecords); i++ {
		var rowsAffected int
		var err error
		// syslog.Printf("[DEBUG] +++++ explainRecords[i] %v", explainRecords[i])
		if rowsAffected, err = explainRecords[i].GetExplainRealRows(); err != nil {
			// syslog.Printf("[DEBUG] +++++ explainRecords[i].Rows %v query %v", rowsAffected, query)
			continue
		}
		rowCnt := rowsAffected

		// syslog.Printf("[DEBUG] +++++ explainRecords[i] %v", explainRecords[i])
		if strings.ToUpper(explainRecords[i].Type.String) == "ALL" {

			if !explainRecords[i].Extra.Valid {
				// 没有使用where语句,Extra "Using where"，则需要排除类似于配置表(1000行以下)
				if rowCnt > DefaultMaxLinesForTypeALL {
					return NewPolicyError(ErrPolicyCodeAllTableScan, fmt.Sprintf("Possbile all table scaned on table %v extra %v pkey %v key %v with rows %v",
						explainRecords[i].Table, explainRecords[i].Extra, explainRecords[i].PossibleKeys, explainRecords[i].Key, explainRecords[i].Rows))
				} else {
					log.MSKLog().Infof("PolicyCheckerFieldsType:Check rowcnt%v <= DefaultMaxLinesForTypeALL%v for all table scan, skipped",
						rowCnt, DefaultMaxLinesForTypeALL)
				}
			} else {
				// using where, but still has full table scans
				if rowCnt > DefaultMaxLinesForTypeALLWithWhere {
					if strings.Contains(strings.ToUpper(explainRecords[i].Extra.String), ExtraKeyWordsUsingWhere) {
						return NewPolicyError(ErrPolicyCodeAllTableScan, fmt.Sprintf("Possbile all table scaned on table %v extra %v pkey %v key %v with rows %v",
							explainRecords[i].Table, explainRecords[i].Extra, explainRecords[i].PossibleKeys, explainRecords[i].Key, explainRecords[i].Rows))
					}
				} else {
					log.MSKLog().Infof("PolicyCheckerFieldsType:Check rowcnt%v <= DefaultMaxLinesForTypeALLWithWhere%v for all table scan with where, skipped",
						rowCnt, DefaultMaxLinesForTypeALLWithWhere)
				}
			}
		}
	}
	return nil
}
