package policy

import (
	"context"
	"database/sql"
	"fmt"
	"gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/misc"
	"gitlab.papegames.com/fringe/mskeeper/sqlparser"
	"gitlab.papegames.com/fringe/mskeeper/sqlparser/dependency/sqltypes"
	// syslog "log"
	"time"
	// "math/big"
	// "reflect"
	"strconv"
	"strings"
)

/*

操作数长度检测策略，对应varchar，text，int等的长度检测。

背景：
通过 SELECT @@GLOBAL.sql_mode;命令，可以查询MySQL长度硬检测的开关是否打开。
如果包含"STRICT_TRANS_TABLES", 则数据过长是会即时返回错误。
本地的话，5.5没开，5.6、5.7以及8.0的都开了
可通过 set @@global.sql_mode=""; 来关闭严格检查。


调查了阿里云和腾讯云，

1. SDK的阿里云，都没开
2. 腾讯云，恋与的部分开了严格，部分又木有。
3. JAVA的JDBC貌似支持i，https://book.51cto.com/art/201008/219125.htm
4. MySQL各种数据类型的操作示例，https://www.cnblogs.com/qq631243523/p/9791393.html

*/

var (
	ErrFieldDataTruncated     = fmt.Errorf("Data truncated")
	ErrExprToSQLValueFail     = fmt.Errorf("Expr to sqlval failure")
	WarnFieldDataMayTruncated = fmt.Errorf("Data might be truncated in future")
	DataTruncationUplimit     = 0.8
)

const (
	NoTruncated   int = 0
	TruncatedWarn int = 1
	Truncated     int = 2
)

type ColumnMap map[string]*ColumnRecord

// func (cm ColumnMap) String() string {
// 	str := "ColumnMap "
// 	for k, v := range cm {
// 		str += fmt.Sprintf("(k:%v,v:%v) ", k, v)
// 	}
// 	return str
// }

type ColumnRecord struct {
	Field sql.NullString
	Type  sql.NullString
	// Key
	// Default
	// Extra
}

func NewColumnRecord() *ColumnRecord {
	return &ColumnRecord{}
}

func MakeColumnRecords(db *sql.DB, table string, timeout time.Duration) (map[string]*ColumnRecord, []ColumnRecord, error) {

	ctx, cancel := context.WithCancel(context.Background())

	// 针对 mysql 5.7.x 版本在context方面的bug，workaround
	if notSupportContext {
		timeout = timeout * 100
	}

	defer time.AfterFunc(timeout, cancel).Stop()
	// ctx, cancel := context.WithTimeout(context.Background(), timeout)
	// defer cancel()

	//	originQuery := query
	columnsMap := ColumnMap{}
	var columnRecords []ColumnRecord
	query := "show columns from " + table

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return columnsMap, columnRecords, err
	}
	defer func() {
		_ = safeRollback(fmt.Sprintf("MakeColumnRecords() query of %v rollback", query), tx)
	}()

	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return columnsMap, columnRecords, err
	}
	defer rows.Close()

	columnRecords, err = genColumnRecordsFromRows(rows)
	if err != nil {
		return columnsMap, columnRecords, err
	}

	err = tx.Commit()
	if err != nil {
		return columnsMap, columnRecords, err
	}

	for i := 0; i < len(columnRecords); i++ {
		if columnRecords[i].Field.Valid {
			columnsMap[columnRecords[i].Field.String] = &columnRecords[i]
		}
	}
	return columnsMap, columnRecords, nil

}

// 计算一段SQL表达式的值，by select 语句
func calExprValue(db *sql.DB, timeout time.Duration, expr string, args ...interface{}) (*sqlparser.SQLVal, error) {
	//  InterfaceToValue
	//  ExprFromValue

	var sqlval *sqlparser.SQLVal
	ctx, cancel := context.WithCancel(context.Background())

	// 针对 mysql 5.7.x 版本在context方面的bug，workaround
	if notSupportContext {
		timeout = timeout * 100
	}
	defer time.AfterFunc(timeout, cancel).Stop()

	query := "SELECT (" + expr + ")"
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return sqlval, err
	}
	defer func() {
		_ = safeRollback(fmt.Sprintf("calExprValue() query of %v rollback", query), tx)
	}()

	var dest interface{}
	err = tx.QueryRowContext(ctx, query, args...).Scan(&dest)
	if err != nil {
		return sqlval, err
	}
	err = tx.Commit()
	if err != nil {
		return sqlval, err
	}

	typeval, _ := sqltypes.InterfaceToValue(dest)
	parserExpr, _ := sqlparser.ExprFromValue(typeval)
	sv, ok := parserExpr.(*sqlparser.SQLVal)
	if !ok {
		return sqlval, ErrExprToSQLValueFail
	}
	sqlval = sv
	log.MSKLog().Infof("calExprValue(%v, %v, %v, %v) got sqlval %v", db, timeout, expr, args, sqlval)
	return sqlval, nil
}

func genColumnRecordsFromRows(rows *sql.Rows) ([]ColumnRecord, error) {
	records := make([]ColumnRecord, 0)

	for rows.Next() {
		record := NewColumnRecord()
		var noCare1, noCare2, noCare3, noCare4 sql.NullString
		if err := rows.Scan(
			&record.Field,
			&record.Type,
			&noCare1, &noCare2, &noCare3, &noCare4); err != nil {
			log.MSKLog().Warnf("genColumnRecordsFromRows(%v) failed %v", rows, err)
			return records, err
		}
		if !record.Field.Valid || !record.Type.Valid {
			log.MSKLog().Warnf("genColumnRecordsFromRows(%v) invalid of record field %v", rows, record)
			continue
		}
		record.Field.String = strings.ToUpper(record.Field.String)
		records = append(records, *record)
	}
	return records, rows.Err()
}

type PolicyCheckerFieldsLength struct {
	uplimit float64
}

func NewPolicyCheckerFieldsLength(uplimits ...interface{}) *PolicyCheckerFieldsLength {

	uplimit := DataTruncationUplimit
	// Parse uplimit
	for i := 0; i < len(uplimits); i++ {
		u, ok := uplimits[i].(float64)
		if ok && u < 1.000000001 {
			uplimit = u
			break
		}
	}
	return &PolicyCheckerFieldsLength{uplimit: uplimit}
}

//    REF: https://dev.mysql.com/doc/refman/5.6/en/integer-types.html
//    MySQL Integer Type:
//    Type:            TINYINT(), SMALLINT,  MEDIUMINT,      INT,     BIGINT
//    Max:                255        65535    16777215  4294967295    2^64-1
//    Min(signed):       -128        -32768   -8388608  -2147483648   -2^63

//    MySQL type representation:
//    T(length) [unsigned]
//    T is in (tinyint, SMALLINT, MEDIUMINT, INT, BIGINT)
//    length is work when zerofill was set, none bussiness with it's value.
func calNumberOfBitsByFieldTypeInt(fieldType string) int {
	switch fieldType {
	case msFieldTypeTiny:
		return 8
	case msFieldTypeShort:
		return 16
	case msFieldTypeInt24:
		return 24
	case msFieldTypeLong:
		return 32
	case msFieldTypeLongLong:
		return 64
	// no ban no sure
	default:
		return 64
	}
}

// 对于Update语句支持的三类table：AliasedTable, (AliasedTable) 和 (AliasedTable join AliasedTable)
// 只试图返回前两种， 暂时不支持join, eg. update test_policy join test on test_policy.value1 = test.value1 set test_policy.value = '123';
func getAliasedTableFromUpdateStmt(tb sqlparser.TableExpr) *sqlparser.AliasedTableExpr {

	aliaTable, ok := tb.(*sqlparser.AliasedTableExpr)
	if ok {
		return aliaTable
	}

	pTable, ok := tb.(*sqlparser.ParenTableExpr)
	if !ok {
		return nil
	}

	// TODO：暂时不支持 update (table1, table2)..., 只支持 update (table1), (table2)...
	if len(pTable.Exprs) > 1 || len(pTable.Exprs) < 1 {
		return nil
	}
	aliaTable, ok = pTable.Exprs[0].(*sqlparser.AliasedTableExpr)
	if ok {
		return aliaTable
	}

	pTable, ok = pTable.Exprs[0].(*sqlparser.ParenTableExpr)
	if ok {
		return getAliasedTableFromUpdateStmt(pTable)
	}

	return nil
}

//    REF:https://dev.mysql.com/doc/refman/5.6/en/string-types.html
func calNumberOfBytesByFieldTypeValue(fieldType string, parenthesisLength int64) int64 {

	switch fieldType {
	case msFieldTypeBinary:
		return parenthesisLength
	case msFieldTypeVarBinary:
		return parenthesisLength
	case msFieldTypeString:
		return parenthesisLength
	case msFieldTypeVarString:
		return parenthesisLength
	case msFieldTypeTinyBLOB, msFieldTypeTinyTEXT:
		return 255
	case msFieldTypeBLOB, msFieldTypeTEXT:
		return 65535
	case msFieldTypeMediumBLOB, msFieldTypeMediumTEXT:
		return 16777215
	case msFieldTypeLongBLOB, msFieldTypeLongTEXT:
		return 4294967295
	// no ban no sure
	default:
		return 4294967295
	}
}

// Check if value was truncated by the definition of cr
// eg. value = 'abcdefg', cr.Type.String = varchar(5)  ==> data in db: 'abcde' was truncated
func (pcri *PolicyCheckerFieldsLength) checkIfMySQLTruncate(cr *ColumnRecord, sqlV *sqlparser.SQLVal, value []byte) int {
	if sqlV == nil {
		log.MSKLog().Debugf("checkIfMySQLTruncate:Check(%v, %v, %v) nil of sqlV, possiblely FuncExpr",
			cr, sqlV, value)
		return NoTruncated
	}
	msFieldType := strings.Split(cr.Type.String, " ")
	var unsigned bool = false
	if len(msFieldType) > 1 && msFieldType[1] == "unsigned" {
		unsigned = true
	}
	typeString := ""
	if strings.Index(msFieldType[0], "(") > 0 {
		typeString = msFieldType[0][0:strings.Index(msFieldType[0], "(")]
	} else {
		typeString = msFieldType[0]
	}
	typeLength := int64(0)

	if strings.Contains(msFieldType[0], "(") && strings.Index(msFieldType[0], ")") > strings.Index(msFieldType[0], "(") {
		typeLengthStr := msFieldType[0][strings.Index(msFieldType[0], "(")+1 : strings.Index(msFieldType[0], ")")]
		var err error
		typeLength, err = strconv.ParseInt(string(typeLengthStr), 0, 64)
		if err != nil {
			log.MSKLog().Warnf("checkIfMySQLTruncate:Check(%v, %v, %v) strconv.ParseInt of (%v) failed with err %v",
				cr, sqlV, value, typeLengthStr, err)
			return NoTruncated
		}
	}
	log.MSKLog().Debugf("~~~~~~~!!!!!column(%v): checkIfMySQLTruncate sqlV.Type %v typeString %v value %v", cr, sqlV.Type, typeString, string(value))
	var numOfBits int
	switch typeString {
	case msFieldTypeYear:
		// TODO
	case msFieldTypeTiny, msFieldTypeShort, msFieldTypeInt24,
		msFieldTypeLong, msFieldTypeLongLong:

		numOfBits = calNumberOfBitsByFieldTypeInt(typeString)
		var valueInSQL uint64
		if strings.Contains(string(value), "-") {
			valueInSQLInt64, err := strconv.ParseInt(string(value), 0, 64)
			if err != nil {
				err, _ := err.(*strconv.NumError)
				if err.Err == strconv.ErrRange {
					log.MSKLog().Warnf("checkIfMySQLTruncate:Check(%v, %v, %v) strconv.ParseInt of %v failed with err %v",
						cr, sqlV, value, string(value), err)
					return Truncated
				} else {
					log.MSKLog().Warnf("checkIfMySQLTruncate:Check(%v, %v, %v) strconv.ParseInt of %v failed with err %v",
						cr, sqlV, value, string(value), err)
					return NoTruncated
				}
			}
			valueInSQL = uint64(valueInSQLInt64)
		} else {
			valueInSQLUnInt64, err := strconv.ParseUint(string(value), 0, 64)
			if err != nil {
				err, _ := err.(*strconv.NumError)
				if err.Err == strconv.ErrRange {
					log.MSKLog().Warnf("checkIfMySQLTruncate:Check(%v, %v, %v) strconv.ParseUint of %v failed with err %v",
						cr, sqlV, value, string(value), err)
					return Truncated
				} else {
					log.MSKLog().Warnf("checkIfMySQLTruncate:Check(%v, %v, %v) strconv.ParseUint of %v failed with err %v",
						cr, sqlV, value, string(value), err)
					return NoTruncated
				}
			}
			valueInSQL = valueInSQLUnInt64
		}

		log.MSKLog().Debugf("@@@@@@@@@@@@@@@@ valueInSQL %v string(value) %v unsigned %v numOfBits %v, unsigned %v",
			valueInSQL, string(value), unsigned, numOfBits, unsigned)
		if unsigned {
			targetVal, _ := strconv.ParseUint(string(value), 0, numOfBits)
			if uint64(targetVal) != valueInSQL {
				return Truncated
			}
		} else {
			targetVal, _ := strconv.ParseInt(string(value), 0, numOfBits)
			if uint64(targetVal) != valueInSQL {
				return Truncated
			}
		}
	case msFieldTypeFloat:
	case msFieldTypeDouble:
		// TODO
	case msFieldTypeBit:
		// https://www.twle.cn/c/yufei/mysqlfav/mysqlfav-basic-bit.html
		// TODO
	case msFieldTypeDecimal, msFieldTypeEnum, msFieldTypeSet:
		// TODO

	case msFieldTypeTinyBLOB, msFieldTypeMediumBLOB, msFieldTypeLongBLOB, msFieldTypeBLOB,
		msFieldTypeTinyTEXT, msFieldTypeMediumTEXT, msFieldTypeLongTEXT, msFieldTypeTEXT,
		msFieldTypeString, msFieldTypeJSON, msFieldTypeVarString,
		msFieldTypeBinary, msFieldTypeVarBinary, msFieldTypeGeometry:

		switch sqlV.Type {
		case sqlparser.StrVal:
		case sqlparser.IntVal, sqlparser.FloatVal, sqlparser.HexNum:
		case sqlparser.HexVal:
			var realVal []byte
			n, err := fmt.Sscanf(string(value), "%x", &realVal)
			if err != nil || n != 1 {
				log.MSKLog().Warnf("checkIfMySQLTruncate:Check(%v, %v, %v) fmt.Sscanf failed %v",
					cr, sqlV, value, err)
				return NoTruncated
			}
			value = realVal
		case sqlparser.BitVal:
			byteLen := len(value) / 8
			if len(value)%8 > 0 {
				byteLen += 1
			}
			value = make([]byte, byteLen)
		case sqlparser.ValArg:
		default:
		}
		dataTruncUplimit := pcri.uplimit
		targetValLen := calNumberOfBytesByFieldTypeValue(typeString, typeLength)
		log.MSKLog().Debugf("################### typeString %v, typeLength %v, targetValLen %v valueLen %v value %v",
			typeString, typeLength, targetValLen, len(value), string(value))
		if int64(len(value)) > targetValLen {
			return Truncated
		} else if int64(len(value)) > int64(float64(targetValLen)*dataTruncUplimit) {
			return TruncatedWarn
		}

	case msFieldTypeDate, msFieldTypeTimestamp, msFieldTypeDateTime, msFieldTypeTime:
		// TODO
	case msFieldTypeNULL:
		// TODO
	default:
		log.MSKLog().Warnf("checkIfMySQLTruncate:Check(%v, %v, %v) unknown typeString %v",
			cr, sqlV, value, typeString)
		return NoTruncated
	}

	return NoTruncated
}

func (pcri *PolicyCheckerFieldsLength) checkValueLengthBy(columnSlice []string, valueSlice []*sqlparser.SQLVal,
	columnTypeValueMap ColumnMap, args interface{}, argsIdx *int) error {

	// argsIdx := 0
	argCnt := 0
	// assert
	argsSlice, ok := args.([]interface{})
	if !ok {
		return fmt.Errorf("checkValueLengthBy: args.([]interface{}) failed %v for it's Type %T ",
			args, args)
	}
	// syslog.Printf("args %v args %T args0 %v args0 %T", args, args, args0, args0)

	for i := 0; i < len(valueSlice); i++ {
		sqlVal := valueSlice[i]
		if sqlVal != nil && sqlVal.Type == sqlparser.ValArg {
			argCnt++
		}
	}
	// assert sth
	// argCnt 为Update的target的?个数，where及之后条件中的？不是本策略关注的重点
	if len(argsSlice) < argCnt {
		return fmt.Errorf(
			fmt.Sprintf("checkValueLengthBy: len(argsSlice) %v < argCnt %v ",
				len(argsSlice), argCnt))
	}

	// assert sth
	if len(columnSlice) != len(valueSlice) {
		return fmt.Errorf("checkValueLengthBy: len(columnSlice) %v != len(valueSlice) %v ",
			len(columnSlice), len(valueSlice))
	}

	for i := 0; i < len(columnSlice); i++ {
		columnRecord, ok := columnTypeValueMap[columnSlice[i]]
		if !ok {
			continue
		}
		sqlVal := valueSlice[i]
		var value []byte
		if sqlVal == nil {
			// *sqlparser.FuncExpr, such as now()
		} else if sqlVal.Type == sqlparser.ValArg {
			value = []byte(fmt.Sprintf("%v", argsSlice[*argsIdx]))
			*argsIdx++
		} else {
			value = sqlVal.Val
		}

		truncated := pcri.checkIfMySQLTruncate(columnRecord, sqlVal, value)
		if truncated == Truncated {
			return ErrFieldDataTruncated
		} else if truncated == TruncatedWarn {
			return WarnFieldDataMayTruncated
		}
		// syslog.Printf("~~~~checkValueLengthBy argsSlice[argsIdx] %v argsSlice[argsIdx]  %T reflect.Type(argsSlice[argsIdx]) %v",
		// 	argsSlice[argsIdx], argsSlice[argsIdx], reflect.TypeOf(argsSlice[argsIdx]))
	}

	return nil
}

func (pcri *PolicyCheckerFieldsLength) Check(db *sql.DB, explainRecords []ExplainRecord, query string, args []interface{}) error {
	log.MSKLog().Infof("PolicyCheckerFieldsLength:Check(%v, %v, %v) with %v", explainRecords, query, args, pcri)

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		log.MSKLog().Warnf("PolicyCheckerFieldsLength:Check(%v, %v, %v) sqlparser.Parse failed with err %v",
			explainRecords, query, args, err)
		return nil
	}
	switch stmt := stmt.(type) {
	case *sqlparser.Insert:
		insertStruct := stmt
		tableNameString := insertStruct.Table.Name.String()

		columnTypeMap, columnNameSlices, err := MakeColumnRecords(db, tableNameString, MaxTimeoutOfExplain)
		if err != nil {
			log.MSKLog().Warnf("PolicyCheckerFieldsLength:Check(%v, %v, %v) MakeColumnRecords of %v failed",
				explainRecords, query, args, tableNameString)
			break
		}
		log.MSKLog().Debugf("insertStruct %v columnTypeMap %v", insertStruct, len(columnTypeMap))
		columnSlice := []string{}
		for _, column := range insertStruct.Columns {
			columnName := column.CompliantName()
			columnSlice = append(columnSlice, strings.ToUpper(columnName))

			log.MSKLog().Debugf("tableName %v column %T column %v CompliantName %v",
				tableNameString, column, column, column)
		}
		// For case of no explicit fields declared, eg. Insert TableName values ....
		if len(columnSlice) <= 0 {
			for i := 0; i < len(columnNameSlices); i++ {
				fieldName := columnNameSlices[i].Field.String
				columnSlice = append(columnSlice, fieldName)
			}
		}

		insertValues, ok := insertStruct.Rows.(sqlparser.Values)
		if !ok {
			log.MSKLog().Infof("PolicyCheckerFieldsLength:Check(%v, %v, %v) insertStruct.Rows %T failed cast into sqlparser.Values",
				explainRecords, query, args, insertStruct.Rows)
			break
		}
		var argIdx int = 0
		for _, rowValues := range insertValues {
			valueSlice := []*sqlparser.SQLVal{}
			for _, columnValue := range rowValues {

				insertValueColumn, ok := columnValue.(*sqlparser.SQLVal)
				if ok {
					valueSlice = append(valueSlice, insertValueColumn)
				} else {
					valueSlice = append(valueSlice, nil)
				}
			}

			err = pcri.checkValueLengthBy(columnSlice, valueSlice, columnTypeMap, args, &argIdx)
			if err != nil {
				if err == WarnFieldDataMayTruncated {
					return NewPolicyError(WarnPolicyCodeDataTruncate, fmt.Sprintf("Possible data fields near the edge of overflow on table %v with err %v",
						tableNameString, err))
				} else if err != nil {
					return NewPolicyError(ErrPolicyCodeDataTruncate, fmt.Sprintf("Possible data fields overflow on table %v with err %v",
						tableNameString, err))
				}
			}
		}

	case *sqlparser.Update:
		updateStruct := stmt

		for _, table := range updateStruct.TableExprs {

			aliaTable := getAliasedTableFromUpdateStmt(table)
			if aliaTable == nil {
				log.MSKLog().Warnf("PolicyCheckerFieldsLength:Check(%v, %v, %v) table %T failed cast into sqlparser.AliasedTableExpr",
					explainRecords, query, args, table)
				break
			}

			tableName := sqlparser.GetTableName(aliaTable.Expr)
			tableNameString := tableName.String()
			columnTypeMap, _, err := MakeColumnRecords(db, tableNameString, MaxTimeoutOfExplain)
			if err != nil {
				log.MSKLog().Warnf("PolicyCheckerFieldsLength:Check(%v, %v, %v) MakeColumnRecords of %v failed",
					explainRecords, query, args, tableNameString)
				break
			}

			var argFilteredIdx int = 0
			valueSlice := []*sqlparser.SQLVal{}
			columnSlice := []string{}
			var argsPrev int = 0
			var argsFilterd []interface{}
			for _, expr := range updateStruct.Exprs {
				columnName := expr.Name
				var updateExpr *sqlparser.SQLVal
				var ok bool
				switch expr.Expr.(type) {
				case *sqlparser.SQLVal:
					updateExpr, ok = expr.Expr.(*sqlparser.SQLVal)
					if updateExpr.Type == sqlparser.ValArg {
						if argFilteredIdx >= len(args) {
							log.MSKLog().Warnf("PolicyCheckerFieldsLength:Check(%v, %v, %v) mismatch of number of valArg with argFilteredIdx %v >= len(args) %v",
								explainRecords, query, args, argFilteredIdx, len(args))
							ok = false
							break
						}
						argsFilterd = append(argsFilterd, args[argFilteredIdx])
						argFilteredIdx++
					}
				// Complex expression instead of single SQLVal
				default:
					// Firstly, calculate the expression by select statement
					buf := sqlparser.NewTrackedBuffer(nil)
					expr.Expr.Format(buf)

					// 统计之前的？的个数，选择相应的args
					strWithQues := misc.ReplaceColonMark(buf.String())
					ac := misc.CountQuestionMark(strWithQues)
					if argsPrev+ac > len(args) {
						log.MSKLog().Warnf("PolicyCheckerFieldsLength:Check(%v, %v, %v) mismatch of number of valArg in %v",
							explainRecords, query, args, strWithQues)
						ok = false
						break
					}
					updateExpr, err = calExprValue(db, MaxTimeoutOfExplain, strWithQues, args[argsPrev:argsPrev+ac]...)
					if err != nil {
						log.MSKLog().Warnf("PolicyCheckerFieldsLength:Check(%v, %v, %v) expr.Expr %T failed(%v) cast into SQLVal",
							explainRecords, query, args, expr.Expr, err)
						ok = false
						break
					}
					ok = true
					argsPrev += ac
					argFilteredIdx += ac
				}
				if !ok {
					break
				}

				valueSlice = append(valueSlice, updateExpr)
				columnSlice = append(columnSlice, strings.ToUpper(columnName.Name.String()))
				log.MSKLog().Debugf("tableNameString %T tableNameString %v columnName %v updateExpr %T updateExpr.Type %v",
					tableNameString, tableNameString, columnName.Name.String(), updateExpr, updateExpr.Type)
			}
			var argIdx int
			err = pcri.checkValueLengthBy(columnSlice, valueSlice, columnTypeMap, argsFilterd, &argIdx)
			if err == WarnFieldDataMayTruncated {
				return NewPolicyError(WarnPolicyCodeDataTruncate, fmt.Sprintf("Possible data fields near the edge of overflow on table %v with err %v",
					tableNameString, err))
			} else if err != nil {
				return NewPolicyError(ErrPolicyCodeDataTruncate, fmt.Sprintf("Possible data fields overflow on table %v with err %v",
					tableNameString, err))
			}
		}
	}
	return nil
}
