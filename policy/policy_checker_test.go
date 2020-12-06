package policy

import (
	// "bytes"
	//"crypto/tls"
	"database/sql"
	"log"
	"strings"
	// "database/sql/driver"
	// "encoding/json"
	"bytes"
	"context"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"math"
	"math/rand"
	// "gitlab.papegames.com/fringe/mskeeper/notifier"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/sqlparser"
	// "io"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	// "strings"
	"testing"
	"time"
)

const defaultBufSize = 4096

var (
	user      string
	pass      string
	prot      string
	addr      string
	dbname    string
	dsn       string
	netAddr   string
	available bool
)

var (
	ErrNoTLS = errors.New("TLS requested but server does not support TLS")
)

// See https://github.com/go-sql-driver/mysql/wiki/Testing
func init() {
	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	user = env("MYSQL_TEST_USER", "root")
	pass = env("MYSQL_TEST_PASS", "19825026")
	prot = env("MYSQL_TEST_PROT", "tcp")
	addr = env("MYSQL_TEST_ADDR", "localhost:3306")
	dbname = env("MYSQL_TEST_DBNAME", "mskeepertestpolicy")
	netAddr = fmt.Sprintf("%s(%s)", prot, addr)
	dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s", user, pass, netAddr, dbname)
	c, err := net.Dial(prot, addr)
	if err == nil {
		available = true
		c.Close()
	}

	log.Printf("init: user %v pass %v prot %v dbname %v netAddr %v", user, pass, prot, dbname, netAddr)
}

var errLog = Logger(log.New(os.Stderr, "[mysql] ", log.Ldate|log.Ltime|log.Lshortfile))

var (
	scanTypeFloat32   = reflect.TypeOf(float32(0))
	scanTypeFloat64   = reflect.TypeOf(float64(0))
	scanTypeInt8      = reflect.TypeOf(int8(0))
	scanTypeInt16     = reflect.TypeOf(int16(0))
	scanTypeInt32     = reflect.TypeOf(int32(0))
	scanTypeInt64     = reflect.TypeOf(int64(0))
	scanTypeNullFloat = reflect.TypeOf(sql.NullFloat64{})
	scanTypeNullInt   = reflect.TypeOf(sql.NullInt64{})
	//scanTypeNullTime  = reflect.TypeOf(NullTime{})
	scanTypeUint8    = reflect.TypeOf(uint8(0))
	scanTypeUint16   = reflect.TypeOf(uint16(0))
	scanTypeUint32   = reflect.TypeOf(uint32(0))
	scanTypeUint64   = reflect.TypeOf(uint64(0))
	scanTypeRawBytes = reflect.TypeOf(sql.RawBytes{})
	scanTypeUnknown  = reflect.TypeOf(new(interface{}))
)

// type NullTime sql.NullTime

// Logger is used to log critical error messages.
type Logger interface {
	Print(v ...interface{})
}

// SetLogger is used to set the logger for critical errors.
// The initial logger is os.Stderr.
func SetLogger(logger Logger) error {
	if logger == nil {
		return errors.New("logger is nil")
	}
	errLog = logger
	return nil
}

// This NullTime implementation is not driver-specific
type NullTime struct {
	Time  time.Time
	Valid bool // Valid is true if Time is not NULL
}

type netErrorMock struct {
	temporary bool
	timeout   bool
}

func (e netErrorMock) Temporary() bool {
	return e.temporary
}

func (e netErrorMock) Timeout() bool {
	return e.timeout
}

func (e netErrorMock) Error() string {
	return fmt.Sprintf("mock net error. Temporary: %v, Timeout %v", e.temporary, e.timeout)
}

type DBTest struct {
	*testing.T
	db *sql.DB
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTest) mustQueryRow(query string, args ...interface{}) (row *sql.Row) {
	row = dbt.db.QueryRow(query, args...)
	if row == nil {
		dbt.fail("queryrow", query, nil)
	}
	return row
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
}

func runRawPolicyTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	defer rawDB.Close()

	rawDB.Exec("DROP TABLE IF EXISTS test_policy")

	dbt := &DBTest{t, rawDB}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS test_policy")
		test(dbt)
	}
}

type PolicyCheckWrapper struct {
	checker PolicyChecker
	db      *sql.DB
}

func NewPolicyCheckWraper(checker PolicyChecker, db *sql.DB) *PolicyCheckWrapper {

	pcw := &PolicyCheckWrapper{}
	pcw.db = db
	pcw.checker = checker

	return pcw
}

func (pcw *PolicyCheckWrapper) Check(query string, args ...interface{}) error {

	explainRecords, err := MakeExplainRecords(pcw.db, query, MaxTimeoutOfExplain, args)
	if err == nil {
		return pcw.checker.Check(pcw.db, explainRecords, query, args)
	}
	// log.Printf("PolicyCheckWrapper got explainRecords %v err %v", explainRecords, err)
	return pcw.checker.Check(pcw.db, nil, query, args)
}

func TestPolicyCheckerIsTableName(t *testing.T) {

	isTable1 := isTableName("<derived>")
	if isTable1 == true {
		t.Errorf("isTable failed")
	}

	isTable2 := isTableName("test")
	if isTable2 == false {
		t.Errorf("isTable failed")
	}

	isTable3 := isTableName("")
	if isTable3 == true {
		t.Errorf("isTable failed")
	}

	isTable4 := isTableName("<union1,2>")
	if isTable4 == true {
		t.Errorf("isTable failed")
	}

	isTable5 := isTableName("abcabcabcabcabcabcabf")
	if isTable5 == false {
		t.Errorf("isTable failed")
	}

	isTable6 := isTableName("123123")
	if isTable6 == false {
		t.Errorf("isTable failed")
	}
}

func TestPolicyCodeAppendingTest(t *testing.T) {

	var unknownPC PolicyCode = PolicyCode(1000)

	if fmt.Sprintf("%v", unknownPC) != "1000" {
		t.Errorf("PolicyCode unknownPC falling down %v", unknownPC)
	}

	var exeCostPolicyCode PolicyCode = ErrPolicyCodeExeCost
	if fmt.Sprintf("%v", exeCostPolicyCode) != "ErrPolicyCodeExeCost" {
		t.Errorf("PolicyCode ErrPolicyCodeExeCost falling down %v", exeCostPolicyCode)
	}

}

func TestPolicyCodeMakeExplainRecordsNormal(t *testing.T) {

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	rawDB.Exec("CREATE TABLE test_policy (value int, value1 int)")
	for i := 0; i < 2000; i++ {
		rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	}

	sql := "select * from test_policy"

	explains, err := MakeExplainRecords(rawDB, sql, MaxTimeoutOfExplain, []interface{}{})
	if err != nil {
		t.Errorf("MakeExplainRecords failed %v", err)
	}

	if len(explains) != 1 {
		t.Errorf("MakeExplainRecords explains %v != 1", len(explains))
	}

}

func TestPolicyCodeMakeExplainRecordsBadDB(t *testing.T) {

	logmsk.MSKLog().SetOutput(os.Stdout)

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	rawDB.Exec("CREATE TABLE test_policy (value int, value1 int)")
	for i := 0; i < 2000; i++ {
		rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	}

	sql := "select *,Sleep(1) from test_policy limit 1"

	rawDB.Close()

	explains, err := MakeExplainRecords(rawDB, sql, MaxTimeoutOfExplain, []interface{}{})
	if err == nil {
		t.Errorf("MakeExplainRecords Db should have benn closed")
	}

	if len(explains) >= 1 {
		t.Errorf("MakeExplainRecords explains %v >= 1", len(explains))
	}
	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func MakeBigExplainSQL(originSQL string, cnt int) string {

	resultSQL := ""
	for i := 0; i < cnt; i++ {
		resultSQL += fmt.Sprintf("select *  from %v%v ", originSQL, i)
		if i != (cnt - 1) {
			resultSQL += "union "
		}
	}

	return resultSQL
}

// Go 语言坑爹的 WithCancel
// https://studygolang.com/articles/16320?fr=sidebar
func TestPolicyCodeMakeExplainRecordsShortTimeout(t *testing.T) {

	rand.Seed(time.Now().Unix())

	logmsk.MSKLog().SetOutput(os.Stdout)

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("DROP TABLE test_policy_tm_%v", i))
		// rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	}

	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("CREATE TABLE test_policy_tm_%v (value int, value1 int)", i))
		for i := 0; i < 10; i++ {
			rawDB.Exec(fmt.Sprintf("INSERT INTO test_policy_tm_%v VALUES (?, ?)", i), i, i)
		}
	}

	// defer func() {
	// 	time.Sleep(2 * time.Second)
	// 	for i := 0; i < 100; i++ {
	// 		rawDB.Exec(fmt.Sprintf("DROP TABLE test_policy_tm_%v", i))
	// 		// rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	// 	}
	// }()

	//	query := "select * from test_policy where value in (select value from test_policy where value in (select value from test_policy))"
	// query := "select * from test_policy"
	// make a sql for explain that will took hundreds of ms
	var unioncnt int = 100
	query := MakeBigExplainSQL("test_policy_tm_", unioncnt)
	log.Printf("query = %v", query)

	for i := 0; i < 3500; i++ {
		log.Printf("Test Scenera %v", i)

		// NG case
		// 50-3050 microsecs = 0.05-3.05ms
		explains, err := MakeExplainRecords(rawDB, query, time.Duration(time.Duration(50+rand.Intn(3000))*time.Microsecond), []interface{}{})
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		switch {
		case (err != nil && err.Error() == "invalid connection"):
		case err == context.Canceled:
		case err == sql.ErrTxDone:
		case err == context.DeadlineExceeded:
		case (err != nil && err.Error() == "sql: Rows are closed"): // .... cov 93.2靠这个
		case (err != nil && err.Error() == "dial tcp: operation was canceled"):
		case (err != nil) && strings.Contains(err.Error(), "Did you run multiple statements at once"):
		default:
			// 行数没法校验，可能在rows.Next()过程中，被cancel
			if err == nil {
				if len(explains) != unioncnt+1 {
					t.Fatalf("MakeExplainRecords explains %v != %v", len(explains), unioncnt+1)
				}
			} else {
				t.Errorf("i %v MakeExplainRecords Db should have been canceled, err %v", i, err)
			}
		}

		// OK case
		explains, err = MakeExplainRecords(rawDB, query, MaxTimeoutOfExplain, []interface{}{})
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		if err != nil {
			t.Errorf("MakeExplainRecords Db should not been canceled, err %v", err)
		}

		if len(explains) != unioncnt+1 {
			t.Errorf("MakeExplainRecords explains %v != %v", len(explains), unioncnt+1)
		}
	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

// Go 语言坑爹的 WithCancel
// https://studygolang.com/articles/16320?fr=sidebar
func TestPolicyCodeMakeColumnRecordsShortTimeout(t *testing.T) {

	rand.Seed(time.Now().Unix())

	logmsk.MSKLog().SetOutput(os.Stdout)

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("DROP TABLE test_policy_tm_%v", i))
		// rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	}

	var unioncnt = 20
	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("CREATE TABLE test_policy_tm_%v (value int,value1 int,value2 int, value3 int,value4 int,value5 int,value6 int,value7 int,value8 int,value9 int,value10 int,value11 int,value12 int,value13 int,value14 int,value15 int,value16 int,value17 int,value18 int,value19 int,value20 int)", i))
	}

	// defer func() {
	// 	time.Sleep(2 * time.Second)
	// 	for i := 0; i < 100; i++ {
	// 		rawDB.Exec(fmt.Sprintf("DROP TABLE test_policy_tm_%v", i))
	// 		// rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	// 	}
	// }()

	//	query := "select * from test_policy where value in (select value from test_policy where value in (select value from test_policy))"
	// query := "select * from test_policy"
	// make a sql for explain that will took hundreds of ms

	for i := 0; i < 4000; i++ {

		// NG case
		// 10-3010 microsecs = 0.01-3.01ms
		_, columns, err := MakeColumnRecords(rawDB, fmt.Sprintf("test_policy_tm_%v", rand.Intn(100)),
			time.Duration(time.Duration(10+rand.Intn(3000))*time.Microsecond))
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		switch {
		case (err != nil && err.Error() == "invalid connection"):
		case err == context.Canceled:
		case err == sql.ErrTxDone:
		case err == context.DeadlineExceeded:
		case (err != nil && err.Error() == "sql: Rows are closed"): // .... cov 93.2靠这个
		case (err != nil && err.Error() == "dial tcp: operation was canceled"):
		case (err != nil) && strings.Contains(err.Error(), "Did you run multiple statements at once"):
		default:
			// 行数没法校验，可能在rows.Next()过程中，被cancel
			if err == nil {
				if len(columns) != unioncnt+1 {
					t.Fatalf("MakeColumnRecords columns %v != %v", len(columns), unioncnt+1)
				}
			} else {
				t.Errorf("i %v MakeColumnRecords Db should have been canceled, err %v", i, err)
			}
		}

		// OK case
		_, columns, err = MakeColumnRecords(rawDB, fmt.Sprintf("test_policy_tm_%v", rand.Intn(100)), MaxTimeoutOfExplain)
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		if err != nil {
			t.Errorf("MakeColumnRecords Db should not been canceled, err %v", err)
		}

		if len(columns) != unioncnt+1 {
			t.Errorf("MakeColumnRecords columns %v != %v", len(columns), unioncnt+1)
		}
	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestPolicyCodeMakeColumnRecordsForMySQL57X(t *testing.T) {

	rand.Seed(time.Now().Unix())

	logmsk.MSKLog().SetOutput(os.Stdout)

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("DROP TABLE test_policy_tm_%v", i))
		// rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	}

	var unioncnt = 20
	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("CREATE TABLE test_policy_tm_%v (value int,value1 int,value2 int, value3 int,value4 int,value5 int,value6 int,value7 int,value8 int,value9 int,value10 int,value11 int,value12 int,value13 int,value14 int,value15 int,value16 int,value17 int,value18 int,value19 int,value20 int)", i))
	}
	/*
		Since the environment of UT of MySQL was not 5.7.x

		By ALL Means, let notSupportContext be true, the timeout will be 100 times larger

		so that all timeout operations will not work
	*/
	notSupportContext = true

	for i := 0; i < 2000; i++ {

		// NG case
		// 100-3000*1000 microsecs = 0.1ms-3s
		_, columns, err := MakeColumnRecords(rawDB, fmt.Sprintf("test_policy_tm_%v", rand.Intn(100)),
			time.Duration(time.Duration(100+rand.Intn(3000*1000))*time.Microsecond))
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		if err != nil {
			t.Fatalf("i %v MakeColumnRecords Db should work in spite of timeout setting when notSupportContext was true, err %v", i, err)
		}

		// OK case
		_, columns, err = MakeColumnRecords(rawDB, fmt.Sprintf("test_policy_tm_%v", rand.Intn(100)), MaxTimeoutOfExplain)
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		if err != nil {
			t.Errorf("MakeColumnRecords Db should not been canceled, err %v", err)
		}

		if len(columns) != unioncnt+1 {
			t.Errorf("MakeColumnRecords columns %v != %v", len(columns), unioncnt+1)
		}
	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestPolicyCodeMakeExplainRecordsForMySQL57X(t *testing.T) {

	rand.Seed(time.Now().Unix())

	logmsk.MSKLog().SetOutput(os.Stdout)

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("DROP TABLE test_policy_tm_%v", i))
		// rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	}

	for i := 0; i < 100; i++ {
		rawDB.Exec(fmt.Sprintf("CREATE TABLE test_policy_tm_%v (value int, value1 int)", i))
		for i := 0; i < 10; i++ {
			rawDB.Exec(fmt.Sprintf("INSERT INTO test_policy_tm_%v VALUES (?, ?)", i), i, i)
		}
	}

	/*
		Since the environment of UT of MySQL was not 5.7.x

		By ALL Means, let notSupportContext be true, the timeout will be 100 times larger

		so that all timeout operations will not work
	*/
	var unioncnt int = 100
	query := MakeBigExplainSQL("test_policy_tm_", unioncnt)
	log.Printf("query = %v", query)

	for i := 0; i < 2000; i++ {
		log.Printf("Test Scenera %v", i)

		// NG case
		// 500-2500*1000 microsecs = 0.5ms-2.5s
		explains, err := MakeExplainRecords(rawDB, query, time.Duration(time.Duration(500+rand.Intn(2500*1000))*time.Microsecond), []interface{}{})
		if err != nil {
			t.Fatalf("i %v MakeExplainRecords Db should work in spite of timeout setting when notSupportContext was true, err %v", i, err)
		}

		// OK case
		explains, err = MakeExplainRecords(rawDB, query, MaxTimeoutOfExplain, []interface{}{})
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		if err != nil {
			t.Errorf("MakeExplainRecords Db should not been canceled, err %v", err)
		}

		if len(explains) != unioncnt+1 {
			t.Errorf("MakeExplainRecords explains %v != %v", len(explains), unioncnt+1)
		}
	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestPolicyCodeCalExprValueNormal(t *testing.T) {

	time.Sleep(5 * time.Second) // take a little care of connections

	rand.Seed(time.Now().Unix())

	logmsk.MSKLog().SetOutput(os.Stdout)

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()
	rawDB.SetMaxOpenConns(100)

	rawDB.Exec("DROP TABLE test_policy IF EXISTS")
	rawDB.Exec("CREATE TABLE test_policy (value int, value1 int)")
	for i := 0; i < 10; i++ {
		rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	}

	expr := "select value1 from test_policy where value1 = 5 limit 1"
	sqlval, err := calExprValue(rawDB, 100*time.Millisecond, expr)
	if err != nil {
		t.Fatalf("calExprValue failed for %v with %v", expr, err)
	}
	if sqlval.Type != sqlparser.StrVal {
		t.Fatalf("calExprValue sqlval.Type %v not match", sqlval.Type)
	}
	dest := sqlparser.NewStrVal([]byte("5"))
	if bytes.Compare(sqlval.Val, dest.Val) != 0 {
		t.Fatalf("calExprValue sqlval.val %v not match", string(sqlval.Val))
	}

	expr = "-1|1"
	sqlval, err = calExprValue(rawDB, 100*time.Millisecond, expr)
	if err != nil {
		t.Fatalf("calExprValue failed for %v with %v", expr, err)
	}
	if sqlval.Type != sqlparser.StrVal {
		t.Fatalf("calExprValue sqlval.Type %v not match", sqlval.Type)
	}
	dest = sqlparser.NewStrVal([]byte("18446744073709551615"))
	if bytes.Compare(sqlval.Val, dest.Val) != 0 {
		t.Fatalf("calExprValue sqlval.val %v not match", string(sqlval.Val))
	}

	expr = "-1|?"
	sqlval, err = calExprValue(rawDB, 100*time.Millisecond, expr, 1)
	if err != nil {
		t.Fatalf("calExprValue failed for %v with %v", expr, err)
	}
	if sqlval.Type != sqlparser.StrVal {
		t.Fatalf("calExprValue sqlval.Type %v not match", sqlval.Type)
	}
	dest = sqlparser.NewStrVal([]byte("18446744073709551615"))
	if bytes.Compare(sqlval.Val, dest.Val) != 0 {
		t.Fatalf("calExprValue sqlval.val %v not match", string(sqlval.Val))
	}

	expr = "255&111"
	sqlval, err = calExprValue(rawDB, 100*time.Millisecond, expr)
	if err != nil {
		t.Fatalf("calExprValue failed for %v with %v", expr, err)
	}
	if sqlval.Type != sqlparser.StrVal {
		t.Fatalf("calExprValue sqlval.Type %v not match", sqlval.Type)
	}
	dest = sqlparser.NewStrVal([]byte("111"))
	if bytes.Compare(sqlval.Val, dest.Val) != 0 {
		t.Fatalf("calExprValue sqlval.val %v, %v not match", string(sqlval.Val), string(dest.Val))
	}

	expr = "1+2+3+!4"
	sqlval, err = calExprValue(rawDB, 100*time.Millisecond, expr)
	if err != nil {
		t.Fatalf("calExprValue failed for %v with %v", expr, err)
	}
	if sqlval.Type != sqlparser.StrVal {
		t.Fatalf("calExprValue sqlval.Type %v not match", sqlval.Type)
	}
	dest = sqlparser.NewStrVal([]byte("6"))
	if bytes.Compare(sqlval.Val, dest.Val) != 0 {
		t.Fatalf("calExprValue sqlval.val %v, %v not match", string(sqlval.Val), string(dest.Val))
	}

	expr = "?+2+?+!4"
	sqlval, err = calExprValue(rawDB, 100*time.Millisecond, expr, 1, 3)
	if err != nil {
		t.Fatalf("calExprValue failed for %v with %v", expr, err)
	}

	// !!!!!!!!!!!!!!!!!!!!! IntVal
	if sqlval.Type != sqlparser.IntVal {
		t.Fatalf("calExprValue sqlval.Type %v not match", sqlval.Type)
	}
	dest = sqlparser.NewStrVal([]byte("6"))
	if bytes.Compare(sqlval.Val, dest.Val) != 0 {
		t.Fatalf("calExprValue sqlval.val %v, %v not match", string(sqlval.Val), string(dest.Val))
	}

	expr = "?|-1|333|?"
	sqlval, err = calExprValue(rawDB, 100*time.Millisecond, expr, math.MaxUint32, 255)
	if err != nil {
		t.Fatalf("calExprValue failed for %v with %v", expr, err)
	}

	if sqlval.Type != sqlparser.StrVal {
		t.Fatalf("calExprValue sqlval.Type %v not match", sqlval.Type)
	}
	dest = sqlparser.NewStrVal([]byte("18446744073709551615"))
	if bytes.Compare(sqlval.Val, dest.Val) != 0 {
		t.Fatalf("calExprValue sqlval.val %v, %v not match", string(sqlval.Val), string(dest.Val))
	}

	log.Printf("sqlval = %v", sqlval)

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestPolicyCodeCalExprValueShortTimeout(t *testing.T) {

	rand.Seed(time.Now().Unix())

	logmsk.MSKLog().SetOutput(os.Stdout)

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	// defer func() {
	// 	time.Sleep(2 * time.Second)
	// 	for i := 0; i < 100; i++ {
	// 		rawDB.Exec(fmt.Sprintf("DROP TABLE test_policy_tm_%v", i))
	// 		// rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
	// 	}
	// }()

	//	query := "select * from test_policy where value in (select value from test_policy where value in (select value from test_policy))"
	// query := "select * from test_policy"
	// make a sql for explain that will took hundreds of ms

	for i := 0; i < 2000; i++ {

		// NG case
		// 100-3000 microsecs = 0.1-3ms
		expr := "(1|?|323&!133)+33*111"
		sqlVal, err := calExprValue(rawDB,
			time.Duration(time.Duration(10+rand.Intn(3000*1000))*time.Microsecond),
			expr, 2)

		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		switch {
		case (err != nil && err.Error() == "invalid connection"):
		case err == context.Canceled:
		case err == sql.ErrTxDone:
		case err == context.DeadlineExceeded:
		case (err != nil && err.Error() == "sql: Rows are closed"): // .... cov 93.2靠这个
		case (err != nil && err.Error() == "dial tcp: operation was canceled"):
		case (err != nil) && strings.Contains(err.Error(), "Did you run multiple statements at once"):
		default:
			if err == nil {
				if sqlVal.Type != sqlparser.IntVal {
					t.Fatalf("sqlVal.Type %v not equal to sqlparser.StrVal ", sqlVal.Type)
				}

				if bytes.Compare(sqlVal.Val, []byte("3666")) != 0 {
					t.Fatalf("sqlVal.Val %v not equal to 3666 ", string(sqlVal.Val))
				}

			} else {
				t.Errorf("i %v calExprValue Db should have been canceled, err %v", i, err)
			}
		}

		// OK case
		sqlVal, err = calExprValue(rawDB, MaxTimeoutOfExplain, expr, 2)
		// log.Printf("[err %v ] [context.Canceled %v]", err, context.Canceled)
		if err != nil {
			t.Errorf("calExprValue Db should not been canceled, err %v", err)
		}

		if sqlVal.Type != sqlparser.IntVal {
			t.Fatalf("sqlVal.Type %v not equal to sqlparser.StrVal ", sqlVal.Type)
		}

		if bytes.Compare(sqlVal.Val, []byte("3666")) != 0 {
			t.Fatalf("sqlVal.Val %v not equal to 3666 ", string(sqlVal.Val))
		}

	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

// func TestSafeRollback(t *testing.T) {

// 	rawDB, err := sql.Open("mysql", dsn)
// 	if err != nil {
// 		t.Fatalf("error connecting: %s", err.Error())
// 	}
// 	defer rawDB.Close()

// 	rawDB.Exec("CREATE TABLE test_policy (value int, value1 int)")
// 	for i := 0; i < 3000; i++ {
// 		rawDB.Exec("INSERT INTO test_policy VALUES (?, ?)", i, i)
// 	}

// 	ctx, _ := context.WithCancel(context.Background())

// 	// time.AfterFunc(1*time.Millisecond, cancel)

// 	con, _ := rawDB.Conn(ctx)

// 	tx, _ := con.BeginTx(ctx, nil)

// 	_, err = tx.ExecContext(ctx, "update test_policy set value = value+1")

// 	log.Printf("err of tx.ExecContent %v", err)

// 	rawDB.SetConnMaxLifetime(1 * time.Millisecond)

// 	go func() {
// 		log.Printf("11111")

// 		// expect ErrInvalidConn
// 		err = safeRollback("oooops", tx)
// 		if err == nil {
// 			t.Fatalf("safeRollback failed %v", err)
// 		}
// 		log.Printf("22222")
// 	}()

// 	con.Close()
// 	time.Sleep(3000 * time.Millisecond)

// }
