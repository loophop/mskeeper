package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	// "io/ioutil"
	"log"
	"net"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	// logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/misc"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
)

// Ensure that all the driver interfaces are implemented
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
	tDate      = time.Date(2012, 6, 14, 0, 0, 0, 0, time.UTC)
	sDate      = "2012-06-14"
	tDateTime  = time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)
	sDateTime  = "2011-11-20 21:27:37"
	tDate0     = time.Time{}
	sDate0     = "0000-00-00"
	sDateTime0 = "0000-00-00 00:00:00"
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
	dbname = env("MYSQL_TEST_DBNAME", "mskeepertestdriver")
	netAddr = fmt.Sprintf("%s(%s)", prot, addr)
	dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s", user, pass, netAddr, dbname)
	c, err := net.Dial(prot, addr)
	if err == nil {
		available = true
		c.Close()
	}
}

type DBTest struct {
	*testing.T
	db *Addon
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

var notifierUnitTestRaw *notifier.NotifierUnitTest
var notifierUnitTest *notifier.NotifierUnitTest

func (dbt *DBTest) mustQueryRow(query string, args ...interface{}) (row *sql.Row) {

	row = dbt.db.QueryRow(query, args...)
	if row == nil {
		dbt.fail("queryrow", query, nil)
	}
	return row
}

func runDefaultPolicyTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	notifierUnitTest = notifier.NewNotifierUnitTest()
	// safeDB := mysql_keeper.NewMySQLSafeGuard(db)

	msk := NewMSKeeperInstance(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifierUnitTest),
		options.WithLogOutput(os.Stdout),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	db := &Addon{db: msk.RawDB(), msk: msk}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS testdriver")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS testdriver")
		test(dbt)
	}
}

func runDefaultPolicyWithOptionTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	notifierUnitTest = notifier.NewNotifierUnitTest()
	// safeDB := mysql_keeper.NewMySQLSafeGuard(db)

	msk := NewMSKeeperInstance(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifier.NewNotifierMux(notifierUnitTest,
			notifier.NewDefaultNotifier())),
		options.WithCapacity(1024),
		options.WithLogOutput(os.Stdout),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	db := &Addon{db: msk.RawDB(), msk: msk}
	defer db.Close()

	capa := options.FetchCapacity(db.GetOptions())

	if capa != 1024 {
		t.Fatalf("capa %v not set", capa)
	}

	db.Exec("DROP TABLE IF EXISTS testdriver")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS testdriver")
		test(dbt)
	}
}

func runDefaultPolicyWithOneCapacityTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	// rawDB.SetConnMaxLifetime(10 * time.Second)
	rawDB.SetMaxOpenConns(10)
	rawDB.SetMaxIdleConns(10)
	rawDB.SetConnMaxLifetime(20 * time.Second)

	notifierUnitTestRaw = notifier.NewNotifierUnitTest()
	// safeDB := mysql_keeper.NewMySQLSafeGuard(db)
	msk := NewMSKeeperInstance(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifierUnitTestRaw),
		options.WithCapacity(2),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	db := &Addon{db: msk.RawDB(), msk: msk}
	defer db.Close()

	capa := options.FetchCapacity(db.GetOptions())

	if capa != 2 {
		t.Fatalf("capa %v not set", capa)
	}

	db.Exec("DROP TABLE IF EXISTS testdriver")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS testdriver")
		test(dbt)
	}
}

func runTestsWithMultiStatement(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	dsn += "&multiStatements=true"

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer rawDB.Close()

	// safeDB := mysql_keeper.NewMySQLSafeGuard(db)
	msk := NewMSKeeperInstance(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifier.NewNotifierLog("./mskeeper.log")),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	db := &Addon{db: msk.RawDB(), msk: msk}
	defer db.Close()

	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db.Exec("DROP TABLE IF EXISTS testdriver")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS testdriver")
	}
}

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	// safeDB := mysql_keeper.NewMySQLSafeGuard(db)
	msk := NewMSKeeperInstance(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifier.NewNotifierLog("./mskeeper.log")),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	db := &Addon{db: msk.RawDB(), msk: msk}
	defer db.Close()

	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db.Exec("DROP TABLE IF EXISTS testdriver")

	dsn2 := dsn + "&interpolateParams=true"
	var rawDB2 *sql.DB
	rawDB2, err = sql.Open("mysql", dsn2)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	msk2 := NewMSKeeperInstance(
		rawDB2,
		options.WithSwitch(true),
		options.WithNotifier(notifier.NewNotifierLog("./mskeeper.log")),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	db2 := &Addon{db: msk2.RawDB(), msk: msk2}
	defer db.Close()

	_ = db2.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db2.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db2.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db2.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	dsn3 := dsn + "&multiStatements=true"
	var rawDB3 *sql.DB
	rawDB3, err = sql.Open("mysql", dsn3)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	msk3 := NewMSKeeperInstance(
		rawDB3,
		options.WithSwitch(true),
		options.WithNotifier(notifier.NewNotifierLog("./mskeeper.log")),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	db3 := &Addon{db: msk3.RawDB(), msk: msk3}
	defer db.Close()

	_ = db3.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db3.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db3.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db3.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	dbt := &DBTest{t, db}
	dbt2 := &DBTest{t, db2}
	dbt3 := &DBTest{t, db3}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS testdriver")
		if db2 != nil {
			test(dbt2)
			dbt2.db.Exec("DROP TABLE IF EXISTS testdriver")
		}
		if db3 != nil {
			test(dbt3)
			dbt3.db.Exec("DROP TABLE IF EXISTS testdriver")
		}
	}
}

func (dbt *DBTest) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTest) mustExec(query string, args ...interface{}) (res sql.Result) {
	// log.Printf("mustExec args %v Type of args %T", args, args)
	// log.Printf("mustExec args %v %v %v", args...)

	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTest) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
}

func maybeSkip(t *testing.T, err error, skipErrno uint16) {
	mySQLErr, ok := err.(*MySQLError)
	if !ok {
		return
	}

	if mySQLErr.Number == skipErrno {
		t.Skipf("skipping test for error: %v", err)
	}
}

func TestEmptyQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// just a comment, no query
		rows := dbt.mustQuery("--")
		defer rows.Close()
		// will hang before #255
		if rows.Next() {
			dbt.Errorf("next on rows must be false")
		}
	})
}

type Addon struct {
	db  *sql.DB
	msk MSKeeperInter
}

func NewMSKeeperAddon(db *sql.DB, opts ...options.Option) *Addon {

	mska := &Addon{db: db, msk: NewMSKeeperInstance(db, opts...)}
	return mska
}

func (mska *Addon) Begin() (*MSKTx, error) {
	tx, err := mska.db.Begin()
	msTx := &MSKTx{msk: mska.msk}

	msTx.Tx = tx
	return msTx, err
}

func (mska *Addon) BeginTx(ctx context.Context, opts *sql.TxOptions) (*MSKTx, error) {
	tx, err := mska.db.BeginTx(ctx, opts)
	msTx := &MSKTx{msk: mska.msk}

	msTx.Tx = tx
	return msTx, err
}

func (mska *Addon) Conn(ctx context.Context) (*sql.Conn, error) {

	return mska.db.Conn(ctx)
}

func (mska *Addon) Close() error {

	return mska.db.Close()
}

func (mska *Addon) Ping() error {

	return mska.db.Ping()
}

func (mska *Addon) PingContext(ctx context.Context) error {

	return mska.db.PingContext(ctx)
}

func (mska *Addon) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {

	return mska.db.ExecContext(ctx, query, args...)
}

func (mska *Addon) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {

	return mska.db.QueryContext(ctx, query, args...)
}

func (mska *Addon) PrepareContext(ctx context.Context, query string) (*MSKStmt, error) {
	stmt, err := mska.db.PrepareContext(ctx, query)
	msStmt := &MSKStmt{msk: mska.msk, querysql: query}

	msStmt.Stmt = stmt
	return msStmt, err
}

func (mska *Addon) Prepare(query string) (*MSKStmt, error) {
	stmt, err := mska.db.Prepare(query)
	msStmt := &MSKStmt{msk: mska.msk, querysql: query}

	msStmt.Stmt = stmt
	return msStmt, err
}

func (mska *Addon) QueryRow(query string, args ...interface{}) *sql.Row {
	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.QueryRow(query, args...)
}

func (mska *Addon) Query(query string, args ...interface{}) (*sql.Rows, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.Query(query, args...)
}

func (mska *Addon) Exec(query string, args ...interface{}) (sql.Result, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.Exec(query, args...)
}

func (a *Addon) AttachPolicy(policy policy.PolicyChecker) error {
	return a.msk.AttachPolicy(policy)
}

func (a *Addon) RawDB() *sql.DB {
	return a.msk.RawDB()
}

func (a *Addon) ClearPolicies() {
	a.msk.ClearPolicies()
}

func (a *Addon) ResetOptions(opts *options.Options) {
	a.msk.ResetOptions(opts)
}

func (a *Addon) ResyncInfoQueue() {
	a.msk.ResyncInfoQueue()
}

func (a *Addon) ResyncPingTimer() {
	a.msk.ResyncPingTimer()
}

func (a *Addon) SetOption(o options.Option) {
	a.msk.SetOption(o)
}

func (a *Addon) GetOptions() *options.Options {
	return a.msk.GetOptions()
}

func (a *Addon) SetOptions(opts ...options.Option) {
	a.msk.SetOptions(opts...)
}

func (a *Addon) GetErr() []NotifyInfo {
	return a.msk.GetErr()
}

func (a *Addon) Flush() error {
	return a.msk.Flush()
}

func (a *Addon) SyncProcess(t time.Time, query string, args []driver.Value, reterrors *[]error) error {
	return a.msk.SyncProcess(t, query, args, reterrors)
}

func (a *Addon) ClearErr() {
	a.msk.ClearErr()
}

func (a *Addon) HasErr(errCode policy.PolicyCode) bool {
	return a.msk.HasErr(errCode)
}

var jsonType = reflect.TypeOf(json.RawMessage{})

type converter struct{}

func (c converter) ConvertValues(args []interface{}) ([]driver.Value, error) {

	nargs := []driver.Value{}
	var err error
	var narg driver.Value
	for i := 0; i < len(args); i++ {
		narg, err = c.ConvertValue(args[i])
		if err != nil {
			break
		}
		nargs = append(nargs, narg)
	}
	// syslog.Printf("ConvertValues args %v nargs %v", args, nargs)
	return nargs, err
}

// ConvertValue mirrors the reference/default converter in database/sql/driver
// with _one_ exception.  We support uint64 with their high bit and the default
// implementation does not.  This function should be kept in sync with
// database/sql/driver defaultConverter.ConvertValue() except for that
// deliberate difference.
func (c converter) ConvertValue(v interface{}) (driver.Value, error) {
	if driver.IsValue(v) {
		return v, nil
	}

	if vr, ok := v.(driver.Valuer); ok {
		sv, err := callValuerValue(vr)
		if err != nil {
			return nil, err
		}
		if !driver.IsValue(sv) {
			return nil, fmt.Errorf("non-Value type %T returned from Value", sv)
		}
		return sv, nil
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr:
		// indirect pointers
		if rv.IsNil() {
			return nil, nil
		} else {
			return c.ConvertValue(rv.Elem().Interface())
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return rv.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return rv.Float(), nil
	case reflect.Bool:
		return rv.Bool(), nil
	case reflect.Slice:
		switch t := rv.Type(); {
		case t == jsonType:
			return v, nil
		case t.Elem().Kind() == reflect.Uint8:
			return rv.Bytes(), nil
		default:
			return nil, fmt.Errorf("unsupported type %T, a slice of %s", v, t.Elem().Kind())
		}
	case reflect.String:
		return rv.String(), nil
	}
	return nil, fmt.Errorf("unsupported type %T, a %s", v, rv.Kind())
}

var valuerReflectType = reflect.TypeOf((*driver.Valuer)(nil)).Elem()

// callValuerValue returns vr.Value(), with one exception:
// If vr.Value is an auto-generated method on a pointer type and the
// pointer is nil, it would panic at runtime in the panicwrap
// method. Treat it like nil instead.
//
// This is so people can implement driver.Value on value types and
// still use nil pointers to those types to mean nil/NULL, just like
// string/*string.
//
// This is an exact copy of the same-named unexported function from the
// database/sql package.
func callValuerValue(vr driver.Valuer) (v driver.Value, err error) {
	if rv := reflect.ValueOf(vr); rv.Kind() == reflect.Ptr &&
		rv.IsNil() &&
		rv.Type().Elem().Implements(valuerReflectType) {
		return nil, nil
	}
	return vr.Value()
}

type MSKStmt struct {
	*sql.Stmt
	querysql string
	msk      MSKeeperInter
}

func (msks *MSKStmt) Close() error {

	return msks.Stmt.Close()
}

func (msks *MSKStmt) Exec(args ...interface{}) (sql.Result, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer msks.msk.AfterProcess(time.Now(), msks.querysql, nargs)

	return msks.Stmt.Exec(args...)
}

func (msks *MSKStmt) QueryRow(args ...interface{}) *sql.Row {
	nargs, _ := converter{}.ConvertValues(args)
	defer msks.msk.AfterProcess(time.Now(), msks.querysql, nargs)

	return msks.Stmt.QueryRow(args...)
}

func (msks *MSKStmt) Query(args ...interface{}) (*sql.Rows, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer msks.msk.AfterProcess(time.Now(), msks.querysql, nargs)

	return msks.Stmt.Query(args...)
}

type MSKTx struct {
	*sql.Tx
	msk MSKeeperInter
}

func (tx *MSKTx) Commit() (err error) {

	return tx.Tx.Commit()
}

func (tx *MSKTx) Rollback() (err error) {

	return tx.Tx.Rollback()
}

func (tx *MSKTx) Exec(query string, args ...interface{}) (sql.Result, error) {

	nargs, _ := converter{}.ConvertValues(args)
	defer tx.msk.AfterProcess(time.Now(), query, nargs)
	result, err := tx.Tx.Exec(query, args...)
	return result, err
}

// MySQLError is an error type which represents a single MySQL error
type MySQLError struct {
	Number  uint16
	Message string
}

func (me *MySQLError) Error() string {
	return fmt.Sprintf("Error %d: %s", me.Number, me.Message)
}

func TestGetNotifyLevelByPolicyCode(t *testing.T) {

	pe := policy.NewPolicyError(policy.ErrPolicyCodeSafe, fmt.Sprintf("%v", policy.ErrPolicyCodeSafe))
	lvl := getNotifyLevelByPolicyCode(pe)

	if lvl != notifier.InfoLevel {
		t.Fatalf("unexpteced level %v", lvl)
	}

	pe = policy.NewPolicyError(policy.ErrPolicyCodeExeCost, fmt.Sprintf("%v", policy.ErrPolicyCodeExeCost))
	lvl = getNotifyLevelByPolicyCode(pe)

	if lvl != notifier.ErrorLevel {
		t.Fatalf("unexpteced level %v", lvl)
	}

	pe = policy.NewPolicyError(policy.ErrPolicyCodeDataTruncate, fmt.Sprintf("%v", policy.ErrPolicyCodeDataTruncate))
	lvl = getNotifyLevelByPolicyCode(pe)

	if lvl != notifier.ErrorLevel {
		t.Fatalf("unexpteced level %v", lvl)
	}

	pe = policy.NewPolicyError(policy.ErrPolicyCodeRowsInvolve, fmt.Sprintf("%v", policy.ErrPolicyCodeRowsInvolve))
	lvl = getNotifyLevelByPolicyCode(pe)

	if lvl != notifier.ErrorLevel {
		t.Fatalf("unexpteced level %v", lvl)
	}

	pe = policy.NewPolicyError(policy.ErrPolicyCodeRowsAbs, fmt.Sprintf("%v", policy.ErrPolicyCodeRowsAbs))
	lvl = getNotifyLevelByPolicyCode(pe)

	if lvl != notifier.ErrorLevel {
		t.Fatalf("unexpteced level %v", lvl)
	}

	pe = policy.NewPolicyError(policy.WarnPolicyCodeDataTruncate, fmt.Sprintf("%v", policy.WarnPolicyCodeDataTruncate))
	lvl = getNotifyLevelByPolicyCode(pe)

	if lvl != notifier.WarnLevel {
		t.Fatalf("unexpteced level %v", lvl)
	}

	lvl = getNotifyLevelByPolicyCode(fmt.Errorf("any other type of errors"))

	if lvl != notifier.WarnLevel {
		t.Fatalf("unexpteced level %v", lvl)
	}
}

func TestResyncInfoQueue(t *testing.T) {

	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		dbt.db.SetOption(options.WithCapacity(2))

		notifierUnitTest.ClearErr()
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				log.Printf("11111111111111111111111111111111111111111111111111111111 %v", idx)
				// dbt.db.Exec("select 1 from testdriver where value = '1'")
				dbt.mustExec("insert into testdriver values(?,?) ", 255+idx, 255+idx)
				wg.Done()

			}(i)
		}
		wg.Wait()

		dbt.db.Flush()
		time.Sleep(1 * time.Second)
		// Queue capacity has not been resynced
		if notifierUnitTest.GetNoSafeErrsCnt() != 10 {
			dbt.Fatalf("should be 10 errors but %v", notifierUnitTest.GetNoSafeErrsCnt())
		}

		// 同步主线程后，应该有部分task被丢弃
		notifierUnitTest.ClearErr()
		dbt.db.ResyncInfoQueue()
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				log.Printf("11111111111111111111111111111111111111111111111111111111 %v", idx)
				// dbt.db.Exec("select 1 from testdriver where value = '1'")
				dbt.mustExec("insert into testdriver values(?,?) ", 265+idx, 265+idx)
				wg.Done()

			}(i)
		}
		wg.Wait()

		dbt.db.Flush()
		time.Sleep(1 * time.Second)
		// Queue capacity has not been resynced
		if notifierUnitTest.GetNoSafeErrsCnt() >= 10 {
			dbt.Fatalf("should be less than 10 errors but %v", notifierUnitTest.GetNoSafeErrsCnt())
		}
	})
}

func TestGetSetOptions(t *testing.T) {
	/*
		options.WithSwitch(true),
		options.WithNotifier(notifierUnitTest),
		options.WithLogOutput(os.Stdout),
	*/
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		oldopts := dbt.db.GetOptions().Clone()

		dbt.db.SetOptions(
			options.WithMaxSilentPeriod(1*time.Second),    // 1s之后提交的sql依然会检查，1s之内的忽略
			options.WithMaxExecTime(500*time.Microsecond), // 最大执行时间为0.5ms
			options.WithSQLWhiteLists( // 白名单
				"insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);"),
		)

		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver where value != 1);")
		dbt.db.Flush()
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("max exec time should be covered")
		}

		notifierUnitTest.ClearErr()

		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver where value != 1);")
		dbt.db.Flush()
		dbt.db.Flush()
		// max slient perod 范围内 不报错
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("max exec time should not be covered")
		}

		notifierUnitTest.ClearErr()
		time.Sleep(2 * time.Second)

		// 过了 max slient period 继续报错
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver where value != 1);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("max exec time should be covered")
		}

		// 白名单
		notifierUnitTest.ClearErr()
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		if !notifierUnitTest.WithNoErrs() {
			dbt.Fatalf("should be no errors")
		}

		/////// 恢复 设置
		notifierUnitTest.ClearErr()
		dbt.db.ResetOptions(oldopts)
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver where value != 2);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely not covered")
		}
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("time exec time should not covered, %v", oldopts.MaxExecTime)
		}

		// max slient time检测
		notifierUnitTest.ClearErr()
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver where value != 2);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.WithNoErrs() {
			dbt.Fatalf("should be no errors")
		}

	})
}

func TestClearPolicies(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely not covered")
		}

		dbt.mustExec("update testdriver set value = ?", 256)
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("data truncation not covered")
		}

		// Remove all policies
		dbt.db.ClearPolicies()

		// Try again
		notifierUnitTest.ClearErr()

		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver where value=1);")
		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely should not be covered")
		}

		dbt.mustExec("update testdriver set value = ?", 999)
		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("data truncation should not be covered")
		}

	})
}

func TestFilterExplainLikeSQL(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		time.Sleep(1 * time.Second)
		dbt.db.Flush()
		dbt.db.Flush()
		oldCnt := notifierUnitTest.ErrsCount()

		// 过滤 explain
		rows := dbt.mustQuery("explain select * from testdriver")
		defer rows.Close()

		columns, _ := rows.Columns()
		if len(columns) != 12 && len(columns) != 10 {
			dbt.Fatalf("bad columns count %v for explain table", len(columns))
		}

		// 过滤 show
		rows = dbt.mustQuery("show columns from testdriver")
		columns, _ = rows.Columns()
		if len(columns) != 6 {
			dbt.Fatalf("bad columns count %v for show columns", columns)
		}

		newCnt := notifierUnitTest.ErrsCount()

		log.Printf("oldCnt %v, newCnt %v", oldCnt, newCnt)
		if oldCnt != newCnt {
			dbt.Fatalf("sql of explain or show not filtered (oldCnt %v != newCnt %v)", oldCnt, newCnt)
		}
	})
}

func configForTests(t *testing.T) *mysql.Config {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mycnf := mysql.NewConfig()
	mycnf.User = user
	mycnf.Passwd = pass
	mycnf.Addr = addr
	mycnf.Net = prot
	mycnf.DBName = dbname
	return mycnf
}

func TestNewMSKWithNewConnector(t *testing.T) {
	mycnf := configForTests(t)
	conn, err := mysql.NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}
	msk := NewMSK(conn)
	msk.ClearStatus()

	notifierUnitTest = notifier.NewNotifierUnitTest()

	msk.SetOptions(
		options.WithSwitch(true),
		options.WithNotifier(notifierUnitTest),
		options.WithLogOutput(os.Stdout),
	)

	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db := &Addon{db: msk.RawDB(), msk: msk}
	defer db.Close()

	notifierUnitTest.ClearErr()

	dbt := &DBTest{t, db}
	dbt.mustExec("DROP TABLE IF EXISTS testdriver")
	dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
	for i := 0; i < 101; i++ {
		dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
	}

	dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
	dbt.db.Flush()
	dbt.db.Flush()
	if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
		dbt.Fatalf("rows absolutely not covered")
	}

	dbt.mustExec("update testdriver set value = ?", 256)
	dbt.db.Flush()
	if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
		dbt.Fatalf("data truncation not covered")
	}
}

func TestSQLSigLRUCache(t *testing.T) {
	// 测试Cache开启时是否正常命中or失效
	{
		rawDB, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("error connecting: %s", err.Error())
		}
		notifierUnitTest = notifier.NewNotifierUnitTest()

		cacheSize := 30
		overflow := 30

		msk := NewMSKeeperInstance(
			rawDB,
			options.WithSwitch(true),
			options.WithNotifier(notifierUnitTest),
			options.WithLogOutput(os.Stdout),
			options.WithSQLCacheSize(cacheSize),
			options.WithMaxSilentPeriod(1*time.Second),
		)
		db := &Addon{db: msk.RawDB(), msk: msk}
		defer db.Close()

		db.Exec("DROP TABLE IF EXISTS testdriver")
		db.Exec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		isql := "INSERT INTO testdriver VALUES (?, ?)"
		for i := 0; i < cacheSize+overflow; i++ {
			db.Exec(isql, i, "")
		}

		nargs, _ := converter{}.ConvertValues([]interface{}{cacheSize + overflow - 1, ""})
		sig := misc.MD5String(isql, nargs)
		_, ok := msk.sigmap.Peek(sig)
		if !ok {
			t.Errorf("sql sig lru cache miss!")
		}

		nargs2, _ := converter{}.ConvertValues([]interface{}{0, ""})
		sig2 := misc.MD5String(isql, nargs2)
		_, ok2 := msk.sigmap.Peek(sig2)
		if ok2 {
			t.Errorf("sql sig lru cache holds oldest sql!")
		}

		notifierUnitTest.ClearErr()

		for i := 0; i < 10; i++ {
			db.Exec(isql, 0, 1)
		}
		if len(notifierUnitTest.GetErrs()) == 10 {
			for i := 0; i < len(notifierUnitTest.GetSQLs()); i++ {
				t.Errorf("Cache for notifier failed ! %v", notifierUnitTest.GetSQLs()[i])
			}
		}

		notifierUnitTest.ClearErr()

		for i := 0; i < 2; i++ {
			db.Exec(isql, 1, 1)
			time.Sleep(2 * time.Second)
		}
		if len(notifierUnitTest.GetErrs()) <= 1 {
			t.Errorf("Cache for notifier failed !")
		}

	}

	// 测试关闭Cache的情况
	{
		rawDB, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("error connecting: %s", err.Error())
		}
		notifierUnitTest = notifier.NewNotifierUnitTest()

		cacheSize := 0

		msk := NewMSKeeperInstance(
			rawDB,
			options.WithSwitch(true),
			options.WithNotifier(notifierUnitTest),
			options.WithLogOutput(os.Stdout),
			options.WithSQLCacheSize(cacheSize),
		)
		db := &Addon{db: msk.RawDB(), msk: msk}
		defer db.Close()

		db.Exec("DROP TABLE IF EXISTS testdriver")
		db.Exec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		isql := "INSERT INTO testdriver VALUES (?, ?)"
		for i := 0; i < 10; i++ {
			db.Exec(isql, i, "")
		}

		if options.FetchSQLCacheSize(msk.opts) != cacheSize || (msk.sigmap != nil && msk.sigmap.Len() > 0) {
			t.Errorf("sql sig lru cache closed failed!")
		}
	}
}

func TestSyncProcessOK(t *testing.T) {

	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		// dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		// func (a *Addon) SyncProcess(t time.Time, query string, args []sqldriver.Value) error
		notifierUnitTest.ClearErr()
		var errs []error = []error{}
		dbt.db.SyncProcess(time.Now().Add(-1*time.Second), "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);", []driver.Value{}, &errs)

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely not covered")
		}
		pe, _ := errs[0].(*policy.PolicyError)
		if pe.Code != policy.ErrPolicyCodeRowsAbs {
			dbt.Fatalf("rows absolutely not covered")
		}

		log.Printf("errs %v", notifierUnitTest.GetErrs())
		if len(errs) != 1 {
			dbt.Fatalf("length of notifies failed %v", len(errs))
		}
	})
}

func TestSyncProcessIgnoreFailure(t *testing.T) {

	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		// dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		// func (a *Addon) SyncProcess(t time.Time, query string, args []sqldriver.Value) error
		notifierUnitTest.ClearErr()
		var errs []error = []error{}
		err := dbt.db.SyncProcess(time.Now().Add(-1*time.Second), "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);", []driver.Value{}, &errs)
		if err != nil {
			t.Fatalf("should not be ignored")
		}
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely not covered")
		}
		pe, _ := errs[0].(*policy.PolicyError)
		if pe.Code != policy.ErrPolicyCodeRowsAbs {
			dbt.Fatalf("rows absolutely not covered")
		}

		log.Printf("errs %v", notifierUnitTest.GetErrs())
		if len(errs) != 1 {
			dbt.Fatalf("length of notifies failed %v", len(errs))
		}

		dbt.db.SetOption(options.WithSQLWhiteLists("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);"))
		err = dbt.db.SyncProcess(time.Now().Add(-1*time.Second), "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);", []driver.Value{}, &errs)
		if err == nil {
			t.Fatalf("should be skipped since whitelist")
		}
		//
	})
}

func TestBackboneKeepAlivePingNoIdleMax10(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 1; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.RawDB().SetMaxOpenConns(10)
		dbt.db.RawDB().SetMaxIdleConns(0)
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}
		row := dbt.mustQueryRow("select connection_id();")
		var connectionIDOld int
		err := row.Scan(&connectionIDOld)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}

		time.Sleep(3 * time.Second)

		log.Printf("connectionIDOld %v", connectionIDOld)
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal("should create a new connection")
		}

		// should create a new connection
		row = dbt.mustQueryRow("select connection_id();")
		var connectionIDNew int
		err = row.Scan(&connectionIDNew)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}
		log.Printf("connectionIDNew %v", connectionIDNew)

		if connectionIDNew == connectionIDOld {
			dbt.Fatalf("should have create new connection when con was passively closed by mysql")
		}
	})

}

func TestBackboneKeepAlivePingNoIdleMax1(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 1; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.RawDB().SetMaxOpenConns(1)
		dbt.db.RawDB().SetMaxIdleConns(0)
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}
		row := dbt.mustQueryRow("select connection_id();")
		var connectionIDOld int
		err := row.Scan(&connectionIDOld)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}

		time.Sleep(3 * time.Second)

		log.Printf("connectionIDOld %v", connectionIDOld)
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal("should create a new connection")
		}

		// should create a new connection
		row = dbt.mustQueryRow("select connection_id();")
		var connectionIDNew int
		err = row.Scan(&connectionIDNew)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}
		log.Printf("connectionIDNew %v", connectionIDNew)

		if connectionIDNew == connectionIDOld {
			dbt.Fatalf("should have create new connection when con was passively closed by mysql")
		}
	})

}

// 2 max and 1 idle
func TestBackboneKeepAlivePingByDefault(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 1; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}

		dbt.db.RawDB().SetMaxOpenConns(2)
		dbt.db.RawDB().SetMaxIdleConns(1)

		row := dbt.mustQueryRow("select connection_id();")
		var connectionIDOld int
		err := row.Scan(&connectionIDOld)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}

		time.Sleep(3 * time.Second)

		log.Printf("connectionIDOld %v", connectionIDOld)
		if err := dbt.db.Ping(); err == nil {
			dbt.Fatal("Ping() should failed since it was cloased by mysql wait_timeout")
		}
		// should create a new connection
		row = dbt.mustQueryRow("select connection_id();")
		var connectionIDNew int
		err = row.Scan(&connectionIDNew)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}
		log.Printf("connectionIDNew %v", connectionIDNew)

		if connectionIDNew == connectionIDOld {
			dbt.Fatalf("should have create new connection when con was passively closed by mysql")
		}
	})
}

func TestBackboneKeepAlivePingWithIdleMax1(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 1; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.RawDB().SetMaxOpenConns(1)
		dbt.db.RawDB().SetMaxIdleConns(1)
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}
		row := dbt.mustQueryRow("select connection_id();")
		var connectionIDOld int
		err := row.Scan(&connectionIDOld)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}

		time.Sleep(3 * time.Second)

		log.Printf("connectionIDOld %v", connectionIDOld)
		if err := dbt.db.Ping(); err == nil {
			dbt.Fatal("should failed since this connection was closed by mysql")
		}

		// should create a new connection
		row = dbt.mustQueryRow("select connection_id();")
		var connectionIDNew int
		err = row.Scan(&connectionIDNew)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}
		log.Printf("connectionIDNew %v", connectionIDNew)

		if connectionIDNew == connectionIDOld {
			dbt.Fatalf("should have create new connection when con was passively closed by mysql")
		}
	})

}

func TestBackboneKeepAlivePingWithIdleMax1AndPing(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		dbt.db.RawDB().SetMaxOpenConns(1)
		dbt.db.RawDB().SetMaxIdleConns(1)

		dbt.db.SetOption(options.WithKeepAlivePeriod(1 * time.Second))
		dbt.db.ResyncPingTimer()

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 1; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}
		row := dbt.mustQueryRow("select connection_id();")
		var connectionIDOld int
		err := row.Scan(&connectionIDOld)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}

		time.Sleep(3 * time.Second)

		log.Printf("connectionIDOld %v", connectionIDOld)
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal("the connection was kept alived by Ping()")
		}

		// should create a new connection
		row = dbt.mustQueryRow("select connection_id();")
		var connectionIDNew int
		err = row.Scan(&connectionIDNew)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}
		log.Printf("connectionIDNew %v", connectionIDNew)

		if connectionIDNew != connectionIDOld {
			dbt.Fatalf("the connection should be reused")
		}
	})
}

func TestBackboneKeepAlivePingWithIdleMax1AndPingNG(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		dbt.db.RawDB().SetMaxOpenConns(1)
		dbt.db.RawDB().SetMaxIdleConns(1)

		dbt.db.SetOption(options.WithKeepAlivePeriod(5 * time.Second))
		dbt.db.ResyncPingTimer()

		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
		for i := 0; i < 1; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.mustExec("SET @@SESSION.wait_timeout = 2")
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatal(err)
		}
		row := dbt.mustQueryRow("select connection_id();")
		var connectionIDOld int
		err := row.Scan(&connectionIDOld)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}

		time.Sleep(3 * time.Second)

		log.Printf("connectionIDOld %v", connectionIDOld)

		// since ping period was 5 > 3s, the only connection would be released by mysql still
		if err := dbt.db.Ping(); err == nil {
			dbt.Fatal("the connection should have been released")
		}

		// should create a new connection
		row = dbt.mustQueryRow("select connection_id();")
		var connectionIDNew int
		err = row.Scan(&connectionIDNew)
		if err != nil {
			dbt.Fatalf("row.Scan failed %v", err)
		}
		log.Printf("connectionIDNew %v", connectionIDNew)

		if connectionIDNew == connectionIDOld {
			dbt.Fatalf("should have create new connection when con was passively closed by mysql")
		}
	})
}

func TestNewMSKWithKeepAlive(t *testing.T) {
	mycnf := configForTests(t)
	conn, err := mysql.NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}
	msk := NewMSK(conn)
	msk.ClearStatus()

	notifierUnitTest = notifier.NewNotifierUnitTest()

	msk.SetOptions(
		options.WithSwitch(true),
		options.WithNotifier(notifierUnitTest),
		options.WithLogOutput(os.Stdout),
	)

	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db := &Addon{db: msk.RawDB(), msk: msk}
	defer db.Close()

	notifierUnitTest.ClearErr()

	dbt := &DBTest{t, db}
	dbt.mustExec("DROP TABLE IF EXISTS testdriver")
	dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
	for i := 0; i < 1; i++ {
		dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
	}
	dbt.mustExec("SET @@SESSION.wait_timeout = 2")
	if err := dbt.db.Ping(); err != nil {
		dbt.Fatal(err)
	}

	// NewMSK中已实现msk层的db链接数设置。
	// dbt.db.RawDB().SetMaxOpenConns(2)
	// dbt.db.RawDB().SetMaxIdleConns(1)

	row := dbt.mustQueryRow("select connection_id();")
	var connectionIDOld int
	err = row.Scan(&connectionIDOld)
	if err != nil {
		dbt.Fatalf("row.Scan failed %v", err)
	}

	time.Sleep(3 * time.Second)

	log.Printf("connectionIDOld %v", connectionIDOld)
	if err := dbt.db.Ping(); err == nil {
		dbt.Fatal("Ping() should failed since it was cloased by mysql wait_timeout")
	}
	// should create a new connection
	row = dbt.mustQueryRow("select connection_id();")
	var connectionIDNew int
	err = row.Scan(&connectionIDNew)
	if err != nil {
		dbt.Fatalf("row.Scan failed %v", err)
	}
	log.Printf("connectionIDNew %v", connectionIDNew)

	if connectionIDNew == connectionIDOld {
		dbt.Fatalf("should have create new connection when con was passively closed by mysql")
	}

}

// func TestSyncProcessNG(t *testing.T) {

// 	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
// 		notifierUnitTest.ClearErr()
// 		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
// 		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

// 		dbt.mustExec("CREATE TABLE testdriver (value tinyint, value1 tinyint)")
// 		for i := 0; i < 101; i++ {
// 			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
// 		}
// 		// dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
// 		// func (a *Addon) SyncProcess(t time.Time, query string, args []sqldriver.Value) error
// 		dbt.db.SetOption(options.WithSwitch(false))
// 		err := dbt.db.SyncProcess(time.Now().Add(-1*time.Second), "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);", []driver.Value{})
// 		if err != ErrMSKeeperClosed {
// 			t.Fatal("msk should have been closed")
// 		}
// 		dbt.db.SetOption(options.WithSwitch(true))
// 	})
// }

// func TestSyncProcessFlushTimeout(t *testing.T) {
// 	runDefaultPolicyWithOneCapacityTests(t, dsn, func(dbt *DBTest) {
// 		logmsk.MSKLog().SetOutput(os.Stdout)

// 		notifierUnitTestRaw.ClearErr()
// 		// 防止 notify 的 error由于重复被过滤，导致超时设定不准
// 		dbt.db.SetOption(options.WithMaxSilentPeriod(1 * time.Millisecond))

// 		notifierUnitTestRaw.SetNotifyDelay(0)
// 		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
// 		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
// 		for i := 0; i < 101; i++ {
// 			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
// 		}

// 		dbt.db.Flush()

// 		var wg sync.WaitGroup

// 		notifierUnitTestRaw.SetNotifyDelay(10 * time.Second)
// 		// 比capacity 2 大一点，使得ch排队，并由于Delay在11s，排队的ch写入会超时。
// 		for i := 0; i < 3; i++ {
// 			wg.Add(1)
// 			log.Print("Add")
// 			go func(idx int) {
// 				// dbt.db.Exec("select 1 from testdriver where value = '1'")
// 				dbt.mustExec(fmt.Sprintf("INSERT INTO testdriver VALUES (%v, %v)", idx, idx))
// 				wg.Done()
// 				log.Print("Done")
// 			}(i)
// 		}
// 		wg.Wait()

// 		err := dbt.db.SyncProcess(time.Now().Add(-1*time.Second), "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);", []driver.Value{})
// 		if err.Error() != "MSKeeper:Flush timed out" {
// 			t.Fatalf("flush should be timed out %v", err)
// 		}

// 		notifierUnitTestRaw.SetNotifyDelay(0)
// 		// 5 < 10s，此时队列满，但是结合Flush的5s，能够处理结束。
// 		time.Sleep(5 * time.Second)

// 		// 行数检查由于channel满，被忽略了，所以此处检查不应报错
// 		if notifierUnitTestRaw.WithNoErrs() == false {
// 			dbt.Errorf("rows absolutely catched unexpectly")
// 		}
// 		logmsk.MSKLog().SetOutput(ioutil.Discard)
// 	})
// }
