package service

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"gitlab.papegames.com/fringe/mskeeper/driver"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"

	"os"
	// "sync"
	"testing"
	"time"
)

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

func TestMySQLCheckNormal(t *testing.T) {

	// 获取 msid
	sess, err := NewMySQLService(dsn, 1000)
	assert.True(t, err == nil, "New mysql service failure")

	// normal
	request := ServiceCheckSQLRequest{
		SQL:   "select * from test_tmp where value = ?",
		MSID:  sess.GetUUID(),
		Args:  []interface{}{1},
		STime: time.Now(),
	}
	response := &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}

	assert.True(t, response.Notifies[0].Code == policy.ErrPolicyCodeSafe, "sql safe")

}

func TestMySQLCheckNormalPolicy(t *testing.T) {

	logmsk.MSKLog().SetOutput(os.Stdout)

	Init("../langs")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(6),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 2002; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO test_tmp VALUES (?, ?)", i, fmt.Sprintf("%v", i))
		if err != nil {
			log.Printf("Exec err = %v i = %v", err, i)
			panic(err)
		}
	}

	// 获取 msid
	sess, err := NewMySQLService(dsn, 0)
	assert.True(t, err == nil, "New mysql service failure")

	// Policy: ErrPolicyCodeRowsInvolve/ErrPolicyCodeAllTableScan
	request := ServiceCheckSQLRequest{
		SQL:   "select value, value1 from test_tmp where value1 = ?",
		MSID:  sess.GetUUID(),
		Args:  []interface{}{1},
		STime: time.Now(),
	}

	response := &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}
	// 由于8.0和5.6对于类型不匹配的优化成都不同，8.0的rowaffected是200，5.6时2000，所以此处满足任意一个即可
	if !(response.Notifies[0].Code == policy.ErrPolicyCodeRowsInvolve || response.Notifies[0].Code == policy.ErrPolicyCodeAllTableScan) {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	// Policy: ErrPolicyCodeExeCost, Use Default 3s
	request = ServiceCheckSQLRequest{
		SQL:   "select value, sleep(3) from test_tmp where value = 1",
		MSID:  sess.GetUUID(),
		STime: time.Now().Add(-3001 * time.Millisecond),
	}

	response = &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}
	if response.Notifies[0].Code != policy.ErrPolicyCodeExeCost {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	// Policy: ErrPolicyCodeDataTruncate
	request = ServiceCheckSQLRequest{
		SQL:   "update test_tmp set value1 = ? where value = 1",
		MSID:  sess.GetUUID(),
		Args:  []interface{}{"1234567"},
		STime: time.Now(),
	}

	response = &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}

	if response.Notifies[0].Code != policy.ErrPolicyCodeDataTruncate {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	// Policy: WarnPolicyCodeDataTruncate
	request = ServiceCheckSQLRequest{
		SQL:  "update test_tmp set value1 = ? where value = 1",
		MSID: sess.GetUUID(),
		Args: []interface{}{"123456"}, // 0.9 rate reached
	}

	response = &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}

	if response.Notifies[0].Code != policy.WarnPolicyCodeDataTruncate {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestMySQLRunNormalPolicy(t *testing.T) {

	logmsk.MSKLog().SetOutput(os.Stdout)

	Init("../langs")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(6),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 2002; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO test_tmp VALUES (?, ?)", i, fmt.Sprintf("%v", i))
		if err != nil {
			log.Printf("Exec err = %v i = %v", err, i)
			panic(err)
		}
	}

	// 获取 msid
	sess, err := NewMySQLService(dsn, 1000)
	assert.True(t, err == nil, "New mysql service failure")

	// Policy: ErrPolicyCodeRowsInvolve/ErrPolicyCodeAllTableScan
	request := ServiceRunSQLRequest{
		SQL:  "select value, value1 from test_tmp where value1 = ?",
		MSID: sess.GetUUID(),
		Args: []interface{}{1},
	}

	response := &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}
	// 由于8.0和5.6对于类型不匹配的优化成都不同，8.0的rowaffected是200，5.6时2000，所以此处满足任意一个即可
	if !(response.Notifies[0].Code == policy.ErrPolicyCodeRowsInvolve || response.Notifies[0].Code == policy.ErrPolicyCodeAllTableScan) {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	// Policy: ErrPolicyCodeExeCost
	request = ServiceRunSQLRequest{
		SQL:  "select value, sleep(4) from test_tmp where value = 1",
		MSID: sess.GetUUID(),
	}

	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}
	if response.Notifies[0].Code != policy.ErrPolicyCodeExeCost {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	// Policy: ErrPolicyCodeDataTruncate
	request = ServiceRunSQLRequest{
		SQL:  "update test_tmp set value1 = ? where value = 1",
		MSID: sess.GetUUID(),
		Args: []interface{}{"1234567"},
	}

	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}

	if response.Notifies[0].Code != policy.ErrPolicyCodeDataTruncate {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	// Policy: WarnPolicyCodeDataTruncate
	request = ServiceRunSQLRequest{
		SQL:  "update test_tmp set value1 = ? where value = 1",
		MSID: sess.GetUUID(),
		Args: []interface{}{"123456"}, // 0.9 rate reached
	}

	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}

	if response.Notifies[0].Code != policy.WarnPolicyCodeDataTruncate {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestMySQLRunNormal(t *testing.T) {

	Init("../langs")

	// 获取 msid
	sess, err := NewMySQLService(dsn, 1000)
	assert.True(t, err == nil, "New mysql service failure")

	// normal
	request := ServiceRunSQLRequest{
		SQL:  "drop table if exists test_tmp",
		MSID: sess.GetUUID(),
	}
	response := &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	request = ServiceRunSQLRequest{
		SQL:  "CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(60),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	request = ServiceRunSQLRequest{
		SQL:  "insert into test_tmp values(?,'1'),(?,'2'),(?,'3')",
		MSID: sess.GetUUID(),
		Args: []interface{}{1, 2, 3},
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}
	if response.Result.RowsAffected != 3 {
		t.Fatalf("rows affected %v !=3 ", response.Result.RowsAffected)
	}

	request = ServiceRunSQLRequest{
		SQL:  "select * from test_tmp",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}
	if response.Result.RowsAffected != 0 {
		t.Fatalf("response.Result.RowsAffected != 0 ")
	}
	if response.Result.Columns[0].Name != "value" ||
		response.Result.Columns[1].Name != "value1" {
		t.Fatalf("response.Result.Columns %v 's Name wrong ", response.Result.Columns)
	}
	if len(response.Result.Rows) != 3 {
		t.Fatalf("len(response.Result.Rows) %v != 3 ", len(response.Result.Rows))
	}

	for i := 0; i < len(response.Result.Rows); i++ {
		if response.Result.Rows[i][0].(int64) != int64(i+1) {
			t.Fatalf("response.Result.Rows[i][0] %v != %v ", response.Result.Rows[i][0], i+1)
		}
		if response.Result.Rows[i][1].(string) != fmt.Sprintf("%v", i+1) {
			t.Fatalf("response.Result.Rows[i][1] %v != %v ", response.Result.Rows[i][1], fmt.Sprintf("%v", i+1))
		}
	}

	request = ServiceRunSQLRequest{
		SQL:  "drop table if exists test_tmp",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}
}

func TestMySQLRunUnsupportSQL(t *testing.T) {

	Init("../langs")

	// 获取 msid
	sess, err := NewMySQLService(dsn, 0)
	assert.True(t, err == nil, "New mysql service failure")

	// unsupport sql
	request := ServiceRunSQLRequest{
		SQL:  "START TRANSACTION",
		MSID: sess.GetUUID(),
	}
	response := &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != ErrStatementNotSupported {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	// unsupport sql
	request = ServiceRunSQLRequest{
		SQL:  "BEGIN",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != ErrStatementNotSupported {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	// unsupport sql
	request = ServiceRunSQLRequest{
		SQL:  "rollback",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != ErrStatementNotSupported {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	// unsupport sql
	request = ServiceRunSQLRequest{
		SQL:  "commit",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != ErrStatementNotSupported {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	request = ServiceRunSQLRequest{
		SQL:  "CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(60),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	// unsupport sql
	request = ServiceRunSQLRequest{
		SQL:  "LOCK TABLES test_tmp write",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err == nil { // banned by sql.parse
		t.Fatalf("sess.RunSQL failed in ban LOCK TABLES")
	}

	request = ServiceRunSQLRequest{
		SQL:  "DROP TABLE IF EXISTS test_tmp",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

}

func TestMySQLPingNormal(t *testing.T) {

	// 获取 msid
	sess, err := NewMySQLService(dsn, 0)
	assert.True(t, err == nil, "New mysql service failure")

	// normal
	request := ServicePingDBRequest{
		Timeout: 5000000, // 5s
	}
	response := &ServicePingDBResponse{Request: request}
	err = sess.PingDB(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	request = ServicePingDBRequest{
		// use default value 5s
	}
	response = &ServicePingDBResponse{Request: request}
	err = sess.PingDB(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	request = ServicePingDBRequest{
		Timeout: -1, // set to default
	}
	response = &ServicePingDBResponse{Request: request}
	err = sess.PingDB(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	request = ServicePingDBRequest{
		Timeout: 10 * 1000, // 10 ms // normal it's enough for just opening a database
	}
	response = &ServicePingDBResponse{Request: request}
	err = sess.PingDB(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}
}

func TestMySQLPingTimeout(t *testing.T) {

	sess, err := NewMySQLService(dsn, 0)
	assert.True(t, err == nil, "New mysql service failure")

	request := ServicePingDBRequest{
		Timeout: 0.1, // 0.1 micro sec // should be failed since it's toooooo short
	}
	response := &ServicePingDBResponse{Request: request}
	err = sess.PingDB(request, response)
	if err == nil {
		t.Fatalf("sess.RunSQL should timeout ")
	}

}

func TestMySQLRunTimeout(t *testing.T) {

	Init("../langs")

	// -----------------prepare data
	totalRows := 1000
	sess, err := NewMySQLService(dsn, 1000)
	assert.True(t, err == nil, "New mysql service failure")

	request := ServiceRunSQLRequest{
		SQL:  "drop table if exists test_tmp",
		MSID: sess.GetUUID(),
	}
	response := &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	request = ServiceRunSQLRequest{
		SQL:  "CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(60),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	sqlForPrepareData := "insert into test_tmp values"
	for i := 1; i < totalRows; i++ {
		sqlForPrepareData += fmt.Sprintf("(%d, '%d'),", i, i+1)
	}
	sqlForPrepareData += "(1000, '1001')"

	request = ServiceRunSQLRequest{
		SQL:  sqlForPrepareData,
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

	if response.Result.RowsAffected != int64(totalRows) {
		t.Fatalf("rows affected %v != totalRows %v ", response.Result.RowsAffected, totalRows)
	}
	// -----------------prepare data finished

	// Query
	for i := 0; i < 100; i++ {
		log.Printf("testing row senare %v", i)
		request = ServiceRunSQLRequest{
			SQL:     "select * from test_tmp",
			MSID:    sess.GetUUID(),
			Timeout: float64(1 + rand.Intn(100)), // 1-101 microsecs
		}
		response = &ServiceRunSQLResponse{Request: request}
		err = sess.RunSQL(request, response)
		switch {
		case err == context.Canceled:
			fallthrough
		case err == context.DeadlineExceeded:
			fallthrough
		case (err != nil && strings.Contains(err.Error(), "operation was canceled")):
		case (err != nil) && strings.Contains(err.Error(), "Did you run multiple statements at once"):
		case (err != nil) && strings.Contains(err.Error(), "dial tcp: i/o timeout"):
			// In expectation
		case err == nil:
			if len(response.Result.Rows) != totalRows {
				t.Fatalf("len(response.Result.Rows) %v != %v ", len(response.Result.Rows), totalRows)
			}
			// randomly select a row to check value
			randRowIdx := rand.Intn(totalRows)
			if response.Result.Rows[randRowIdx][0].(int64) != int64(randRowIdx+1) {
				t.Fatalf("response.Result.Rows[randRowIdx][0] %v != %v ", response.Result.Rows[randRowIdx][0], randRowIdx+1)
			}
			if response.Result.Rows[randRowIdx][1].(string) != fmt.Sprintf("%v", randRowIdx+1+1) {
				t.Fatalf("response.Result.Rows[randRowIdx][1] %v != %v ", response.Result.Rows[randRowIdx][1], fmt.Sprintf("%v", randRowIdx+1+1))
			}
		default:
			t.Fatalf("unexpected err %v", err)
		}
	}
	// Execution
	for i := 0; i < 100; i++ {
		log.Printf("testing row senare %v", i)
		request = ServiceRunSQLRequest{
			SQL:     "update test_tmp set value = ? where value = ?",
			MSID:    sess.GetUUID(),
			Args:    []interface{}{i + 1, i},
			Timeout: float64(1 + rand.Intn(100)), // 1-101 microsecs
		}
		response = &ServiceRunSQLResponse{Request: request}
		err = sess.RunSQL(request, response)
		switch {
		case err == context.Canceled:
			fallthrough
		case err == context.DeadlineExceeded:
			fallthrough
		case (err != nil && strings.Contains(err.Error(), "operation was canceled")):
			fallthrough
		case (err != nil) && strings.Contains(err.Error(), "Did you run multiple statements at once"):
		case (err != nil) && strings.Contains(err.Error(), "dial tcp: i/o timeout"):
			// In expectation
		case err == nil:
			if response.Result.RowsAffected != 1 {
				t.Fatalf("response.Result.RowsAffected %v != 1 ", response.Result.RowsAffected)
			}

		default:
			t.Fatalf("unexpected err %v", err)
		}
	}
	// clear data
	request = ServiceRunSQLRequest{
		SQL:  "drop table if exists test_tmp",
		MSID: sess.GetUUID(),
	}
	response = &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("sess.RunSQL failed %v", err)
	}

}

func TestMySQLPingBadDsn(t *testing.T) {

	sess, err := NewMySQLService("mskeeper_test:123456@tcp(192.168.0.86:3306)/nodb?timeout=30s", 0)
	assert.True(t, err == nil, "New mysql service failure")

	request := ServicePingDBRequest{
		Timeout: 0.1, // 0.1 micro sec // should be failed since it's toooooo short
	}
	response := &ServicePingDBResponse{Request: request}
	err = sess.PingDB(request, response)
	if err == nil {
		t.Fatalf("sess.RunSQL should timeout")
	}

}

func TestMySQLCheckFailure(t *testing.T) {

	Init("../langs")

	logmsk.MSKLog().SetOutput(os.Stdout)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(60),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 2002; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO test_tmp VALUES (?, ?)", i, fmt.Sprintf("%v", i))
		if err != nil {
			log.Printf("Exec err = %v i = %v", err, i)
			panic(err)
		}
	}

	// 创建一个dsn
	sess, err := NewMySQLService(dsn, 0)
	assert.True(t, err == nil, "New mysql service failure")

	time.Sleep(5 * time.Second)
	sess.GetMSKeeper().Flush()
	// normal
	request := ServiceCheckSQLRequest{
		SQL:   "select * from test_tmp where value = ?",
		MSID:  sess.GetUUID(),
		Args:  []interface{}{1},
		STime: time.Now(),
	}
	response := &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}

	assert.True(t, response.Notifies[0].Code == policy.ErrPolicyCodeSafe, "sql safe")

	// 复用dsn
	sess, err = NewMySQLService(dsn, 0)
	assert.True(t, err == nil, "New mysql service failure")

	time.Sleep(5 * time.Second)
	sess.GetMSKeeper().Flush()

	// normal
	request = ServiceCheckSQLRequest{
		SQL:   "select value, value1 from test_tmp where value1 = ?",
		MSID:  sess.GetUUID(),
		Args:  []interface{}{1},
		STime: time.Now(),
	}

	// log.Printf("111111111111111111111111111111111111111111113232323232323retData %v", retData)
	response = &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("sess.CheckSQL failed %v", err)
	}
	// 由于8.0和5.6对于类型不匹配的优化成都不同，8.0的rowaffected是200，5.6时2000，所以此处满足任意一个即可
	if !(response.Notifies[0].Code == policy.ErrPolicyCodeRowsInvolve || response.Notifies[0].Code == policy.ErrPolicyCodeAllTableScan) {
		t.Fatalf("code(%v) not match", response.Notifies[0].Code)
	}

	// bad dsn
	badDsn := "maqiu@123"
	_, err = NewMySQLService(badDsn, 0)

	assert.True(t, err != nil, "sql.open failed")

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

var notifierUnitTestRaw *notifier.NotifierUnitTest

func TestMySQLCheckSyncProcessFail(t *testing.T) {
	logmsk.MSKLog().SetOutput(os.Stdout)
	Init("../langs")
	notifierUnitTestRaw = notifier.NewNotifierUnitTest()
	notifierUnitTestRaw.ClearErr()

	// 获取 msid
	sess, err := NewMySQLService(dsn+"&222", 0)
	assert.True(t, err == nil, "New mysql service failure")

	sess.GetMSKeeper().ClearStatus()
	sess.GetMSKeeper().SetOption(options.WithSwitch(true))

	// first time
	request := ServiceCheckSQLRequest{
		SQL:   "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);",
		MSID:  sess.GetUUID(),
		Args:  []interface{}{},
		STime: time.Now(),
	}

	response := &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		t.Fatalf("CheckSQL should be ok %v", err)
	}

	// second time, ignored since silence setting
	request = ServiceCheckSQLRequest{
		SQL:   "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);",
		MSID:  sess.GetUUID(),
		Args:  []interface{}{},
		STime: time.Now(),
	}

	response = &ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != driver.ErrMSKeeperSQLIgnore {
		t.Fatalf("flush should be ignored %v", err)
	}

	log.Printf("aaaaa 4 err %v", err)
	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestMySQLRunSyncCheckSQLFail(t *testing.T) {
	logmsk.MSKLog().SetOutput(os.Stdout)
	Init("../langs")

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(60),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 2002; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO test_tmp VALUES (?, ?)", i, fmt.Sprintf("%v", i))
		if err != nil {
			log.Printf("Exec err = %v i = %v", err, i)
			panic(err)
		}
	}

	// 获取 msid
	sess, err := NewMySQLService(dsn+"&222", 0)
	assert.True(t, err == nil, "New mysql service failure")

	// first time
	request := ServiceRunSQLRequest{
		SQL:  "explain insert into test_tmp select * from test_tmp where test_tmp.value in (select value from test_tmp);",
		MSID: sess.GetUUID(),
		Args: []interface{}{},
	}

	response := &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("RunSQL should be ok %v", err)
	}
	if len(response.Notifies) > 0 {
		t.Fatalf("explain should have been ignored in sql check")
	}

	log.Printf("response %v", response)

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

func TestMySQLRunIgnored(t *testing.T) {
	Init("../langs")

	logmsk.MSKLog().SetOutput(os.Stdout)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS test_tmp")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("CREATE TABLE test_tmp ( `value` int(11), `value1` varchar(60),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1001; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO test_tmp VALUES (?, ?)", i, fmt.Sprintf("%v", i))
		if err != nil {
			log.Printf("Exec err = %v i = %v", err, i)
			panic(err)
		}
	}

	// 创建一个dsn
	sess, err := NewMySQLService(dsn, 0)
	assert.True(t, err == nil, "New mysql service failure")

	time.Sleep(5 * time.Second)
	sess.GetMSKeeper().Flush()
	// normal
	request := ServiceRunSQLRequest{
		SQL:  "insert into test_tmp select * from test_tmp where test_tmp.value in (select value from test_tmp);",
		MSID: sess.GetUUID(),
		Args: []interface{}{},
	}

	response := &ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		t.Fatalf("run should not fail %v", err)
	}

	if response.Result.RowsAffected <= 0 {
		t.Fatalf("insertion failed ")
	}
	log.Printf("response %v", response)

	logmsk.MSKLog().SetOutput(ioutil.Discard)
}

// func TestMySQLRunFlushTimeout(t *testing.T) {
// 	Init("../langs")

// 	logmsk.MSKLog().SetOutput(os.Stdout)

// 	notifierUnitTestRaw = notifier.NewNotifierUnitTest()
// 	notifierUnitTestRaw.ClearErr()

// 	// 获取 msid
// 	sess, err := NewMySQLService(dsn + "&222")
// 	assert.True(t, err == nil, "New mysql service failure")

// 	// 设置用于控制上报延迟的notifier
// 	sess.GetMSKeeper().SetOption(options.WithNotifier(notifierUnitTestRaw))
// 	// 防止 notify 的 error由于重复被过滤，导致超时设定不准
// 	sess.GetMSKeeper().SetOption(options.WithMaxSilentPeriod(1 * time.Millisecond))
// 	sess.GetMSKeeper().SetOption(options.WithCapacity(2))
// 	sess.GetMSKeeper().ResyncInfoQueue()

// 	var wg sync.WaitGroup

// 	notifierUnitTestRaw.SetNotifyDelay(10 * time.Second)
// 	// 比capacity 2 大一点，使得ch排队，并由于Delay在11s，排队的ch写入会超时。
// 	for i := 0; i < 3; i++ {
// 		wg.Add(1)
// 		log.Print("Add")
// 		go func(idx int) {
// 			// dbt.db.Exec("select 1 from testdriver where value = '1'")
// 			request := ServiceRunSQLRequest{
// 				SQL:  "INSERT INTO testdriver VALUES (?, ?)",
// 				MSID: sess.GetUUID(),
// 				Args: []interface{}{idx, idx},
// 			}
// 			response := &ServiceRunSQLResponse{Request: request}
// 			_ = sess.RunSQL(request, response)

// 			wg.Done()
// 			log.Print("Done")
// 		}(i)
// 	}

// 	request := ServiceRunSQLRequest{
// 		SQL:  "insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);",
// 		MSID: sess.GetUUID(),
// 		Args: []interface{}{},
// 	}

// 	response := &ServiceRunSQLResponse{Request: request}
// 	err = sess.RunSQL(request, response)
// 	if err.Error() != "MSKeeper:Flush timed out" {
// 		t.Fatalf("flush should be timed out %v", err)
// 	}
// 	wg.Wait()
// 	log.Printf("aaaaa 4 err %v", err)
// 	logmsk.MSKLog().SetOutput(ioutil.Discard)
// }
