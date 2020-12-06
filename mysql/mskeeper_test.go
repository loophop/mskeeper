package mysql

import (
	"context"
	"database/sql"
	// "database/sql/driver"
	"fmt"
	mskdriver "gitlab.papegames.com/fringe/mskeeper/driver"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestPolicyOptionsKeywordFilterEmpty(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn + "&columnsWithAlias=true")
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		dbt.db.Exec("")

		msk.Flush()
	})
}

func TestPolicyOptionsKeywordFilterNormal(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		msk := MSKeeperInstance(dsn + "&columnsWithAlias=true")
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		notifierUnitTest.ClearErr()

		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		dbt.mustExec("ALTER TABLE `test` CHANGE `value1` `value1` BIGINT(11)  NULL  DEFAULT NULL;")

		dbt.mustExec("RENAME TABLE `test` TO `test123`;")

		dbt.mustExec("RENAME TABLE `test123` TO `test`;")

		dbt.mustExec("TRUNCATE TABLE test")

		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		dbt.mustExec("DROP TABLE IF EXISTS test333")
		dbt.mustExec("CREATE TABLE test333 (value3 int, value4 int)")

		dbt.db.Exec("CREATE DATABASE `testdb` DEFAULT CHARACTER SET = `utf8`;")
		dbt.db.Exec("USE testdb")

		dbt.db.Exec("USE mskeepertestmysql")

		dbt.db.Exec(" DROP PROCEDURE IF EXISTS test_signal; CREATE PROCEDURE test_signal(ret INT) BEGIN END")
		defer dbt.db.Exec("DROP PROCEDURE test_signal")

		// text protocol
		rows, err := dbt.db.Query("CALL test_signal(42)")
		if err == nil {
			rows.Close()
		}

		dbt.db.Exec("USE mskeepertestmysql")
		dbt.mustExec("DROP TABLE IF EXISTS test")
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")

		dbt.mustExec("LOCK TABLES test write")
		dbt.mustExec("UNLOCK TABLES")

		dbt.mustExec("DROP TABLE test")

		msk.Flush()

		log.Printf("notifierUnitTest.ErrsCount() = %v len(dbt.db.GetErr()) = %v",
			notifierUnitTest.ErrsCount(), len(msk.GetErr()))

		// CREATE and DROP, etc. if not skipped
		if len(msk.GetErr()) != 100 {
			dbt.Errorf("CREATE and TABLE not skipped")
		}

	})
}

func TestPolicyOptions(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}
		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()
	})
}

func TestPolicyOptionsNotifier(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		dbt.mustExec("CREATE TABLE test (value int, value1 int)")

		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		noti := options.FetchNotifier(msk.GetOptions())
		noti.Notify(notifier.InfoLevel, "just check, OK", []error{policy.NewPolicyErrorSafe(123, 1*time.Second)})

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeSafe) {
			dbt.Errorf("notifier not the one underline")
		}
	})
}

func TestPolicyOptionsSwitch(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		msk.Flush()

		// switch off
		msk.SetOption(options.WithSwitch(false))
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		dbt.mustQueryRow("select * from test where test.value in (select value from test);")
		msk.Flush()
		if notifierUnitTest.WithNoErrs() == false {
			dbt.Errorf("switch not off properly")
		}

		// switch on
		msk.SetOption(options.WithSwitch(true))
		dbt.mustQueryRow("select * from test where test.value in (select value from test);")
		msk.Flush()
		log.Printf("dbt.db.GetErr() %v", msk.GetErr())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}

	})
}

func TestPolicyOptionsSwitchInProcess(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		msk.Flush()
		for i := 0; i < 10; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		var wg sync.WaitGroup
		// 10 << capacity 1024，所以不存在dbt.db.SetOption(WithSwitch(true)) 作用不到 dbt.mustQueryRow
		log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				msk.SetOption(options.WithSwitch(true))
				wg.Done()
			}()
			dbt.mustQueryRow("select * from test where test.value in (select value from test) and test.value = ?;", i)
			// 这条语句的执行，时序上和Process是并行的，期望这句能够和process中的开关处理碰撞，是的某些select处理可以跳过
			wg.Add(1)
			go func() {
				msk.SetOption(options.WithSwitch(false))
				wg.Done()
			}()
		}
		msk.Flush()
		wg.Wait()

		// create+select version+10*insert+1000*select
		if notifierUnitTest.ErrsCount() >= 22 {
			dbt.Errorf("switch off not work properly in process routine of mskeeper, %v", notifierUnitTest.ErrsCount())
		}

		log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
		log.Printf("notifierUnitTest.GetErrs() = %v", notifierUnitTest.GetErrs())
		log.Printf("notifierUnitTest.GetSQLs() = %v", notifierUnitTest.GetSQLs())

	})
}

func TestPolicyOptionsMaxSilentPeriod(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {

		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		for i := 0; i < 1001; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		// 设置30s 安静期，
		msk.Flush()
		msk.SetOption(options.WithMaxSilentPeriod(30 * time.Second))
		dbt.mustExec("insert into test select * from test where test.value in (select value from test);")
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}
		msk.Flush()

		notifierUnitTest.ClearErr()
		log.Printf("看不清。。。。。。。。。分割一下")

		dbt.mustExec("insert into test select * from test where test.value in (select value from test);")
		msk.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("in slient period, should not notify ErrPolicyCodeRowsAbs")
		}

		// 下一个新的Period开始，所有错误正常告警。
		time.Sleep(31 * time.Second)
		notifierUnitTest.ClearErr()
		dbt.mustQueryRow("insert into test select * from test where test.value in (select value from test);")
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields not covered")
		}

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("rows involved not covered")
		}

	})
}

type SimpleWriterUT struct {
	logs []string
}

func NewSimpleWriterUT() *SimpleWriterUT {

	return &SimpleWriterUT{}
}
func (swut *SimpleWriterUT) Count() int {

	return len(swut.logs)
}
func (swut *SimpleWriterUT) GetLogs() []string {
	return swut.logs
}

func (niow *SimpleWriterUT) Write(p []byte) (n int, err error) {
	niow.logs = append(niow.logs, string(p))

	return len(p), nil
}

// Race condition of log instance of msk, TODO

// func TestPolicyOptionsLogOutput(t *testing.T) {
// 	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {
// 		notifierUnitTest.ClearErr()

// 		msk := MSKeeperInstance(dsn)
// 		if msk == nil {
// 			t.Fatalf("msk is nil")
// 		}

// 		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

// 		nsw := NewSimpleWriterUT()
// 		msk.SetOption(options.WithLogOutput(nsw))
// 		time.Sleep(5 * time.Second)

// 		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
// 		msk.Flush()
// 		for i := 0; i < 10; i++ {
// 			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
// 		}
// 		msk.Flush()

// 		// 当前输出的日志条数
// 		curCount := nsw.Count()
// 		if curCount <= 0 {
// 			dbt.Fatalf("curCount %v <= 0", curCount)
// 		}
// 		outoput := options.FetchLogOutput(msk.opts)
// 		outoput.Write([]byte("append one more logs"))
// 		newCount := nsw.Count()
// 		if nsw.Count() != curCount+1 {
// 			dbt.Errorf("nsw.Count() %v != curCount+1 %v", nsw.Count(), curCount+1)
// 		}

// 		// 置空output，再来一次
// 		msk.SetOption(options.WithLogOutput(ioutil.Discard))
// 		for i := 0; i < 10; i++ {
// 			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
// 		}
// 		msk.Flush()

// 		if nsw.Count() != newCount {
// 			dbt.Errorf("nsw.Count() %v != newCount %v", nsw.Count(), newCount)
// 		}

// 		log.Printf("notifierUnitTest.Count() = %v", nsw.Count())
// 		log.Printf("notifierUnitTest.GetLogs() = %v", nsw.GetLogs())
// 	})
// }

func TestPolicyOptionsLogLevelTests(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

		nsw := NewSimpleWriterUT()
		msk.SetOption(options.WithLogOutput(nsw))
		msk.SetOption(options.WithLogLevel(notifier.WarnLevel))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		msk.Flush()
		for i := 0; i < 10; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		// 当前输出的日志条数，正常没有报错的情况下，输出应该是空
		curCount := nsw.Count()
		if curCount > 0 {
			dbt.Errorf("curCount %v > 0", curCount)
		}
		logLevel := options.FetchLogLevel(msk.GetOptions())
		if logLevel != notifier.WarnLevel {
			dbt.Errorf("logLevel %v != notifier.WarnLevel %v", logLevel, notifier.WarnLevel)
		}

		msk.SetOption(options.WithLogLevel(notifier.InfoLevel))
		for i := 0; i < 10; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		// 每条SQL分析，有4条Info日志
		if nsw.Count() != 10*6 {
			dbt.Errorf("nsw.Count() %v != 10*6", nsw.Count())
		}

		// 再次设置Warn
		msk.SetOption(options.WithLogLevel(notifier.WarnLevel))
		// dbt.db.SetOption(WithLogOutput(os.Stdout))
		for i := 0; i < 10; i++ {
			// wrong sql, should output warning
			dbt.db.QueryRow("select xxxvalue from test where 13123123=test.value and value = ?", i)
		}
		msk.Flush()

		if nsw.Count() != 10*6+10 {
			dbt.Errorf("nsw.Count() %v != 60+10", nsw.Count())
		}

		log.Printf("nsw.GetLogs() = %v", nsw.GetLogs())
	})
}

func TestPolicyOptionsSQLWhiteList(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		msk.Flush()
		for i := 0; i < 1010; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		// 加入了白名单，应该被忽略
		msk.SetOption(options.WithSQLWhiteLists("select * from test where test.value in (select value from test) and test.value = ?;"))
		dbt.mustQueryRow("select * from test where test.value in (select value from test) and test.value = ?;", 1)
		time.Sleep(100 * time.Millisecond)
		msk.Flush()
		msk.Flush()
		if !notifierUnitTest.WithNoErrs() {
			dbt.Errorf("sql whitelist not work properly in process routine of mskeeper")
		}
		log.Printf("22222222222222notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())

		notifierUnitTest.ClearErr()
		time.Sleep(10 * time.Second)
		// 没有加入白名单，应该被检测出来
		dbt.mustQueryRow("select value1 from test where test.value in (select value from test) and test.value = ?;", 111)
		time.Sleep(10 * time.Second) // 上面这个语句跑的时间不确定，1s保险一点可以进入队列然后分析完毕。
		msk.Flush()
		msk.Flush()
		msk.Flush()
		log.Printf("11111111111111notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
		if notifierUnitTest.WithNoErrs() {
			dbt.Errorf("sql whitelist not work properly in process routine of mskeeper")
		}

		log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
	})
}

type DBTestRaw struct {
	*testing.T
	db *sql.DB
}

func (dbt *DBTestRaw) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTestRaw) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTestRaw) mustQueryRow(query string, args ...interface{}) (row *sql.Row) {
	row = dbt.db.QueryRow(query, args...)
	if row == nil {
		dbt.fail("queryrow", query, nil)
	}
	return row
}

func (dbt *DBTestRaw) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
}

func TestPolicyFlushTimeoutOK(t *testing.T) {
	runDefaultPolicyWithOneCapacityTests(t, dsn, func(dbt *DBTest) {
		logmsk.MSKLog().SetOutput(os.Stdout)
		notifierUnitTestRaw.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			dbt.Fatalf("msk is nil")
		}
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1000; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		notifierUnitTestRaw.SetNotifyDelay(10 * time.Second)

		// Makesure the channel buffer is of length 2
		for i := 0; i < 3; i++ {
			dbt.mustQueryRow("select * from test where value = ?", i)
		}
		// trigger 5 sec timeout of Flush
		start := time.Now()
		var err error

		log.Printf("Started Flush")
		err = msk.Flush()

		secs := time.Since(start).Seconds()
		log.Printf("flushed took %vs notifierUnitTestRaw len %v", secs, len(notifierUnitTestRaw.GetErrs()))

		if err == nil && (int(secs) < mskdriver.MaxTimeoutSecondsForFlush || int(secs) > mskdriver.MaxTimeoutSecondsForFlush+1) {
			dbt.Errorf("flush not timed out with MaxTimeoutSecondsForFlush %v actual :%0.1vs ",
				mskdriver.MaxTimeoutSecondsForFlush, secs)
		}
		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestPolicyafterFlushTimeoutFail(t *testing.T) {
	runDefaultPolicyWithOneCapacityTests(t, dsn, func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		notifierUnitTestRaw.SetNotifyDelay(10 * time.Second)
		var wg sync.WaitGroup

		// 比capacity 2 大一点，使得ch后来的都忽略
		for i := 0; i < 3; i++ {
			wg.Add(1)
			log.Print("Add")
			go func(idx int) {
				// dbt.db.Exec("select 1 from test where value = '1'")
				dbt.mustExec("INSERT INTO test VALUES (?, ?)", idx, idx)
				wg.Done()
				log.Print("Done")
			}(i)
		}
		wg.Wait()

		rows := dbt.mustQuery("SELECT value from test ")
		defer rows.Close()

		for rows.Next() {
			var value int

			if err := rows.Scan(&value); err != nil {
				dbt.Errorf("rows.Scan failed %v", err)
			}
			log.Printf("value %v ", value)
		}

		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				// dbt.db.Exec("select 1 from test where value = '1'")
				dbt.mustExec("INSERT INTO test VALUES (?, ?)", idx, idx)
				wg.Done()
			}(i)
		}
		wg.Wait()

		row := dbt.mustQueryRow("SELECT count(1) from test ")

		var count int
		err := row.Scan(&count)
		if err != nil {
			dbt.Errorf("row.Scan failed %v", err)
		}
		if count != 6 {
			dbt.Fatalf("something wrong with channel of mskeeper %v", count)
		}
		log.Printf("count %v ", count)

	})
}

func TestPolicyafterFlushTimeoutSkip(t *testing.T) {
	runDefaultPolicyWithOneCapacityTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTestRaw.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		logmsk.MSKLog().SetOutput(os.Stdout)

		// 防止 notify 的 error由于重复被过滤，导致超时设定不准
		msk.SetOption(options.WithMaxSilentPeriod(1 * time.Millisecond))

		notifierUnitTestRaw.SetNotifyDelay(0)
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		msk.Flush()
		var wg sync.WaitGroup

		notifierUnitTestRaw.SetNotifyDelay(10 * time.Second)
		// 比capacity 2 大一点，使得ch排队，并由于Delay在11s，排队的ch写入会超时。
		for i := 0; i < 3; i++ {
			wg.Add(1)
			log.Print("Add")
			go func(idx int) {
				// dbt.db.Exec("select 1 from test where value = '1'")
				dbt.mustExec(fmt.Sprintf("INSERT INTO test VALUES (%v, %v)", idx, idx))
				wg.Done()
				log.Print("Done")
			}(i)
		}
		wg.Wait()

		// 4*2 < 10s，此时队列满
		time.Sleep(4 * time.Second)

		// 以下insert语句刚刚好被Skip。
		notifierUnitTestRaw.SetNotifyDelay(0)
		dbt.mustExec("insert into test select * from test where test.value in (select value from test);")
		msk.Flush()

		// 行数检查由于channel满，被忽略了，所以此处检查不应报错
		if notifierUnitTestRaw.WithNoErrs() == false {
			dbt.Errorf("rows absolutely catched unexpectly")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)

	})
}

func TestPolicyExcutionWithNoErrs(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE test (value2 int, value1 int)")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		dbt.mustExec("insert into test select * from test where test.value2 in (select value2 from test);")

		rows := dbt.mustQuery("select * from test where test.value2 in (select value1 from test);")
		defer rows.Close()
		msk.Flush()

		if !notifierUnitTest.WithNoErrs() {
			dbt.Errorf("should have no errors")
		}
	})
}

func TestPolicyExcutionTime1(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		logmsk.MSKLog().SetOutput(os.Stdout)
		log.Printf("--------------------------1")
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		for i := 0; i < 10000; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		log.Printf("--------------------------2")

		msk.SetOption(options.WithMaxExecTime(30 * time.Millisecond))
		dbt.mustExec("insert into test select * from test where test.value in (select value from test);")
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}

		msk.SetOption(options.WithMaxExecTime(5 * time.Millisecond))
		rows := dbt.mustQuery("select * from test where test.value in (select value from test);")
		msk.Flush()
		defer rows.Close()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}
		msk.Flush()
		log.Printf("--------------------------3")
		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestPolicyExcutionTime2(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		msk.SetOption(options.WithMaxExecTime(10 * time.Second))
		dbt.mustExec("insert into test select * from test where test.value1 in (select value1 from test);")
		msk.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost banned")
		}

		msk.SetOption(options.WithMaxExecTime(1 * time.Millisecond))
		dbt.mustExec("insert into test select * from test where test.value1 in (select value from test);")
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}
	})
}

func TestPolicyRowsInvolved(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		logmsk.MSKLog().SetOutput(os.Stdout)
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))

		dbt.mustExec("CREATE TABLE `test` ( `value` int(11) DEFAULT NULL, `value1` int(11) DEFAULT NULL, KEY `value1` (`value1`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 3004; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		// hit the index, its ok
		dbt.mustQueryRow("select * from test where value1 < 20 limit 1")
		msk.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("rows involved 1/3 illegally trigged")
		}

		// hit the index, however the rows larger than 1/3 all rows
		dbt.mustQueryRow("select * from test where value1 < 1005")
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("rows involved 1/3 not 35 trigged")
		}

		// no index, all table scan
		dbt.mustQueryRow("select * from test where value > 20")
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("rows involved 1/3 not trigged")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestPolicyRowsAbsolute(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE test (value int, value1 int)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}

		dbt.mustExec("insert into test select value, value1 from test where test.value in (select value from test);")
		msk.Flush()
		log.Printf("msk.GetErr() %v", msk.GetErr())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}
		msk.ClearErr()

		if msk.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("cached errors not cleared")
		}

		dbt.mustQueryRow("select * from test where test.value in (select value from test);")
		msk.Flush()
		log.Printf("msk.GetErr() %v", msk.GetErr())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}
		if !msk.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}

		msk.ClearErr()
	})
}

func TestPolicySwitch(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE test (value int, value2 int)")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		msk.SetOption(options.WithSwitch(false))
		msk.SetOption(options.WithMaxExecTime(10 * time.Millisecond))
		dbt.mustExec("insert into test select * from test where test.value2 in (select value from test);")
		msk.Flush()
		// should not detect error
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost banned illegally")
		}

		// open again
		msk.SetOption(options.WithSwitch(true))
		msk.SetOption(options.WithMaxExecTime(1 * time.Millisecond))
		dbt.mustExec("insert into test select * from test where test.value2 in (select value from test);")
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}
	})
}

func TestPolicyFieldsTypeSimple(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		logmsk.MSKLog().SetOutput(os.Stdout)

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1010; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		var godamnv int = 5

		rows := dbt.mustQuery("select * from test where value1 = ?", godamnv)
		defer rows.Close()
		msk.Flush()

		// value1 is string
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Fatalf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("cost should be ok")
		}

		// 5.6 以下，全表1010会触发 Involve告警，5.7OK
		// if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
		// 	dbt.Fatalf("involve rows should be ok")
		// }
		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestPolicyFieldsTypeUnionOK(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test where test.value1 = '111' union select * from test where test.value1 = '222';")
		defer rows.Close()
		msk.Flush()

		// UnionResult should be skipped
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("involve rows should be ok")
		}

	})
}

func TestPolicyFieldsTypeUnionFail(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}
		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 10002; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test where test.value1 = 111 union select * from test where test.value1 = '222';")
		defer rows.Close()
		msk.Flush()

		// value1 = 111 should be cached for type of value1 is varchar
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}
		// 5.6 以下，全表1010会触发 Involve告警，5.7OK
		// if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
		// 	dbt.Errorf("involve rows should be ok")
		// }

	})
}

// 全表扫描有where语句
func TestPolicyFieldsTypeAllWithWhere(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}
		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1010; i++ { //
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test where test.value1 = 111 union select * from test where test.value1 = '222';")
		defer rows.Close()
		msk.Flush()

		// value1 = 111 should be cached for type of value1 is varchar
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}

		// 5.6 以下，全表1010会触发 Involve告警，5.7OK
		// if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
		// 	dbt.Errorf("involve rows should be ok")
		// }

	})
}

// 全表扫描没有where语句, 但是行数不超过1000行
func TestPolicyFieldsTypeAllWithNotWhereOK(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}
		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 99; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test")
		defer rows.Close()
		msk.Flush()

		// value1 = 111 should be cached for type of value1 is varchar
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("involve rows should be ok")
		}

	})
}

// 全表扫描没有where语句, 但是行数超过1000行
func TestPolicyFieldsTypeAllWithNotWhereNG(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, i)
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test")
		defer rows.Close()
		time.Sleep(1 * time.Second)
		msk.Flush()
		msk.Flush()
		msk.Flush()
		// value1 = 111 should be cached for type of value1 is varchar
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("involve rows should be ok")
		}

	})
}

func TestPolicyFieldsLengthStringOK1(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO test VALUES (?, ?)", i, fmt.Sprintf("%v", i))
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test")
		defer rows.Close()
		msk.Flush()

		tx, err := dbt.db.Begin()
		if err != nil {
			t.Errorf("TestPolicyFieldsLengthStringOK1: start transaction failed")
		}

		if _, err := tx.Exec("INSERT INTO test VALUES(?, ?)", 11111, "222222"); err != nil {
			tx.Rollback()
			t.Errorf("TestPolicyFieldsLengthStringOK1: transaction exec insert failed")
		}
		if _, err := tx.Exec("UPDATE test SET value1 = ? WHERE value = ? ", "222222", 22222); err != nil {
			tx.Rollback()
			t.Errorf("TestPolicyFieldsLengthStringOK1: transaction exec insert failed")
		}

		tx.Commit()

		// value1 = 111 should be cached for type of value1 is varchar
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("involve rows should be ok")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Errorf("code truncate should be ok")
		}
	})
}

func TestPolicyFieldsLengthStringOK2(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value2` varchar(5), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO test VALUES (?, ?, ?)", i, "1234", i)
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test")
		defer rows.Close()
		msk.Flush()

		// value1 = 111 should be cached for type of value1 is varchar
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("involve rows should be ok")
		}
		// value2 5*0.8 =4
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Errorf("code truncate should be ok")
		}
	})
}

func TestPolicyFieldsLengthStringNG1(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		msk.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `test` ( `value` int(11), `value2` varchar(5), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO test VALUES (?, ?, ?)", i, "123456", i+1)
		}
		msk.Flush()

		rows := dbt.mustQuery("select * from test")
		defer rows.Close()
		msk.Flush()

		// value1 = 111 should be cached for type of value1 is varchar
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Errorf("fields check not banned")
		}

		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("cost should be ok")
		}

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("involve rows should be ok")
		}

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Errorf("code truncate should be NG")
		}

		log.Printf("msk.GetErr() = %v len(msk.GetErr() = %v)", notifierUnitTest.GetErrs(), len(msk.GetErr()))
	})
}

// 各种SQL语句都过一下，看是否有漏掉，没有进mskeeper的process的
func TestPolicyTouchAllTypeOfDMLs(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		notifierUnitTest.ClearErr()
		msk := MSKeeperInstance(dsn)
		if msk == nil {
			t.Fatalf("msk is nil")
		}

		msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())
		msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

		dbt.mustExec("CREATE TABLE `test` (`value` varchar(2) DEFAULT NULL,`value2` varchar(26) DEFAULT NULL,`value1` int(11) unsigned DEFAULT NULL,`value3` mediumtext,`value4` blob, KEY `value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2001; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?, ?, ?, ?)", "1", "1", "1", "1", "1")
		}
		// Case 1
		_, err := dbt.db.Exec("INSERT INTO test(value, value1) VALUES (?, 123123)", "123")
		if err != nil {
			dbt.Fatal(err)
		}
		msk.Flush()

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("code truncate should be NG")
		}
		notifierUnitTest.ClearErr()

		_, err = dbt.db.Exec("INSERT INTO test(value, value1) VALUES (?, 123123)", "223")
		if err != nil {
			dbt.Fatal(err)
		}
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("code truncate should be NG")
		}
		notifierUnitTest.ClearErr()

		// Case 2
		row := dbt.db.QueryRow("SELECT * from test where value = ?", "1")
		if row == nil {
			dbt.Fatal("QueryRow failed")
		}
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}

		notifierUnitTest.ClearErr()

		// Case 3
		rows, err := dbt.db.Query("SELECT * from test where value = ?", "4")
		if err != nil {
			dbt.Fatal(err)
		}
		defer rows.Close()

		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}

		notifierUnitTest.ClearErr()

		// Case 4
		tx, err := dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		row = tx.QueryRow("SELECT * from test where value = ?", "2")
		if row == nil {
			tx.Rollback()
			dbt.Fatal("QueryRow failed")
		}

		tx.Commit()
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 5
		rows, err = dbt.db.Query("SELECT * from test where value = ?", "3")
		if err != nil {
			dbt.Fatal("Query failed")
		}
		defer rows.Close()
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 6
		log.Printf("------------------------------------------------------------------------------------------")
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		_, err = tx.Exec("INSERT INTO test(value, value1) VALUES (?, 123123)", "333")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}
		tx.Commit()
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 7
		con, err := dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		_, err = con.ExecContext(context.Background(), "INSERT INTO test(value, value1) VALUES (?, 123123)", "444")
		if err != nil {
			dbt.Fatal(err)
		}
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 8
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		rows, err = con.QueryContext(context.Background(), "SELECT * from test where value = ?", "5")
		if err != nil {
			dbt.Fatal(err)
		}
		defer rows.Close()
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 9
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		row = con.QueryRowContext(context.Background(), "SELECT * from test where value = ?", "6")
		if row == nil {
			dbt.Fatalf("should be ok")
		}
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 10
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		con.Close()

		_, err = con.QueryContext(context.Background(), "SELECT * from test where value = ?", "1")
		// already closed
		if err == nil {
			dbt.Fatalf("closed conn with %v", err)
		}

		// Case 11
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		con.Close()

		err = con.PingContext(context.Background())
		// already closed
		if err == nil {
			dbt.Fatal(err)
		}

		// Case 12
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		con.Close()

		_, err = con.ExecContext(context.Background(), "UPDATE test set value = ?", "1")
		// already closed
		if err == nil {
			dbt.Fatal(err)
		}

		// Case 13
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		con.Close()

		_, err = con.PrepareContext(context.Background(), "UPDATE test set value = ?")
		// already closed
		if err == nil {
			dbt.Fatal(err)
		}

		// Case 14
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		con.Close()

		_, err = con.BeginTx(context.Background(), nil)
		// already closed
		if err == nil {
			dbt.Fatal(err)
		}

		// Case 15
		stmt, err := dbt.db.Prepare("SELECT * from test where value = ?")
		if err != nil {
			dbt.Fatal(err)
		}
		res, err := stmt.Exec("1")
		if err != nil {
			dbt.Fatalf("stmt.Exec failed %v", err)
		}
		ra, _ := res.RowsAffected()
		if ra > 0 {
			dbt.Fatalf("should be failed")
		}
		msk.Flush()

		res, err = stmt.Exec("22")
		if err != nil {
			dbt.Fatalf("stmt.Exec failed %v", err)
		}
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 16
		stmt, err = dbt.db.Prepare("SELECT * from test where value = ?")
		if err != nil {
			dbt.Fatal(err)
		}
		rows, err = stmt.Query("526")
		if err != nil {
			dbt.Fatalf("stmt.Query failed %v", err)
		}
		defer rows.Close()
		if rows.Next() {
			dbt.Fatalf("should be empty")
		}
		msk.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()
		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}
