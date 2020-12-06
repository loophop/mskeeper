package driver

import (
	// "bytes"
	//"crypto/tls"
	// "database/sql"
	// "log"
	// "database/sql/driver"
	// "encoding/json"
	// "errors"
	// "fmt"
	// "github.com/go-sql-driver/mysql"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	// "io/ioutil"
	"sync"
	// "io"
	// "io/ioutil"
	"log"
	// "net"
	// "os"
	// "reflect"
	// "strings"
	"testing"
	"time"
)

func TestPolicyOptions(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()
	})
}

func TestPolicyOptionsNotifier(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")

		noti := options.FetchNotifier(dbt.db.GetOptions())
		noti.Notify(notifier.InfoLevel, "just check, OK", []error{policy.NewPolicyErrorSafe(123, 1*time.Second)})

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeSafe) {
			dbt.Errorf("notifier not the one underline")
		}
	})
}

func TestPolicyOptionsSwitch(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		dbt.db.Flush()

		// switch off
		dbt.db.SetOption(options.WithSwitch(false))
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		dbt.mustQueryRow("select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		if notifierUnitTest.WithNoErrs() == false {
			dbt.Errorf("switch not off properly")
		}

		// switch on
		dbt.db.SetOption(options.WithSwitch(true))
		dbt.mustQueryRow("select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		log.Printf("dbt.db.GetErr() %v", dbt.db.GetErr())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}

	})
}

func TestPolicyOptionsSwitchInProcess(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		dbt.db.Flush()
		for i := 0; i < 10; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		var wg sync.WaitGroup
		// 10 << capacity 1024，所以不存在dbt.db.SetOption(WithSwitch(true)) 作用不到 dbt.mustQueryRow
		log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				dbt.db.SetOption(options.WithSwitch(true))
				wg.Done()
			}()
			dbt.mustQueryRow("select * from testdriver where testdriver.value in (select value from testdriver) and testdriver.value = ?;", i)
			// 这条语句的执行，时序上和Process是并行的，期望这句能够和process中的开关处理碰撞，是的某些select处理可以跳过
			wg.Add(1)
			go func() {
				dbt.db.SetOption(options.WithSwitch(false))
				wg.Done()
			}()
		}
		dbt.db.Flush()
		wg.Wait()

		// create+select version+10*insert+1000*select
		if notifierUnitTest.ErrsCount() >= 22 {
			dbt.Errorf("switch off not work properly in process routine of mskeeper, %v", notifierUnitTest.ErrsCount())
		}

		log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
		log.Printf("notifierUnitTest.GetErrs() = %v", notifierUnitTest.GetErrs())
		log.Printf("notifierUnitTest.GetSQLs() = %v", notifierUnitTest.GetSQLs())
		// ---------------------------
		// dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		// dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		// dbt.db.Flush()
		// for i := 0; i < 10; i++ {
		// 	dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		// }
		// dbt.db.Flush()

		// // 10 << capacity 1024，所以不存在dbt.db.SetOption(WithSwitch(true)) 作用不到 dbt.mustQueryRow
		// log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
		// for i := 0; i < 10; i++ {
		// 	dbt.db.SetOption(options.WithSwitch(true))
		// 	dbt.mustQueryRow("select * from testdriver where testdriver.value in (select value from testdriver) and testdriver.value = ?;", i)
		// 	// 这条语句的执行，时序上和Process是并行的，期望这句能够和process中的开关处理碰撞，是的某些select处理可以跳过
		// 	dbt.db.SetOption(options.WithSwitch(false))
		// }

		// dbt.db.Flush()
		// if notifierUnitTest.ErrsCount() >= 20 {
		// 	dbt.Errorf("switch off not work properly in process routine of mskeeper")
		// }

		// log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
		// log.Printf("notifierUnitTest.GetErrs() = %v", notifierUnitTest.GetErrs())

	})
}

func TestPolicyOptionsMaxSilentPeriod(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 1001; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		// 设置30s 安静期，
		dbt.db.Flush()
		dbt.db.SetOption(options.WithMaxSilentPeriod(30 * time.Second))
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}
		dbt.db.Flush()

		notifierUnitTest.ClearErr()
		log.Printf("看不清。。。。。。。。。分割一下")

		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("in slient period, should not notify ErrPolicyCodeRowsAbs")
		}

		// 下一个新的Period开始，所有错误正常告警。
		time.Sleep(31 * time.Second)
		notifierUnitTest.ClearErr()
		dbt.mustQueryRow("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
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
// 	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
// 		notifierUnitTest.ClearErr()

// 		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

// 		nsw := NewSimpleWriterUT()
// 		dbt.db.SetOption(options.WithLogOutput(nsw))
// 		time.Sleep(5 * time.Second)
// 		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
// 		dbt.db.Flush()
// 		for i := 0; i < 10; i++ {
// 			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
// 		}
// 		dbt.db.Flush()

// 		// 当前输出的日志条数
// 		curCount := nsw.Count()
// 		if curCount <= 0 {
// 			dbt.Fatalf("curCount %v <= 0", curCount)
// 		}
// 		outoput := options.FetchLogOutput(dbt.db.opts)
// 		outoput.Write([]byte("append one more logs"))
// 		newCount := nsw.Count()
// 		if nsw.Count() != curCount+1 {
// 			dbt.Errorf("nsw.Count() %v != curCount+1 %v", nsw.Count(), curCount+1)
// 		}

// 		// 置空output，再来一次
// 		dbt.db.SetOption(options.WithLogOutput(ioutil.Discard))
// 		for i := 0; i < 10; i++ {
// 			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
// 		}
// 		dbt.db.Flush()

// 		if nsw.Count() != newCount {
// 			dbt.Errorf("nsw.Count() %v != newCount %v", nsw.Count(), newCount)
// 		}

// 		log.Printf("notifierUnitTest.Count() = %v", nsw.Count())
// 		log.Printf("notifierUnitTest.GetLogs() = %v", nsw.GetLogs())
// 	})
// }

func TestPolicyOptionsLogLevelTests(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		time.Sleep(3 * time.Second) // skip keep alive
		notifierUnitTest.ClearErr()

		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))

		nsw := NewSimpleWriterUT()
		dbt.db.SetOption(options.WithLogOutput(nsw))
		dbt.db.SetOption(options.WithLogLevel(notifier.WarnLevel))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		dbt.db.Flush()
		for i := 0; i < 10; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		// 当前输出的日志条数，正常没有报错的情况下，输出应该是空
		curCount := nsw.Count()
		if curCount > 0 {
			dbt.Errorf("curCount %v > 0 %v", curCount, nsw.GetLogs())
		}
		logLevel := options.FetchLogLevel(dbt.db.GetOptions())
		if logLevel != notifier.WarnLevel {
			dbt.Errorf("logLevel %v != notifier.WarnLevel %v", logLevel, notifier.WarnLevel)
		}

		dbt.db.SetOption(options.WithLogLevel(notifier.InfoLevel))
		for i := 0; i < 10; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		// 每条SQL分析，有两条Info日志
		if nsw.Count() != 30 {
			dbt.Errorf("nsw.Count() %v != 30", nsw.Count())
		}

		// 再次设置Warn
		dbt.db.SetOption(options.WithLogLevel(notifier.WarnLevel))
		// dbt.db.SetOption(WithLogOutput(os.Stdout))
		for i := 0; i < 10; i++ {
			// wrong sql, should output warning
			dbt.db.QueryRow("select xxxvalue from testdriver where 13123123=testdriver.value and value = ?", i)
		}
		dbt.db.Flush()

		if nsw.Count() != 30+10 {
			dbt.Errorf("nsw.Count() %v != 30+10+1, %v", nsw.Count(), nsw.GetLogs())
		}

		log.Printf("nsw.GetLogs() = %v", nsw.GetLogs())
	})
}

func TestPolicyOptionsSQLWhiteList(t *testing.T) {
	runDefaultPolicyWithOptionTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()

		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		dbt.db.Flush()
		for i := 0; i < 1010; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		// 加入了白名单，应该被忽略
		dbt.db.SetOption(options.WithSQLWhiteLists("select * from testdriver where testdriver.value in (select value from testdriver) and testdriver.value = ?;"))
		dbt.mustQueryRow("select * from testdriver where testdriver.value in (select value from testdriver) and testdriver.value = ?;", 1)

		dbt.db.Flush()
		if !notifierUnitTest.WithNoErrs() {
			dbt.Errorf("sql whitelist not work properly in process routine of mskeeper")
		}
		log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())

		// 没有加入白名单，应该被检测出来
		dbt.mustQueryRow("select value from testdriver where testdriver.value in (select value from testdriver) and testdriver.value = ?;", 1)
		dbt.db.Flush()
		if notifierUnitTest.WithNoErrs() {
			dbt.Errorf("sql whitelist not work properly in process routine of mskeeper")
		}

		log.Printf("notifierUnitTest.ErrsCount() = %v", notifierUnitTest.ErrsCount())
	})
}
