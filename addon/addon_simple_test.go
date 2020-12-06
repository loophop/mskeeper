package addon

import (
	"context"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/mysql"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func TestClearPolicies(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testaddon (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}

		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely not covered")
		}

		dbt.mustExec("update testaddon set value = ?", 256)
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("data truncation not covered")
		}

		// Remove all policies
		dbt.db.ClearPolicies()

		// Try again
		notifierUnitTest.ClearErr()

		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon where value=1);")
		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely should not be covered")
		}

		dbt.mustExec("update testaddon set value = ?", 999)
		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("data truncation should not be covered")
		}

	})
}

func TestRawDB(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testaddon (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}

		err := dbt.db.RawDB().Ping()
		if err != nil {
			dbt.Fatalf("not the raw db?")
		}
		_, err = dbt.db.RawDB().Exec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon);")
		if err != nil {
			dbt.Fatalf("not the raw db?")
		}
		dbt.db.Flush()
		dbt.db.Flush()

		// 用rawdb 跳过了mskeeper的检查
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("rows absolutely not covered")
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

		dbt.mustExec("CREATE TABLE testaddon (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}

		oldopts := dbt.db.GetOptions().Clone()

		dbt.db.SetOptions(
			options.WithMaxSilentPeriod(1*time.Second),  // 1s之后提交的sql依然会检查，1s之内的忽略
			options.WithMaxExecTime(1*time.Millisecond), // 最大执行时间为1ms
			options.WithSQLWhiteLists( // 白名单
				"insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon);"),
		)

		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon where value != 1);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("max exec time should be covered")
		}

		notifierUnitTest.ClearErr()

		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon where value != 1);")
		dbt.db.Flush()
		dbt.db.Flush()
		// max slient perod 范围内 不报错
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("max exec time should not be covered")
		}

		notifierUnitTest.ClearErr()
		time.Sleep(2 * time.Second)

		// 过了 max slient period 继续报错
		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon where value != 1);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Fatalf("max exec time should be covered")
		}

		// 白名单
		notifierUnitTest.ClearErr()
		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon);")
		if !notifierUnitTest.WithNoErrs() {
			dbt.Fatalf("should be no errors")
		}

		/////// 恢复 设置
		notifierUnitTest.ClearErr()
		dbt.db.ResetOptions(oldopts)
		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon where value != 2);")
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
		dbt.mustExec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon where value != 2);")
		dbt.db.Flush()
		dbt.db.Flush()
		if !notifierUnitTest.WithNoErrs() {
			dbt.Fatalf("should be no errors")
		}

	})
}

func TestResyncInfoQueue(t *testing.T) {

	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testaddon (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}

		dbt.db.SetOption(options.WithCapacity(2))

		notifierUnitTest.ClearErr()
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				log.Printf("11111111111111111111111111111111111111111111111111111111 %v", idx)
				// dbt.db.Exec("select 1 from testaddon where value = '1'")
				dbt.mustExec("insert into testaddon values(?,?) ", 255+idx, 255+idx)
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
				// dbt.db.Exec("select 1 from testaddon where value = '1'")
				dbt.mustExec("insert into testaddon values(?,?) ", 265+idx, 265+idx)
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

// 各种SQL语句都过一下，看是否有漏掉，没有进mskeeper的process的
func TestPolicyTouchAllTypeOfDMLs(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		dbt.mustExec("CREATE TABLE `testaddon` (`value` varchar(2) DEFAULT NULL,`value2` varchar(26) DEFAULT NULL,`value1` int(11) unsigned DEFAULT NULL,`value3` mediumtext,`value4` blob, KEY `value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2001; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?, ?, ?, ?)", "1", "1", "1", "1", "1")
		}
		// Case 1
		_, err := dbt.db.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "123")
		if err != nil {
			dbt.Fatal(err)
		}
		dbt.db.Flush()

		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("code truncate should be NG")
		}
		notifierUnitTest.ClearErr()

		_, err = dbt.db.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "223")
		if err != nil {
			dbt.Fatal(err)
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("code truncate should be NG")
		}
		notifierUnitTest.ClearErr()

		// Case 2
		row := dbt.db.QueryRow("SELECT * from testaddon where value = ?", "1")
		if row == nil {
			dbt.Fatal("QueryRow failed")
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}

		notifierUnitTest.ClearErr()

		// Case 3
		rows, err := dbt.db.Query("SELECT * from testaddon where value = ?", "4")
		if err != nil {
			dbt.Fatal(err)
		}
		defer rows.Close()

		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}

		notifierUnitTest.ClearErr()

		// Case 4
		tx, err := dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		row = tx.QueryRow("SELECT * from testaddon where value = ?", "2")
		if row == nil {
			tx.Rollback()
			dbt.Fatal("QueryRow failed")
		}

		tx.Commit()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 5
		rows, err = dbt.db.Query("SELECT * from testaddon where value = ?", "3")
		if err != nil {
			dbt.Fatal("Query failed")
		}
		defer rows.Close()
		dbt.db.Flush()
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
		_, err = tx.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "333")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}
		tx.Commit()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 7
		ctx := context.Background()
		con, err := dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatal(err)
		}
		_, err = con.ExecContext(ctx, "INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "444")
		if err != nil {
			dbt.Fatal(err)
		}
		dbt.db.Flush()
		dbt.db.Flush()
		dbt.db.Flush()
		log.Printf("notifierUnitTest.GetErr() = %v", notifierUnitTest.GetErrs())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 8
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		rows, err = con.QueryContext(context.Background(), "SELECT * from testaddon where value = ?", "5")
		if err != nil {
			dbt.Fatal(err)
		}
		defer rows.Close()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 9
		con, err = dbt.db.Conn(context.Background())
		if err != nil {
			dbt.Fatal(err)
		}
		row = con.QueryRowContext(context.Background(), "SELECT * from testaddon where value = ?", "6")
		if row == nil {
			dbt.Fatalf("should be ok")
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 10
		stmt, err := dbt.db.PrepareContext(ctx, "SELECT value from testaddon where value = ?")
		if err != nil {
			dbt.Fatal(err)
		}

		row = stmt.QueryRow("1")
		if row == nil {
			dbt.Fatalf("stmt.QueryRow failed")
		}

		var val string
		if err := row.Scan(&val); val != "1" {
			dbt.Fatal(err)
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Fatalf("should be failed")
		}

		// Case 11
		stmt, err = dbt.db.Prepare("SELECT value from testaddon where value = ?")
		if err != nil {
			dbt.Fatal(err)
		}

		row = stmt.QueryRow("1")
		if row == nil {
			dbt.Fatalf("stmt.QueryRow failed")
		}

		if err := row.Scan(&val); val != "1" {
			dbt.Fatal(err)
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 12
		row = dbt.db.QueryRowContext(context.Background(), "SELECT * from testaddon where value = ?", "88")
		if row == nil {
			dbt.Fatalf("should be ok")
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestSynProcess(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.mustExec("CREATE TABLE testaddon (value tinyint, value1 tinyint)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}

		var rawerrors []error
		nargs, _ := mysql.Converter{}.ConvertValues([]interface{}{2})
		err := dbt.db.SyncProcess(time.Now(),
			"insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon where value != ?);",
			nargs,
			&rawerrors,
		)
		if err != nil {
			dbt.Fatalf("sync failed %v", err)
		}
		log.Printf("rawerrors %v", rawerrors)
		pe, _ := rawerrors[0].(*policy.PolicyError)
		if pe.Code != policy.ErrPolicyCodeRowsAbs {
			dbt.Fatalf("rows absolutely not covered")
		}

	})
}

func TestResyncKeepAlive(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		dbt.db.RawDB().SetMaxOpenConns(1)
		dbt.db.RawDB().SetMaxIdleConns(1)

		dbt.db.SetOption(options.WithKeepAlivePeriod(1 * time.Second))
		dbt.db.ResyncPingTimer()

		dbt.mustExec("CREATE TABLE testaddon (value tinyint, value1 tinyint)")
		for i := 0; i < 1; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
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
