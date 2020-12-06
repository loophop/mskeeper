package addon

import (
	"context"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestPolicyExcutionTransNormal(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE testaddon (value int, value1 int)")
		for i := 0; i < 10000; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}
		time.Sleep(5 * time.Second)
		dbt.db.Flush()

		dbt.db.SetOption(options.WithMaxExecTime(30 * time.Millisecond))

		tx, err := dbt.db.Begin()
		if err != nil {
			dbt.Fatalf("error on Begin %s", err.Error())
		}

		_, err = tx.Exec("insert into testaddon select * from testaddon where testaddon.value in (select value from testaddon);")
		if err != nil {
			dbt.Fatalf("error on Exec %s", err.Error())
		}
		err = tx.Commit()
		if err != nil {
			dbt.Fatalf("error on Commit %s", err.Error())
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}

		dbt.db.SetOption(options.WithMaxExecTime(5 * time.Millisecond))
		rows := dbt.mustQuery("select * from testaddon where testaddon.value in (select value from testaddon);")
		dbt.db.Flush()
		defer rows.Close()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}
		dbt.db.Flush()
	})
}

func TestPolicyExcutionTransRollback(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE testaddon (value int, value1 int)")
		for i := 0; i < 10000; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		dbt.db.SetOption(options.WithMaxExecTime(30 * time.Millisecond))

		tx, err := dbt.db.Begin()
		if err != nil {
			dbt.Fatalf("error on Begin %s", err.Error())
		}

		_, err = tx.Exec("insert into testaddon value(5026111, 5026111)")
		if err != nil {
			dbt.Fatalf("error on Exec %s", err.Error())
		}

		err = tx.Rollback()
		if err != nil {
			dbt.Fatalf("error on Commit %s", err.Error())
		}

		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost banned")
		}

		row := dbt.mustQueryRow("select value from testaddon where value=5026111111 and value1=5026111111")
		dbt.db.Flush()

		var v1, v2 int
		if err := row.Scan(&v1, &v2); err == nil {
			dbt.Fatalf("trans rollback failed %v value got", err)
		}

		dbt.db.Flush()
	})
}

// 各种SQL语句都过一下，看是否有漏掉，没有进mskeeper的process的
func TestTransMSKTouchAllTypeOfDMLs(t *testing.T) {
	runDefaultPolicyTests(t, dsn, func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		ctx := context.Background()

		dbt.mustExec("CREATE TABLE `testaddon` (`value` varchar(2) DEFAULT NULL,`value2` varchar(26) DEFAULT NULL,`value1` int(11) unsigned DEFAULT NULL,`value3` mediumtext,`value4` blob, KEY `value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2001; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?, ?, ?, ?)", "1", "1", "1", "1", "1")
		}
		// Case 1
		tx, err := dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}

		_, err = tx.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "123")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			tx.Rollback()
			dbt.Fatalf("code truncate should be NG")
		}
		notifierUnitTest.ClearErr()

		_, err = tx.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "223")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			tx.Rollback()
			dbt.Fatalf("code truncate should be NG")
		}
		tx.Commit()
		notifierUnitTest.ClearErr()

		// Case 2
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		row := tx.QueryRow("SELECT * from testaddon where value = ?", "1")
		if row == nil {
			dbt.Fatal("QueryRow failed")
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			tx.Rollback()
			dbt.Fatalf("should be failed")
		}
		tx.Commit()
		notifierUnitTest.ClearErr()

		// Case 3
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		rows, err := tx.Query("SELECT * from testaddon where value = ?", "4")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}
		defer rows.Close()

		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			tx.Rollback()
			dbt.Fatalf("should be failed")
		}
		tx.Commit()
		notifierUnitTest.ClearErr()

		// Case 4
		tx, err = dbt.db.Begin()
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
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		rows, err = tx.Query("SELECT * from testaddon where value = ?", "3")
		if err != nil {
			tx.Rollback()
			dbt.Fatal("Query failed")
		}
		defer rows.Close()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			tx.Rollback()
			dbt.Fatalf("should be failed")
		}
		tx.Commit()
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
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "444")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}
		tx.Commit()
		dbt.db.Flush()
		log.Printf("notifierUnitTest.GetErr() = %v", notifierUnitTest.GetErrs())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 8
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		rows, err = tx.QueryContext(ctx, "SELECT * from testaddon where value = ?", "5")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}
		defer rows.Close()
		tx.Commit()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 9
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		row = tx.QueryRowContext(context.Background(), "SELECT * from testaddon where value = ?", "6")
		if row == nil {
			tx.Rollback()
			dbt.Fatalf("should be ok")
		}
		dbt.db.Flush()
		tx.Commit()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 10
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		stmt, err := tx.PrepareContext(ctx, "SELECT value from testaddon where value = ?")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}

		row = stmt.QueryRow("1")
		if row == nil {
			tx.Rollback()
			dbt.Fatalf("stmt.QueryRow failed")
		}

		var val string
		if err := row.Scan(&val); val != "1" {
			dbt.Fatal(err)
		}
		tx.Commit()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Fatalf("should be failed")
		}

		// Case 11
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}

		stmt, err = tx.Prepare("SELECT value from testaddon where value = ?")
		if err != nil {
			tx.Rollback()
			dbt.Fatal(err)
		}

		row = stmt.QueryRow("1")
		if row == nil {
			tx.Rollback()
			dbt.Fatalf("stmt.QueryRow failed")
		}

		if err := row.Scan(&val); val != "1" {
			tx.Rollback()
			dbt.Fatal(err)
		}
		tx.Commit()
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Fatalf("should be failed")
		}

		// Case 12
		tx, err = dbt.db.Begin()
		if err != nil {
			dbt.Fatal(err)
		}
		row = tx.QueryRowContext(context.Background(), "SELECT * from testaddon where value = ?", "88")
		if row == nil {
			tx.Rollback()
			dbt.Fatalf("should be ok")
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		tx.Commit()
		notifierUnitTest.ClearErr()

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}
