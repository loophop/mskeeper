package addon

import (
	"context"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

// 各种SQL语句都过一下，看是否有漏掉，没有进mskeeper的process的
func TestConnMSKTouchAllTypeOfDMLs(t *testing.T) {
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
		// // Case 1
		// _, err := dbt.db.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "123")
		// if err != nil {
		// 	dbt.Fatal(err)
		// }
		// dbt.db.Flush()

		// if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
		// 	dbt.Fatalf("code truncate should be NG")
		// }
		// notifierUnitTest.ClearErr()

		// _, err = dbt.db.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "223")
		// if err != nil {
		// 	dbt.Fatal(err)
		// }
		// dbt.db.Flush()
		// if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
		// 	dbt.Fatalf("code truncate should be NG")
		// }
		// notifierUnitTest.ClearErr()

		// // Case 2
		// row := dbt.db.QueryRow("SELECT * from testaddon where value = ?", "1")
		// if row == nil {
		// 	dbt.Fatal("QueryRow failed")
		// }
		// dbt.db.Flush()
		// if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
		// 	dbt.Fatalf("should be failed")
		// }

		// notifierUnitTest.ClearErr()

		// // Case 3
		// rows, err := dbt.db.Query("SELECT * from testaddon where value = ?", "4")
		// if err != nil {
		// 	dbt.Fatal(err)
		// }
		// defer rows.Close()

		// dbt.db.Flush()
		// if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
		// 	dbt.Fatalf("should be failed")
		// }

		// notifierUnitTest.ClearErr()

		// // Case 4
		// tx, err := dbt.db.Begin()
		// if err != nil {
		// 	dbt.Fatal(err)
		// }
		// row = tx.QueryRow("SELECT * from test where value = ?", "2")
		// if row == nil {
		// 	tx.Rollback()
		// 	dbt.Fatal("QueryRow failed")
		// }

		// tx.Commit()
		// dbt.db.Flush()
		// if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
		// 	dbt.Fatalf("should be failed")
		// }
		// notifierUnitTest.ClearErr()

		// // Case 5
		// rows, err = dbt.db.Query("SELECT * from testaddon where value = ?", "3")
		// if err != nil {
		// 	dbt.Fatal("Query failed")
		// }
		// defer rows.Close()
		// dbt.db.Flush()
		// if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
		// 	dbt.Fatalf("should be failed")
		// }
		// notifierUnitTest.ClearErr()

		// // Case 6
		// log.Printf("------------------------------------------------------------------------------------------")
		// tx, err = dbt.db.Begin()
		// if err != nil {
		// 	dbt.Fatal(err)
		// }
		// _, err = tx.Exec("INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "333")
		// if err != nil {
		// 	tx.Rollback()
		// 	dbt.Fatal(err)
		// }
		// tx.Commit()
		// dbt.db.Flush()
		// if !notifierUnitTest.HasErr(policy.ErrPolicyCodeDataTruncate) {
		// 	dbt.Fatalf("should be failed")
		// }
		// notifierUnitTest.ClearErr()
		ctx := context.Background()
		// Case 1
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

		// Case 2
		con, err = dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatal(err)
		}
		row := con.QueryRowContext(ctx, "SELECT * from testaddon where value = ?", "6")
		if row == nil {
			dbt.Fatalf("should be ok")
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Fatalf("should be failed")
		}
		notifierUnitTest.ClearErr()

		// Case 3
		con, err = dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatal(err)
		}

		stmt, err := con.PrepareContext(ctx, "SELECT value from testaddon where value = ?")
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

		// Case 4
		rows, err := con.QueryContext(ctx, "SELECT value from testaddon where value = ? limit 10", "1")
		if err != nil {
			dbt.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var val string
			if err := rows.Scan(&val); val != "1" {
				dbt.Fatal(err)
			}
		}
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeAllTableScan) {
			dbt.Fatalf("should be failed")
		}

		// Case 5
		err = con.PingContext(ctx)
		if err != nil {
			dbt.Fatal(err)
		}

		err = con.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		_, err = con.ExecContext(ctx, "INSERT INTO testaddon(value, value1) VALUES (?, 123123)", "111")
		if err == nil {
			dbt.Fatalf("con should be closed")
		}
		dbt.db.Flush()

		// Case 6
		con, err = dbt.db.Conn(ctx)
		if err != nil {
			dbt.Fatal(err)
		}

		tx, err := con.BeginTx(ctx, nil)
		log.Printf("1111111111111111111tx %v", tx)
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

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}
