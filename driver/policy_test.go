package driver

import (
	// "bytes"
	//"crypto/tls"
	// "database/sql"
	"fmt"
	"log"

	// "database/sql/driver"
	// "encoding/json"
	// "errors"
	// "fmt"
	// "github.com/go-sql-driver/mysql"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	// "gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"

	// "io"
	// "io/ioutil"
	// "log"
	// "net"
	// "os"
	// "reflect"
	// "strings"
	// "sync"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestPolicyExcutionWithNoErrs(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")

		rows := dbt.mustQuery("select * from testdriver where testdriver.value in (select value from testdriver);")
		defer rows.Close()
		dbt.db.Flush()

		if !notifierUnitTest.WithNoErrs() {
			dbt.Errorf("should have no errors")
		}
	})
}

func TestPolicyExcutionTime1(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 10000; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		dbt.db.SetOption(options.WithMaxExecTime(30 * time.Millisecond))
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}

		dbt.db.SetOption(options.WithMaxExecTime(5 * time.Millisecond))
		rows := dbt.mustQuery("select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		defer rows.Close()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}
		dbt.db.Flush()
	})
}

func TestPolicyExcutionTime2(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		dbt.db.SetOption(options.WithMaxExecTime(10 * time.Second))
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value1 in (select value1 from testdriver);")
		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost banned")
		}

		dbt.db.SetOption(options.WithMaxExecTime(1 * time.Millisecond))
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}
	})
}

func TestPolicyRowsInvolved(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		logmsk.MSKLog().SetOutput(os.Stdout)
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))

		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11) DEFAULT NULL, `value1` int(11) DEFAULT NULL, KEY `value1` (`value1`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 3004; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		// hit the index, its ok
		dbt.mustQueryRow("select * from testdriver where value1 < 20 limit 1")
		dbt.db.Flush()
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("rows involved 1/3 illegally trigged")
		}

		// hit the index, however the rows larger than 1/3 all rows
		dbt.mustQueryRow("select * from testdriver where value1 < 1005")
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("rows involved 1/3 not 35 trigged")
		}

		// no index, all table scan
		dbt.mustQueryRow("select * from testdriver where value > 20")
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsInvolve) {
			dbt.Errorf("rows involved 1/3 not trigged")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestPolicyRowsAbsolute(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		log.Printf("dbt.db.GetErr() %v", dbt.db.GetErr())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}
		dbt.db.ClearErr()

		if dbt.db.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("cached errors not cleared")
		}

		dbt.mustQueryRow("select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		log.Printf("dbt.db.GetErr() %v", dbt.db.GetErr())
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}
		if !dbt.db.HasErr(policy.ErrPolicyCodeRowsAbs) {
			dbt.Errorf("rows absolutely not covered")
		}

		dbt.db.ClearErr()
	})
}

func TestPolicySwitch(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE testdriver (value int, value1 int)")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		dbt.db.SetOption(options.WithSwitch(false))
		dbt.db.SetOption(options.WithMaxExecTime(10 * time.Millisecond))
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		// should not detect error
		if notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost banned illegally")
		}

		// open again
		dbt.db.SetOption(options.WithSwitch(true))
		dbt.db.SetOption(options.WithMaxExecTime(1 * time.Millisecond))
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()
		if !notifierUnitTest.HasErr(policy.ErrPolicyCodeExeCost) {
			dbt.Errorf("time cost not banned")
		}
	})
}

func TestPolicyFieldsTypeSimple(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1010; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		var godamnv int = 5

		rows := dbt.mustQuery("select * from testdriver where value1 = ?", godamnv)
		defer rows.Close()
		dbt.db.Flush()

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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		rows := dbt.mustQuery("select * from testdriver where testdriver.value1 = '111' union select * from testdriver where testdriver.value1 = '222';")
		defer rows.Close()
		dbt.db.Flush()

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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 10002; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		time.Sleep(5 * time.Second)
		rows := dbt.mustQuery("select * from testdriver where testdriver.value1 = 111 union select * from testdriver where testdriver.value1 = '222';")
		defer rows.Close()
		dbt.db.Flush()

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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1010; i++ { //
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		rows := dbt.mustQuery("select * from testdriver where testdriver.value1 = 111 union select * from testdriver where testdriver.value1 = '222';")
		defer rows.Close()
		dbt.db.Flush()

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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 99; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		rows := dbt.mustQuery("select * from testdriver")
		defer rows.Close()
		dbt.db.Flush()

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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		dbt.db.Flush()

		rows := dbt.mustQuery("select * from testdriver")
		defer rows.Close()
		dbt.db.Flush()

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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, fmt.Sprintf("%v", i))
		}
		dbt.db.Flush()

		rows := dbt.mustQuery("select * from testdriver")
		defer rows.Close()
		dbt.db.Flush()

		tx, err := dbt.db.Begin()
		if err != nil {
			t.Errorf("TestPolicyFieldsLengthStringOK1: start transaction failed")
		}

		if _, err := tx.Exec("INSERT INTO testdriver VALUES(?, ?)", 11111, "222222"); err != nil {
			tx.Rollback()
			t.Errorf("TestPolicyFieldsLengthStringOK1: transaction exec insert failed")
		}
		if _, err := tx.Exec("UPDATE testdriver SET value1 = ? WHERE value = ? ", "222222", 22222); err != nil {
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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value2` varchar(5), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?, ?)", i, "1234", i)
		}
		dbt.db.Flush()

		rows := dbt.mustQuery("select * from testdriver")
		defer rows.Close()
		dbt.db.Flush()

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
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		notifierUnitTest.ClearErr()
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
		dbt.db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

		dbt.db.SetOption(options.WithMaxExecTime(3 * time.Second))
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value2` varchar(5), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { // 超过1000行
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?, ?)", i, "123456", i+1)
		}
		dbt.db.Flush()

		time.Sleep(5 * time.Second)
		rows := dbt.mustQuery("select * from testdriver")
		defer rows.Close()
		dbt.db.Flush()
		dbt.db.Flush()

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

		log.Printf("dbt.db.GetErr() = %v len(dbt.db.GetErr() = %v)", notifierUnitTest.GetErrs(), len(dbt.db.GetErr()))
	})
}
