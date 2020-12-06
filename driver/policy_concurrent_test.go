package driver

import (
	// "bytes"
	//"crypto/tls"
	"database/sql"
	"log"
	// "database/sql/driver"
	// "encoding/json"
	// "errors"
	"fmt"
	// "github.com/go-sql-driver/mysql"
	// logmsk "gitlab.papegames.com/fringe/mskeeper/log"
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

	"sync"
	"testing"
	"time"
)

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

		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}
		var wg sync.WaitGroup
		for i := 0; i < 100000; i++ {
			wg.Add(1)
			go func(idx int) {
				// dbt.db.Exec("select 1 from testdriver where value = '1'")
				dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", idx, idx)
				wg.Done()
			}(i)
		}

		time.Sleep(10 * time.Millisecond)
		// trigger 5 sec timeout of Flush
		start := time.Now()
		err := dbt.db.Flush()
		secs := time.Since(start).Seconds()

		if err == nil && (int(secs) < MaxTimeoutSecondsForFlush || int(secs) > MaxTimeoutSecondsForFlush+1) {
			dbt.Errorf("flush not timed out with MaxTimeoutSecondsForFlush %v actual :%0.1vs ",
				MaxTimeoutSecondsForFlush, secs)
		}
		log.Printf("flushed took %vs", secs)
		wg.Wait()
	})
}

func TestPolicyafterFlushTimeoutFail(t *testing.T) {
	runDefaultPolicyWithOneCapacityTests(t, dsn, func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		notifierUnitTestRaw.SetNotifyDelay(10 * time.Second)
		var wg sync.WaitGroup

		// 比capacity 2 大一点，使得ch后来的都忽略
		for i := 0; i < 3; i++ {
			wg.Add(1)
			log.Print("Add")
			go func(idx int) {
				// dbt.db.Exec("select 1 from testdriver where value = '1'")
				dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", idx, idx)
				wg.Done()
				log.Print("Done")
			}(i)
		}
		wg.Wait()

		rows := dbt.mustQuery("SELECT value from testdriver ")
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
				// dbt.db.Exec("select 1 from testdriver where value = '1'")
				dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", idx, idx)
				wg.Done()
			}(i)
		}
		wg.Wait()

		row := dbt.mustQueryRow("SELECT count(1) from testdriver ")

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

		// 防止 notify 的 error由于重复被过滤，导致超时设定不准
		dbt.db.SetOption(options.WithMaxSilentPeriod(1 * time.Millisecond))

		notifierUnitTestRaw.SetNotifyDelay(0)
		dbt.mustExec("CREATE TABLE `testdriver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		dbt.db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(100))
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO testdriver VALUES (?, ?)", i, i)
		}

		dbt.db.Flush()

		var wg sync.WaitGroup

		notifierUnitTestRaw.SetNotifyDelay(10 * time.Second)
		// 比capacity 2 大一点，使得ch排队，并由于Delay在11s，排队的ch写入会超时。
		for i := 0; i < 3; i++ {
			wg.Add(1)
			log.Print("Add")
			go func(idx int) {
				// dbt.db.Exec("select 1 from testdriver where value = '1'")
				dbt.mustExec(fmt.Sprintf("INSERT INTO testdriver VALUES (%v, %v)", idx, idx))
				wg.Done()
				log.Print("Done")
			}(i)
		}
		wg.Wait()

		// 5 < 10s，此时队列满，但是结合Flush的5s，能够处理结束。
		time.Sleep(5 * time.Second)

		// 以下insert语句刚刚好被Skip。
		notifierUnitTestRaw.SetNotifyDelay(0)
		dbt.mustExec("insert into testdriver select * from testdriver where testdriver.value in (select value from testdriver);")
		dbt.db.Flush()

		// 行数检查由于channel满，被忽略了，所以此处检查不应报错
		if notifierUnitTestRaw.WithNoErrs() == false {
			dbt.Errorf("rows absolutely catched unexpectly")
		}

	})
}
