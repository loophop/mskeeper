// Go MSKeeper Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package addon

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
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
	dbname = env("MYSQL_TEST_DBNAME", "mskeepertestaddon")
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
	db := NewMSKeeperAddon(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifierUnitTest),
		options.WithLogOutput(os.Stdout),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS testaddon")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS testaddon")
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

	db := NewMSKeeperAddon(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifier.NewNotifierMux(notifierUnitTest,
			notifier.NewDefaultNotifier())),
		options.WithCapacity(1024),
		options.WithLogOutput(os.Stdout),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	defer db.Close()

	capa := options.FetchCapacity(db.GetOptions())

	if capa != 1024 {
		t.Fatalf("capa %v not set", capa)
	}

	db.Exec("DROP TABLE IF EXISTS testaddon")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS testaddon")
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
	db := NewMSKeeperAddon(
		rawDB,
		options.WithSwitch(true),
		options.WithNotifier(notifierUnitTestRaw),
		options.WithCapacity(2),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	defer db.Close()

	capa := options.FetchCapacity(db.GetOptions())

	if capa != 2 {
		t.Fatalf("capa %v not set", capa)
	}

	db.Exec("DROP TABLE IF EXISTS testaddon")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS testaddon")
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
	db := NewMSKeeperAddon(
		rawDB,
		options.WithSwitch(true),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	defer db.Close()
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db.Exec("DROP TABLE IF EXISTS testaddon")

	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS testaddon")
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
	db := NewMSKeeperAddon(
		rawDB,
		options.WithSwitch(true),

		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	defer db.Close()
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db.Exec("DROP TABLE IF EXISTS testaddon")

	dsn2 := dsn + "&interpolateParams=true"
	var rawDB2 *sql.DB
	rawDB2, err = sql.Open("mysql", dsn2)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	db2 := NewMSKeeperAddon(
		rawDB2,
		options.WithSwitch(true),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	defer db2.Close()
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

	db3 := NewMSKeeperAddon(
		rawDB3,
		options.WithSwitch(true),
		// WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)
	defer db3.Close()

	_ = db3.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db3.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db3.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db3.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	dbt := &DBTest{t, db}
	dbt2 := &DBTest{t, db2}
	dbt3 := &DBTest{t, db3}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS testaddon")
		if db2 != nil {
			test(dbt2)
			dbt2.db.Exec("DROP TABLE IF EXISTS testaddon")
		}
		if db3 != nil {
			test(dbt3)
			dbt3.db.Exec("DROP TABLE IF EXISTS testaddon")
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
	mySQLErr, ok := err.(*mysql.MySQLError)
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

func TestCRUD(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE testaddon (value BOOL)")

		// Test for unexpected data
		var out bool
		rows := dbt.mustQuery("SELECT * FROM testaddon")
		if rows.Next() {
			dbt.Error("unexpected data in empty table")
		}
		rows.Close()

		// Create Data
		res := dbt.mustExec("INSERT INTO testaddon VALUES (1)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		id, err := res.LastInsertId()
		if err != nil {
			dbt.Fatalf("res.LastInsertId() returned error: %s", err.Error())
		}
		if id != 0 {
			dbt.Fatalf("expected InsertId 0, got %d", id)
		}

		// Read
		rows = dbt.mustQuery("SELECT value FROM testaddon")
		if rows.Next() {
			rows.Scan(&out)
			if true != out {
				dbt.Errorf("true != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Update
		res = dbt.mustExec("UPDATE testaddon SET value = ? WHERE value = ?", false, true)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check Update
		rows = dbt.mustQuery("SELECT value FROM testaddon")
		if rows.Next() {
			rows.Scan(&out)
			if false != out {
				dbt.Errorf("false != %t", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

		// Delete
		res = dbt.mustExec("DELETE FROM testaddon WHERE value = ?", false)
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Check for unexpected rows
		res = dbt.mustExec("DELETE FROM testaddon")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 0 {
			dbt.Fatalf("expected 0 affected row, got %d", count)
		}
	})
}

func TestMultiQuery(t *testing.T) {
	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE `testaddon` (`id` int(11) NOT NULL, `value` int(11) NOT NULL) ")

		// Create Data
		res := dbt.mustExec("INSERT INTO testaddon VALUES (1, 1)")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Update
		res = dbt.mustExec("UPDATE testaddon SET value = 3 WHERE id = 1; UPDATE testaddon SET value = 4 WHERE id = 1; UPDATE testaddon SET value = 5 WHERE id = 1;")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 1 {
			dbt.Fatalf("expected 1 affected row, got %d", count)
		}

		// Read
		var out int
		rows := dbt.mustQuery("SELECT value FROM testaddon WHERE id=1;")
		if rows.Next() {
			rows.Scan(&out)
			if 5 != out {
				dbt.Errorf("5 != %d", out)
			}

			if rows.Next() {
				dbt.Error("unexpected data")
			}
		} else {
			dbt.Error("no data")
		}
		rows.Close()

	})
}

func TestInt(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [5]string{"TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"}
		in := int64(42)
		var out int64
		var rows *sql.Rows

		// SIGNED
		for _, v := range types {
			dbt.mustExec("CREATE TABLE testaddon (value " + v + ")")

			dbt.mustExec("INSERT INTO testaddon VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM testaddon")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %d != %d", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()

			dbt.mustExec("DROP TABLE IF EXISTS testaddon")
		}

		// UNSIGNED ZEROFILL
		for _, v := range types {
			dbt.mustExec("CREATE TABLE testaddon (value " + v + " ZEROFILL)")

			dbt.mustExec("INSERT INTO testaddon VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM testaddon")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s ZEROFILL: %d != %d", v, in, out)
				}
			} else {
				dbt.Errorf("%s ZEROFILL: no data", v)
			}
			rows.Close()

			dbt.mustExec("DROP TABLE IF EXISTS testaddon")
		}
	})
}

func TestFloat32(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		in := float32(42.23)
		var out float32
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE testaddon (value " + v + ")")
			dbt.mustExec("INSERT INTO testaddon VALUES (?)", in)
			rows = dbt.mustQuery("SELECT value FROM testaddon")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %g != %g", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()
			dbt.mustExec("DROP TABLE IF EXISTS testaddon")
		}
	})
}

func TestFloat64(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		var expected float64 = 42.23
		var out float64
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE testaddon (value " + v + ")")
			dbt.mustExec("INSERT INTO testaddon VALUES (42.23)")
			rows = dbt.mustQuery("SELECT value FROM testaddon")
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()
			dbt.mustExec("DROP TABLE IF EXISTS testaddon")
		}
	})
}

func TestFloat64Placeholder(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [2]string{"FLOAT", "DOUBLE"}
		var expected float64 = 42.23
		var out float64
		var rows *sql.Rows
		for _, v := range types {
			dbt.mustExec("CREATE TABLE testaddon (id int, value " + v + ")")
			dbt.mustExec("INSERT INTO testaddon VALUES (1, 42.23)")
			rows = dbt.mustQuery("SELECT value FROM testaddon WHERE id = ?", 1)
			if rows.Next() {
				rows.Scan(&out)
				if expected != out {
					dbt.Errorf("%s: %g != %g", v, expected, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()
			dbt.mustExec("DROP TABLE IF EXISTS testaddon")
		}
	})
}

func TestString(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		types := [6]string{"CHAR(255)", "VARCHAR(255)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"}
		in := "κόσμε üöäßñóùéàâÿœ'îë Árvíztűrő いろはにほへとちりぬるを イロハニホヘト דג סקרן чащах  น่าฟังเอย"
		var out string
		var rows *sql.Rows

		for _, v := range types {
			dbt.mustExec("CREATE TABLE testaddon (value " + v + ") CHARACTER SET utf8")

			dbt.mustExec("INSERT INTO testaddon VALUES (?)", in)

			rows = dbt.mustQuery("SELECT value FROM testaddon")
			if rows.Next() {
				rows.Scan(&out)
				if in != out {
					dbt.Errorf("%s: %s != %s", v, in, out)
				}
			} else {
				dbt.Errorf("%s: no data", v)
			}
			rows.Close()

			dbt.mustExec("DROP TABLE IF EXISTS testaddon")
		}

		// BLOB
		dbt.mustExec("CREATE TABLE testaddon (id int, value BLOB) CHARACTER SET utf8")

		id := 2
		in = "Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet. " +
			"Lorem ipsum dolor sit amet, consetetur sadipscing elitr, " +
			"sed diam nonumy eirmod tempor invidunt ut labore et dolore magna aliquyam erat, " +
			"sed diam voluptua. At vero eos et accusam et justo duo dolores et ea rebum. " +
			"Stet clita kasd gubergren, no sea takimata sanctus est Lorem ipsum dolor sit amet."
		dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", id, in)

		err := dbt.db.QueryRow("SELECT value FROM testaddon WHERE id = ?", id).Scan(&out)
		if err != nil {
			dbt.Fatalf("Error on BLOB-Query: %s", err.Error())
		} else if out != in {
			dbt.Errorf("BLOB: %s != %s", in, out)
		}
	})
}

func TestRawBytes(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		v1 := []byte("aaa")
		v2 := []byte("bbb")
		rows := dbt.mustQuery("SELECT ?, ?", v1, v2)
		defer rows.Close()
		if rows.Next() {
			var o1, o2 sql.RawBytes
			if err := rows.Scan(&o1, &o2); err != nil {
				dbt.Errorf("Got error: %v", err)
			}
			if !bytes.Equal(v1, o1) {
				dbt.Errorf("expected %v, got %v", v1, o1)
			}
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
			// https://github.com/go-sql-driver/mysql/issues/765
			// Appending to RawBytes shouldn't overwrite next RawBytes.
			o1 = append(o1, "xyzzy"...)
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
		} else {
			dbt.Errorf("no data")
		}
	})
}

func TestRawMessage(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		v1 := json.RawMessage("{}")
		v2 := json.RawMessage("[]")
		rows := dbt.mustQuery("SELECT ?, ?", v1, v2)
		defer rows.Close()
		if rows.Next() {
			var o1, o2 json.RawMessage
			if err := rows.Scan(&o1, &o2); err != nil {
				dbt.Errorf("Got error: %v", err)
			}
			if !bytes.Equal(v1, o1) {
				dbt.Errorf("expected %v, got %v", v1, o1)
			}
			if !bytes.Equal(v2, o2) {
				dbt.Errorf("expected %v, got %v", v2, o2)
			}
		} else {
			dbt.Errorf("no data")
		}
	})
}

type testValuer struct {
	value string
}

func (tv testValuer) Value() (driver.Value, error) {
	return tv.value, nil
}

func TestValuer(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		in := testValuer{"a_value"}
		var out string
		var rows *sql.Rows

		dbt.mustExec("CREATE TABLE testaddon (value VARCHAR(255)) CHARACTER SET utf8")
		dbt.mustExec("INSERT INTO testaddon VALUES (?)", in)
		rows = dbt.mustQuery("SELECT value FROM testaddon")
		if rows.Next() {
			rows.Scan(&out)
			if in.value != out {
				dbt.Errorf("Valuer: %v != %s", in, out)
			}
		} else {
			dbt.Errorf("Valuer: no data")
		}
		rows.Close()

		dbt.mustExec("DROP TABLE IF EXISTS testaddon")
	})
}

type testValuerWithValidation struct {
	value string
}

func (tv testValuerWithValidation) Value() (driver.Value, error) {
	if len(tv.value) == 0 {
		return nil, fmt.Errorf("Invalid string valuer. Value must not be empty")
	}

	return tv.value, nil
}

func TestValuerWithValidation(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		in := testValuerWithValidation{"a_value"}
		var out string
		var rows *sql.Rows

		dbt.mustExec("CREATE TABLE testValuer (value VARCHAR(255)) CHARACTER SET utf8")
		dbt.mustExec("INSERT INTO testValuer VALUES (?)", in)

		rows = dbt.mustQuery("SELECT value FROM testValuer")
		defer rows.Close()

		if rows.Next() {
			rows.Scan(&out)
			if in.value != out {
				dbt.Errorf("Valuer: %v != %s", in, out)
			}
		} else {
			dbt.Errorf("Valuer: no data")
		}

		if _, err := dbt.db.Exec("INSERT INTO testValuer VALUES (?)", testValuerWithValidation{""}); err == nil {
			dbt.Errorf("Failed to check valuer error")
		}

		if _, err := dbt.db.Exec("INSERT INTO testValuer VALUES (?)", nil); err != nil {
			dbt.Errorf("Failed to check nil")
		}

		if _, err := dbt.db.Exec("INSERT INTO testValuer VALUES (?)", map[string]bool{}); err == nil {
			dbt.Errorf("Failed to check not valuer")
		}

		dbt.mustExec("DROP TABLE IF EXISTS testValuer")
	})
}

type timeTests struct {
	dbtype  string
	tlayout string
	tests   []timeTest
}

type timeTest struct {
	s string // leading "!": do not use t as value in queries
	t time.Time
}

type timeMode byte

func (t timeMode) String() string {
	switch t {
	case binaryString:
		return "binary:string"
	case binaryTime:
		return "binary:time.Time"
	case textString:
		return "text:string"
	}
	panic("unsupported timeMode")
}

func (t timeMode) Binary() bool {
	switch t {
	case binaryString, binaryTime:
		return true
	}
	return false
}

const (
	binaryString timeMode = iota
	binaryTime
	textString
)

func (t timeTest) genQuery(dbtype string, mode timeMode) string {
	var inner string
	if mode.Binary() {
		inner = "?"
	} else {
		inner = `"%s"`
	}
	return `SELECT cast(` + inner + ` as ` + dbtype + `)`
}

func (t timeTest) run(dbt *DBTest, dbtype, tlayout string, mode timeMode) {
	var rows *sql.Rows
	query := t.genQuery(dbtype, mode)
	switch mode {
	case binaryString:
		rows = dbt.mustQuery(query, t.s)
	case binaryTime:
		rows = dbt.mustQuery(query, t.t)
	case textString:
		query = fmt.Sprintf(query, t.s)
		rows = dbt.mustQuery(query)
	default:
		panic("unsupported mode")
	}
	defer rows.Close()
	var err error
	if !rows.Next() {
		err = rows.Err()
		if err == nil {
			err = fmt.Errorf("no data")
		}
		dbt.Errorf("%s [%s]: %s", dbtype, mode, err)
		return
	}
	var dst interface{}
	err = rows.Scan(&dst)
	if err != nil {
		dbt.Errorf("%s [%s]: %s", dbtype, mode, err)
		return
	}
	switch val := dst.(type) {
	case []uint8:
		str := string(val)
		if str == t.s {
			return
		}
		if mode.Binary() && dbtype == "DATETIME" && len(str) == 26 && str[:19] == t.s {
			// a fix mainly for TravisCI:
			// accept full microsecond resolution in result for DATETIME columns
			// where the binary protocol was used
			return
		}
		dbt.Errorf("%s [%s] to string: expected %q, got %q",
			dbtype, mode,
			t.s, str,
		)
	case time.Time:
		if val == t.t {
			return
		}
		dbt.Errorf("%s [%s] to string: expected %q, got %q",
			dbtype, mode,
			t.s, val.Format(tlayout),
		)
	default:
		fmt.Printf("%#v\n", []interface{}{dbtype, tlayout, mode, t.s, t.t})
		dbt.Errorf("%s [%s]: unhandled type %T (is '%v')",
			dbtype, mode,
			val, val,
		)
	}
}

func TestDateTime(t *testing.T) {
	afterTime := func(t time.Time, d string) time.Time {
		dur, err := time.ParseDuration(d)
		if err != nil {
			panic(err)
		}
		return t.Add(dur)
	}
	// NOTE: MySQL rounds DATETIME(x) up - but that's not included in the tests
	format := "2006-01-02 15:04:05.999999"
	t0 := time.Time{}
	tstr0 := "0000-00-00 00:00:00.000000"
	testcases := []timeTests{
		{"DATE", format[:10], []timeTest{
			{t: time.Date(2011, 11, 20, 0, 0, 0, 0, time.UTC)},
			{t: t0, s: tstr0[:10]},
		}},
		{"DATETIME", format[:19], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
			{t: t0, s: tstr0[:19]},
		}},
		{"DATETIME(0)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 0, time.UTC)},
			{t: t0, s: tstr0[:19]},
		}},
		{"DATETIME(1)", format[:21], []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 100000000, time.UTC)},
			{t: t0, s: tstr0[:21]},
		}},
		{"DATETIME(6)", format, []timeTest{
			{t: time.Date(2011, 11, 20, 21, 27, 37, 123456000, time.UTC)},
			{t: t0, s: tstr0},
		}},
		{"TIME", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{s: "!-12:34:56"},
			{s: "!-838:59:59"},
			{s: "!838:59:59"},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(0)", format[11:19], []timeTest{
			{t: afterTime(t0, "12345s")},
			{s: "!-12:34:56"},
			{s: "!-838:59:59"},
			{s: "!838:59:59"},
			{t: t0, s: tstr0[11:19]},
		}},
		{"TIME(1)", format[11:21], []timeTest{
			{t: afterTime(t0, "12345600ms")},
			{s: "!-12:34:56.7"},
			{s: "!-838:59:58.9"},
			{s: "!838:59:58.9"},
			{t: t0, s: tstr0[11:21]},
		}},
		{"TIME(6)", format[11:], []timeTest{
			{t: afterTime(t0, "1234567890123000ns")},
			{s: "!-12:34:56.789012"},
			{s: "!-838:59:58.999999"},
			{s: "!838:59:58.999999"},
			{t: t0, s: tstr0[11:]},
		}},
	}
	dsns := []string{
		dsn + "&parseTime=true",
		dsn + "&parseTime=false",
	}
	for _, testdsn := range dsns {
		runTests(t, testdsn, func(dbt *DBTest) {
			microsecsSupported := false
			zeroDateSupported := false
			var rows *sql.Rows
			var err error
			rows, err = dbt.db.Query(`SELECT cast("00:00:00.1" as TIME(1)) = "00:00:00.1"`)
			if err == nil {
				rows.Scan(&microsecsSupported)
				rows.Close()
			}
			rows, err = dbt.db.Query(`SELECT cast("0000-00-00" as DATE) = "0000-00-00"`)
			if err == nil {
				rows.Scan(&zeroDateSupported)
				rows.Close()
			}
			for _, setups := range testcases {
				if t := setups.dbtype; !microsecsSupported && t[len(t)-1:] == ")" {
					// skip fractional second tests if unsupported by server
					continue
				}
				for _, setup := range setups.tests {
					allowBinTime := true
					if setup.s == "" {
						// fill time string wherever Go can reliable produce it
						setup.s = setup.t.Format(setups.tlayout)
					} else if setup.s[0] == '!' {
						// skip tests using setup.t as source in queries
						allowBinTime = false
						// fix setup.s - remove the "!"
						setup.s = setup.s[1:]
					}
					if !zeroDateSupported && setup.s == tstr0[:len(setup.s)] {
						// skip disallowed 0000-00-00 date
						continue
					}
					setup.run(dbt, setups.dbtype, setups.tlayout, textString)
					setup.run(dbt, setups.dbtype, setups.tlayout, binaryString)
					if allowBinTime {
						setup.run(dbt, setups.dbtype, setups.tlayout, binaryTime)
					}
				}
			}
		})
	}
}

func TestTimestampMicros(t *testing.T) {
	format := "2006-01-02 15:04:05.999999"
	f0 := format[:19]
	f1 := format[:21]
	f6 := format[:26]
	runTests(t, dsn, func(dbt *DBTest) {
		// check if microseconds are supported.
		// Do not use timestamp(x) for that check - before 5.5.6, x would mean display width
		// and not precision.
		// Se last paragraph at http://dev.mysql.com/doc/refman/5.6/en/fractional-seconds.html
		microsecsSupported := false
		if rows, err := dbt.db.Query(`SELECT cast("00:00:00.1" as TIME(1)) = "00:00:00.1"`); err == nil {
			rows.Scan(&microsecsSupported)
			rows.Close()
		}
		if !microsecsSupported {
			// skip testaddon
			return
		}
		_, err := dbt.db.Exec(`
			CREATE TABLE testaddon (
				value0 TIMESTAMP NOT NULL DEFAULT '` + f0 + `',
				value1 TIMESTAMP(1) NOT NULL DEFAULT '` + f1 + `',
				value6 TIMESTAMP(6) NOT NULL DEFAULT '` + f6 + `'
			)`,
		)
		if err != nil {
			dbt.Error(err)
		}
		defer dbt.mustExec("DROP TABLE IF EXISTS testaddon")
		dbt.mustExec("INSERT INTO testaddon SET value0=?, value1=?, value6=?", f0, f1, f6)
		var res0, res1, res6 string
		rows := dbt.mustQuery("SELECT * FROM testaddon")
		defer rows.Close()
		if !rows.Next() {
			dbt.Errorf("testaddon contained no selectable values")
		}
		err = rows.Scan(&res0, &res1, &res6)
		if err != nil {
			dbt.Error(err)
		}
		if res0 != f0 {
			dbt.Errorf("expected %q, got %q", f0, res0)
		}
		if res1 != f1 {
			dbt.Errorf("expected %q, got %q", f1, res1)
		}
		if res6 != f6 {
			dbt.Errorf("expected %q, got %q", f6, res6)
		}
	})
}

func TestNULL(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		nullStmt, err := dbt.db.Prepare("SELECT NULL")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nullStmt.Close()

		nonNullStmt, err := dbt.db.Prepare("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}
		defer nonNullStmt.Close()

		// NullBool
		var nb sql.NullBool
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if nb.Valid {
			dbt.Error("valid NullBool which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nb); err != nil {
			dbt.Fatal(err)
		}
		if !nb.Valid {
			dbt.Error("invalid NullBool which should be valid")
		} else if nb.Bool != true {
			dbt.Errorf("Unexpected NullBool value: %t (should be true)", nb.Bool)
		}

		// NullFloat64
		var nf sql.NullFloat64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if nf.Valid {
			dbt.Error("valid NullFloat64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&nf); err != nil {
			dbt.Fatal(err)
		}
		if !nf.Valid {
			dbt.Error("invalid NullFloat64 which should be valid")
		} else if nf.Float64 != float64(1) {
			dbt.Errorf("unexpected NullFloat64 value: %f (should be 1.0)", nf.Float64)
		}

		// NullInt64
		var ni sql.NullInt64
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if ni.Valid {
			dbt.Error("valid NullInt64 which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ni); err != nil {
			dbt.Fatal(err)
		}
		if !ni.Valid {
			dbt.Error("invalid NullInt64 which should be valid")
		} else if ni.Int64 != int64(1) {
			dbt.Errorf("unexpected NullInt64 value: %d (should be 1)", ni.Int64)
		}

		// NullString
		var ns sql.NullString
		// Invalid
		if err = nullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if ns.Valid {
			dbt.Error("valid NullString which should be invalid")
		}
		// Valid
		if err = nonNullStmt.QueryRow().Scan(&ns); err != nil {
			dbt.Fatal(err)
		}
		if !ns.Valid {
			dbt.Error("invalid NullString which should be valid")
		} else if ns.String != `1` {
			dbt.Error("unexpected NullString value:" + ns.String + " (should be `1`)")
		}

		// nil-bytes
		var b []byte
		// Read nil
		if err = nullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil []byte which should be nil")
		}
		// Read non-nil
		if err = nonNullStmt.QueryRow().Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil []byte which should be non-nil")
		}
		// Insert nil
		b = nil
		success := false
		if err = dbt.db.QueryRow("SELECT ? IS NULL", b).Scan(&success); err != nil {
			dbt.Fatal(err)
		}
		if !success {
			dbt.Error("inserting []byte(nil) as NULL failed")
		}
		// Check input==output with input==nil
		b = nil
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b != nil {
			dbt.Error("non-nil echo from nil input")
		}
		// Check input==output with input!=nil
		b = []byte("")
		if err = dbt.db.QueryRow("SELECT ?", b).Scan(&b); err != nil {
			dbt.Fatal(err)
		}
		if b == nil {
			dbt.Error("nil echo from non-nil input")
		}

		// Insert NULL
		dbt.mustExec("CREATE TABLE testaddon (dummmy1 int, value int, dummy2 int)")

		dbt.mustExec("INSERT INTO testaddon VALUES (?, ?, ?)", 1, nil, 2)

		var out interface{}
		rows := dbt.mustQuery("SELECT * FROM testaddon")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if out != nil {
				dbt.Errorf("%v != nil", out)
			}
		} else {
			dbt.Error("no data")
		}
	})
}

func TestUint64(t *testing.T) {
	const (
		u0    = uint64(0)
		uall  = ^u0
		uhigh = uall >> 1
		utop  = ^uhigh
		s0    = int64(0)
		sall  = ^s0
		shigh = int64(uhigh)
		stop  = ^shigh
	)
	runTests(t, dsn, func(dbt *DBTest) {
		stmt, err := dbt.db.Prepare(`SELECT ?, ?, ? ,?, ?, ?, ?, ?`)
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()
		row := stmt.QueryRow(
			u0, uhigh, utop, uall,
			s0, shigh, stop, sall,
		)

		var ua, ub, uc, ud uint64
		var sa, sb, sc, sd int64

		err = row.Scan(&ua, &ub, &uc, &ud, &sa, &sb, &sc, &sd)
		if err != nil {
			dbt.Fatal(err)
		}
		switch {
		case ua != u0,
			ub != uhigh,
			uc != utop,
			ud != uall,
			sa != s0,
			sb != shigh,
			sc != stop,
			sd != sall:
			dbt.Fatal("unexpected result value")
		}
	})
}

func TestLongData(t *testing.T) {
	time.Sleep(1 * time.Second) // Prevent reuse of lock sessoin by lack of connections
	runTests(t, dsn+"&maxAllowedPacket=0", func(dbt *DBTest) {
		var maxAllowedPacketSize int
		err := dbt.db.QueryRow("select @@max_allowed_packet").Scan(&maxAllowedPacketSize)
		if err != nil {
			dbt.Fatal(err)
		}
		maxAllowedPacketSize--

		// don't get too ambitious
		if maxAllowedPacketSize > 1<<25 {
			maxAllowedPacketSize = 1 << 25
		}

		dbt.mustExec("CREATE TABLE testaddon (value LONGBLOB)")

		in := strings.Repeat(`a`, maxAllowedPacketSize+1)
		var out string
		var rows *sql.Rows

		// Long text data
		const nonDataQueryLen = 32 + 10 // length query w/o value
		inS := in[:maxAllowedPacketSize-nonDataQueryLen]
		log.Printf("TestLongData: maxAllowedPacketSize=%v, len(in)=%v, len(inS)=%v",
			maxAllowedPacketSize, len(in), len(inS))
		dbt.mustExec("INSERT INTO testaddon VALUES('" + inS + "')")
		rows = dbt.mustQuery("SELECT value FROM testaddon")
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if inS != out {
				dbt.Fatalf("LONGBLOB: length in: %d, length out: %d", len(inS), len(out))
			}
			if rows.Next() {
				dbt.Error("LONGBLOB: unexpexted row")
			}
		} else {
			dbt.Fatalf("LONGBLOB: no data")
		}

		// Empty table
		dbt.mustExec("TRUNCATE TABLE testaddon")

		// Long binary data
		dbt.mustExec("INSERT INTO testaddon VALUES(?)", in)
		rows = dbt.mustQuery("SELECT value FROM testaddon WHERE 1=?", 1)
		defer rows.Close()
		if rows.Next() {
			rows.Scan(&out)
			if in != out {
				dbt.Fatalf("LONGBLOB: length in: %d, length out: %d", len(in), len(out))
			}
			if rows.Next() {
				dbt.Error("LONGBLOB: unexpexted row")
			}
		} else {
			if err = rows.Err(); err != nil {
				dbt.Fatalf("LONGBLOB: no data (err: %s)", err.Error())
			} else {
				dbt.Fatal("LONGBLOB: no data (err: <nil>)")
			}
		}
	})
}

func TestLoadData(t *testing.T) {
	log.Printf("TestLoadData of dsn %v", dsn)
	runTests(t, dsn, func(dbt *DBTest) {
		verifyLoadDataResult := func() {
			rows, err := dbt.db.Query("SELECT * FROM testaddon")
			if err != nil {
				dbt.Fatal(err.Error())
			}

			i := 0
			values := [4]string{
				"a string",
				"a string containing a \t",
				"a string containing a \n",
				"a string containing both \t\n",
			}

			var id int
			var value string

			for rows.Next() {
				i++
				err = rows.Scan(&id, &value)
				if err != nil {
					dbt.Fatal(err.Error())
				}
				if i != id {
					dbt.Fatalf("%d != %d", i, id)
				}
				if values[i-1] != value {
					dbt.Fatalf("%q != %q", values[i-1], value)
				}
			}
			err = rows.Err()
			if err != nil {
				dbt.Fatal(err.Error())
			}

			if i != 4 {
				dbt.Fatalf("rows count mismatch. Got %d, want 4", i)
			}
		}

		dbt.db.Exec("DROP TABLE IF EXISTS testaddon")
		dbt.mustExec("CREATE TABLE testaddon (id INT NOT NULL PRIMARY KEY, value TEXT NOT NULL) CHARACTER SET utf8")

		// Local File
		file, err := ioutil.TempFile("", "gotest")
		defer os.Remove(file.Name())
		if err != nil {
			dbt.Fatal(err)
		}
		mysql.RegisterLocalFile(file.Name())

		// Try first with empty file
		dbt.mustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE %q INTO TABLE testaddon", file.Name()))
		var count int
		err = dbt.db.QueryRow("SELECT COUNT(*) FROM testaddon").Scan(&count)
		if err != nil {
			dbt.Fatal(err.Error())
		}
		if count != 0 {
			dbt.Fatalf("unexpected row count: got %d, want 0", count)
		}

		// Then fille File with data and try to load it
		file.WriteString("1\ta string\n2\ta string containing a \\t\n3\ta string containing a \\n\n4\ta string containing both \\t\\n\n")
		file.Close()
		dbt.mustExec(fmt.Sprintf("LOAD DATA LOCAL INFILE %q INTO TABLE testaddon", file.Name()))
		verifyLoadDataResult()

		// Try with non-existing file
		_, err = dbt.db.Exec("LOAD DATA LOCAL INFILE 'doesnotexist' INTO TABLE testaddon")
		if err == nil {
			dbt.Fatal("load non-existent file didn't fail")
		} else if err.Error() != "local file 'doesnotexist' is not registered" {
			dbt.Fatal(err.Error())
		}

		// Empty table
		dbt.mustExec("TRUNCATE TABLE testaddon")

		// Reader
		mysql.RegisterReaderHandler("testaddon", func() io.Reader {
			file, err = os.Open(file.Name())
			if err != nil {
				dbt.Fatal(err)
			}
			return file
		})
		dbt.mustExec("LOAD DATA LOCAL INFILE 'Reader::testaddon' INTO TABLE testaddon")
		verifyLoadDataResult()
		// negative test
		_, err = dbt.db.Exec("LOAD DATA LOCAL INFILE 'Reader::doesnotexist' INTO TABLE testaddon")
		if err == nil {
			dbt.Fatal("load non-existent Reader didn't fail")
		} else if err.Error() != "Reader 'doesnotexist' is not registered" {
			dbt.Fatal(err.Error())
		}
	})
}

func TestFoundRows(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (id INT NOT NULL ,data INT NOT NULL)")
		dbt.mustExec("INSERT INTO testaddon (id, data) VALUES (0, 0),(0, 0),(1, 0),(1, 0),(1, 1)")

		res := dbt.mustExec("UPDATE testaddon SET data = 1 WHERE id = 0")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 2 {
			dbt.Fatalf("Expected 2 affected rows, got %d", count)
		}
		res = dbt.mustExec("UPDATE testaddon SET data = 1 WHERE id = 1")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 2 {
			dbt.Fatalf("Expected 2 affected rows, got %d", count)
		}
	})
	runTests(t, dsn+"&clientFoundRows=true", func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (id INT NOT NULL ,data INT NOT NULL)")
		dbt.mustExec("INSERT INTO testaddon (id, data) VALUES (0, 0),(0, 0),(1, 0),(1, 0),(1, 1)")

		res := dbt.mustExec("UPDATE testaddon SET data = 1 WHERE id = 0")
		count, err := res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 2 {
			dbt.Fatalf("Expected 2 matched rows, got %d", count)
		}
		res = dbt.mustExec("UPDATE testaddon SET data = 1 WHERE id = 1")
		count, err = res.RowsAffected()
		if err != nil {
			dbt.Fatalf("res.RowsAffected() returned error: %s", err.Error())
		}
		if count != 3 {
			dbt.Fatalf("Expected 3 matched rows, got %d", count)
		}
	})
}

func TestTLS(t *testing.T) {
	tlsTestReq := func(dbt *DBTest) {
		if err := dbt.db.Ping(); err != nil {
			if err == mysql.ErrNoTLS {
				dbt.Skip("server does not support TLS")
			} else {
				dbt.Fatalf("error on Ping: %s", err.Error())
			}
		}

		rows := dbt.mustQuery("SHOW STATUS LIKE 'Ssl_cipher'")
		defer rows.Close()

		var variable, value *sql.RawBytes
		for rows.Next() {
			if err := rows.Scan(&variable, &value); err != nil {
				dbt.Fatal(err.Error())
			}

			if (*value == nil) || (len(*value) == 0) {
				dbt.Fatalf("no Cipher")
			} else {
				dbt.Logf("Cipher: %s", *value)
			}
		}
	}
	tlsTestOpt := func(dbt *DBTest) {
		if err := dbt.db.Ping(); err != nil {
			dbt.Fatalf("error on Ping: %s", err.Error())
		}
	}

	runTests(t, dsn+"&tls=preferred", tlsTestOpt)
	runTests(t, dsn+"&tls=skip-verify", tlsTestReq)

	// Verify that registering / using a custom cfg works
	mysql.RegisterTLSConfig("custom-skip-verify", &tls.Config{
		InsecureSkipVerify: true,
	})
	runTests(t, dsn+"&tls=custom-skip-verify", tlsTestReq)
}

func TestReuseClosedConnection(t *testing.T) {
	// this testaddon does not use sql.database, it uses the driver directly
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	md := &mysql.MySQLDriver{}
	conn, err := md.Open(dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	stmt, err := conn.Prepare("DO 1")
	if err != nil {
		t.Fatalf("error preparing statement: %s", err.Error())
	}
	_, err = stmt.Exec(nil)
	if err != nil {
		t.Fatalf("error executing statement: %s", err.Error())
	}
	err = conn.Close()
	if err != nil {
		t.Fatalf("error closing connection: %s", err.Error())
	}

	defer func() {
		if err := recover(); err != nil {
			t.Errorf("panic after reusing a closed connection: %v", err)
		}
	}()
	_, err = stmt.Exec(nil)
	if err != nil && err != driver.ErrBadConn {
		t.Errorf("unexpected error '%s', expected '%s'",
			err.Error(), driver.ErrBadConn.Error())
	}
}

func TestCharset(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mustSetCharset := func(charsetParam, expected string) {
		runTests(t, dsn+"&"+charsetParam, func(dbt *DBTest) {
			rows := dbt.mustQuery("SELECT @@character_set_connection")
			defer rows.Close()

			if !rows.Next() {
				dbt.Fatalf("error getting connection charset: %s", rows.Err())
			}

			var got string
			rows.Scan(&got)

			if got != expected {
				dbt.Fatalf("expected connection charset %s but got %s", expected, got)
			}
		})
	}

	// non utf8 testaddon
	mustSetCharset("charset=ascii", "ascii")

	// when the first charset is invalid, use the second
	mustSetCharset("charset=none,utf8", "utf8")

	// when the first charset is valid, use it
	mustSetCharset("charset=ascii,utf8", "ascii")
	mustSetCharset("charset=utf8,ascii", "utf8")
}

func TestFailingCharset(t *testing.T) {
	runTests(t, dsn+"&charset=none", func(dbt *DBTest) {
		// run query to really establish connection...
		_, err := dbt.db.Exec("SELECT 1")
		if err == nil {
			dbt.db.Close()
			t.Fatalf("connection must not succeed without a valid charset")
		}
	})
}

func TestCollation(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	defaultCollation := "utf8mb4_general_ci"
	testCollations := []string{
		"",               // do not set
		defaultCollation, // driver default
		"latin1_general_ci",
		"binary",
		"utf8_unicode_ci",
		"cp1257_bin",
	}

	for _, collation := range testCollations {
		var expected, tdsn string
		if collation != "" {
			tdsn = dsn + "&collation=" + collation
			expected = collation
		} else {
			tdsn = dsn
			expected = defaultCollation
		}

		runTests(t, tdsn, func(dbt *DBTest) {
			var got string
			if err := dbt.db.QueryRow("SELECT @@collation_connection").Scan(&got); err != nil {
				dbt.Fatal(err)
			}

			if got != expected {
				dbt.Fatalf("expected connection collation %s but got %s", expected, got)
			}
		})
	}
}

func TestColumnsWithAlias(t *testing.T) {
	log.Printf("TestColumnsWithAlias: dsn %v", dsn)
	runTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		log.Printf("TestColumnsWithAlias: dsn %v", dsn)

		rows := dbt.mustQuery("SELECT 1 AS A")
		defer rows.Close()
		cols, _ := rows.Columns()
		if len(cols) != 1 {
			t.Fatalf("expected 1 column, got %d", len(cols))
		}
		if cols[0] != "A" {
			t.Fatalf("expected column name \"A\", got \"%s\"", cols[0])
		}

		rows = dbt.mustQuery("SELECT * FROM (SELECT 1 AS one) AS A")
		defer rows.Close()
		cols, _ = rows.Columns()
		if len(cols) != 1 {
			t.Fatalf("expected 1 column, got %d", len(cols))
		}
		if cols[0] != "A.one" {
			t.Fatalf("expected column name \"A.one\", got \"%s\"", cols[0])
		}
	})
}

func TestTimezoneConversion(t *testing.T) {
	zones := []string{"UTC", "US/Central", "US/Pacific", "Local"}

	// Regression testaddon for timezone handling
	tzTest := func(dbt *DBTest) {
		dbt.mustExec("DROP TABLE IF EXISTS test_addon_timezone")

		// Create table
		dbt.mustExec("CREATE TABLE test_addon_timezone (ts TIMESTAMP)")

		// Insert local time into database (should be converted)
		usCentral, _ := time.LoadLocation("US/Central")
		log.Printf("TestTimezoneConversion: usCentral %v", usCentral)
		reftime := time.Date(2014, 05, 30, 18, 03, 17, 0, time.UTC).In(usCentral)
		dbt.mustExec("INSERT INTO test_addon_timezone VALUE (?)", reftime)

		// Retrieve time from DB
		rows := dbt.mustQuery("SELECT ts FROM test_addon_timezone")
		defer rows.Close()
		if !rows.Next() {
			dbt.Fatal("did not get any rows out")
		}

		var dbTime time.Time
		err := rows.Scan(&dbTime)
		if err != nil {
			dbt.Fatal("Err", err)
		}

		// Check that dates match
		if reftime.Unix() != dbTime.Unix() {
			dbt.Errorf("times do not match.\n")
			dbt.Errorf(" Now(%v)=%v\n", usCentral, reftime)
			dbt.Errorf(" Now(UTC)=%v\n", dbTime)
		}

		dbt.mustExec("DROP TABLE IF EXISTS test_addon_timezone")
	}

	for _, tz := range zones {
		runTests(t, dsn+"&parseTime=true&loc="+url.QueryEscape(tz), tzTest)
	}
}

// Special cases

func TestRowsClose(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows, err := dbt.db.Query("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}

		err = rows.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		if rows.Next() {
			dbt.Fatal("unexpected row after rows.Close()")
		}

		err = rows.Err()
		if err != nil {
			dbt.Fatal(err)
		}
	})
}

// dangling statements
// http://code.google.com/p/go/issues/detail?id=3865
func TestCloseStmtBeforeRows(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		stmt, err := dbt.db.Prepare("SELECT 1")
		if err != nil {
			dbt.Fatal(err)
		}

		rows, err := stmt.Query()
		if err != nil {
			stmt.Close()
			dbt.Fatal(err)
		}
		defer rows.Close()

		err = stmt.Close()
		if err != nil {
			dbt.Fatal(err)
		}

		if !rows.Next() {
			dbt.Fatal("getting row failed")
		} else {
			err = rows.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			var out bool
			err = rows.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}
	})
}

// It is valid to have multiple Rows for the same Stmt
// http://code.google.com/p/go/issues/detail?id=3734
func TestStmtMultiRows(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		stmt, err := dbt.db.Prepare("SELECT 1 UNION SELECT 0")
		if err != nil {
			dbt.Fatal(err)
		}

		rows1, err := stmt.Query()
		if err != nil {
			stmt.Close()
			dbt.Fatal(err)
		}
		defer rows1.Close()

		rows2, err := stmt.Query()
		if err != nil {
			stmt.Close()
			dbt.Fatal(err)
		}
		defer rows2.Close()

		var out bool

		// 1
		if !rows1.Next() {
			dbt.Fatal("first rows1.Next failed")
		} else {
			err = rows1.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows1.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}

		if !rows2.Next() {
			dbt.Fatal("first rows2.Next failed")
		} else {
			err = rows2.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows2.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != true {
				dbt.Errorf("true != %t", out)
			}
		}

		// 2
		if !rows1.Next() {
			dbt.Fatal("second rows1.Next failed")
		} else {
			err = rows1.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows1.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != false {
				dbt.Errorf("false != %t", out)
			}

			if rows1.Next() {
				dbt.Fatal("unexpected row on rows1")
			}
			err = rows1.Close()
			if err != nil {
				dbt.Fatal(err)
			}
		}

		if !rows2.Next() {
			dbt.Fatal("second rows2.Next failed")
		} else {
			err = rows2.Err()
			if err != nil {
				dbt.Fatal(err)
			}

			err = rows2.Scan(&out)
			if err != nil {
				dbt.Fatalf("error on rows.Scan(): %s", err.Error())
			}
			if out != false {
				dbt.Errorf("false != %t", out)
			}

			if rows2.Next() {
				dbt.Fatal("unexpected row on rows2")
			}
			err = rows2.Close()
			if err != nil {
				dbt.Fatal(err)
			}
		}
	})
}

// Regression testaddon for
// * more than 32 NULL parameters (issue 209)
// * more parameters than fit into the buffer (issue 201)
// * parameters * 64 > max_allowed_packet (issue 734)
func TestPreparedManyCols(t *testing.T) {
	numParams := 65535
	runTests(t, dsn, func(dbt *DBTest) {
		query := "SELECT ?" + strings.Repeat(",?", numParams-1)
		stmt, err := dbt.db.Prepare(query)
		if err != nil {
			dbt.Fatal(err)
		}
		defer stmt.Close()

		// create more parameters than fit into the buffer
		// which will take nil-values
		params := make([]interface{}, numParams)
		rows, err := stmt.Query(params...)
		if err != nil {
			dbt.Fatal(err)
		}
		rows.Close()

		// Create 0byte string which we can't send via STMT_LONG_DATA.
		for i := 0; i < numParams; i++ {
			params[i] = ""
		}
		rows, err = stmt.Query(params...)
		if err != nil {
			dbt.Fatal(err)
		}
		rows.Close()
	})
}

func testDialError(t *testing.T, dialErr error, expectErr error) {
	mysql.RegisterDialContext("mydial", func(ctx context.Context, addr string) (net.Conn, error) {
		return nil, dialErr
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@mydial(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	_, err = db.Exec("DO 1")
	if err != expectErr {
		t.Fatalf("was expecting %s. Got: %s", dialErr, err)
	}
}

func TestDialUnknownError(t *testing.T) {
	testErr := fmt.Errorf("testaddon")
	testDialError(t, testErr, testErr)
}

func TestDialNonRetryableNetErr(t *testing.T) {
	testErr := netErrorMock{}
	testDialError(t, testErr, testErr)
}

func TestDialTemporaryNetErr(t *testing.T) {
	testErr := netErrorMock{temporary: true}
	testDialError(t, testErr, testErr)
}

// Tests custom dial functions
func TestCustomDial(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	// our custom dial function which justs wraps net.Dial here
	mysql.RegisterDialContext("mydial", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, prot, addr)
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@mydial(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	if _, err = db.Exec("DO 1"); err != nil {
		t.Fatalf("connection failed: %s", err.Error())
	}

	// Deprecated, but still work, our custom dial function which justs wraps net.Dial here
	mysql.RegisterDial("mydialdepre", func(addr string) (net.Conn, error) {
		var d net.Dialer
		return d.Dial(prot, addr)
	})

	db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@mydialdepre(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	if _, err = db.Exec("DO 1"); err != nil {
		t.Fatalf("connection failed: %s", err.Error())
	}
}

func TestSQLInjection(t *testing.T) {
	createTest := func(arg string) func(dbt *DBTest) {
		return func(dbt *DBTest) {
			dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
			dbt.mustExec("INSERT INTO testaddon VALUES (?)", 1)

			var v int
			// NULL can't be equal to anything, the idea here is to inject query so it returns row
			// This testaddon verifies that escapeQuotes and escapeBackslash are working properly
			err := dbt.db.QueryRow("SELECT v FROM testaddon WHERE NULL = ?", arg).Scan(&v)
			if err == sql.ErrNoRows {
				return // success, sql injection failed
			} else if err == nil {
				dbt.Errorf("sql injection successful with arg: %s", arg)
			} else {
				dbt.Errorf("error running query with arg: %s; err: %s", arg, err.Error())
			}
		}
	}

	dsns := []string{
		dsn,
		dsn + "&sql_mode='NO_BACKSLASH_ESCAPES'",
	}
	for _, testdsn := range dsns {
		runTests(t, testdsn, createTest("1 OR 1=1"))
		runTests(t, testdsn, createTest("' OR '1'='1"))
	}
}

// Test if inserted data is correctly retrieved after being escaped
func TestInsertRetrieveEscapedData(t *testing.T) {
	testData := func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v VARCHAR(255))")

		// All sequences that are escaped by escapeQuotes and escapeBackslash
		v := "foo \x00\n\r\x1a\"'\\"
		dbt.mustExec("INSERT INTO testaddon VALUES (?)", v)

		var out string
		err := dbt.db.QueryRow("SELECT v FROM testaddon").Scan(&out)
		if err != nil {
			dbt.Fatalf("%s", err.Error())
		}

		if out != v {
			dbt.Errorf("%q != %q", out, v)
		}
	}

	dsns := []string{
		dsn,
		dsn + "&sql_mode='NO_BACKSLASH_ESCAPES'",
	}
	for _, testdsn := range dsns {
		runTests(t, testdsn, testData)
	}
}

// See Issue #422
func TestInterruptBySignal(t *testing.T) {
	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		dbt.mustExec(`
			DROP PROCEDURE IF EXISTS test_signal;
			CREATE PROCEDURE test_signal(ret INT)
			BEGIN
				SELECT ret;
				SIGNAL SQLSTATE
					'45001'
				SET
					MESSAGE_TEXT = "an error",
					MYSQL_ERRNO = 45001;
			END
		`)
		defer dbt.mustExec("DROP PROCEDURE test_signal")

		var val int

		// text protocol
		rows, err := dbt.db.Query("CALL test_signal(42)")
		if err != nil {
			dbt.Fatalf("error on text query: %s", err.Error())
		}
		for rows.Next() {
			if err := rows.Scan(&val); err != nil {
				dbt.Error(err)
			} else if val != 42 {
				dbt.Errorf("expected val to be 42")
			}
		}
		rows.Close()

		// binary protocol
		rows, err = dbt.db.Query("CALL test_signal(?)", 42)
		if err != nil {
			dbt.Fatalf("error on binary query: %s", err.Error())
		}
		for rows.Next() {
			if err := rows.Scan(&val); err != nil {
				dbt.Error(err)
			} else if val != 42 {
				dbt.Errorf("expected val to be 42")
			}
		}
		rows.Close()
	})
}

func TestRejectReadOnly(t *testing.T) {
	time.Sleep(1 * time.Second) // Prevent reuse of lock sessoin by lack of connections

	runTests(t, dsn, func(dbt *DBTest) {
		// Create Table
		dbt.mustExec("CREATE TABLE testaddon (value BOOL)")
		// Set the session to read-only. We didn't set the `rejectReadOnly`
		// option, so any writes after this should fail.
		_, err := dbt.db.Exec("SET SESSION TRANSACTION READ ONLY")
		// Error 1193: Unknown system variable 'TRANSACTION' => skip testaddon,
		// MySQL server version is too old
		maybeSkip(t, err, 1193)
		if _, err := dbt.db.Exec("DROP TABLE testaddon"); err == nil {
			t.Fatalf("writing to DB in read-only session without " +
				"rejectReadOnly did not error")
		}
		// Set the session back to read-write so runTests() can properly clean
		// up the table `testaddon`.
		dbt.mustExec("SET SESSION TRANSACTION READ WRITE")
	})

	time.Sleep(1 * time.Second) // Prevent reuse of lock sessoin by lack of connections
	// Enable the `rejectReadOnly` option.
	runTests(t, dsn+"&rejectReadOnly=true", func(dbt *DBTest) {

		// Create Table
		dbt.mustExec("CREATE TABLE testaddon (value BOOL)")
		// Set the session to read only. Any writes after this should error on
		// a driver.ErrBadConn, and cause `database/sql` to initiate a new
		// connection.
		dbt.mustExec("SET SESSION TRANSACTION READ ONLY")
		// This would error, but `database/sql` should automatically retry on a
		// new connection which is not read-only, and eventually succeed.
		dbt.mustExec("DROP TABLE testaddon")
	})
}

func TestPing(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		if err := dbt.db.Ping(); err != nil {
			dbt.fail("Ping", "Ping", err)
		}
	})
}

// See Issue #799
func TestEmptyPassword(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	dsn := fmt.Sprintf("%s:%s@%s/%s?timeout=30s", user, "", netAddr, dbname)
	db, err := sql.Open("mysql", dsn)
	if err == nil {
		defer db.Close()
		err = db.Ping()
	}

	if pass == "" {
		if err != nil {
			t.Fatal(err.Error())
		}
	} else {
		if err == nil {
			t.Fatal("expected authentication error")
		}
		if !strings.HasPrefix(err.Error(), "Error 1045") {
			t.Fatal(err.Error())
		}
	}
}

func TestMultiResultSet(t *testing.T) {
	type result struct {
		values  [][]int
		columns []string
	}

	// checkRows is a helper testaddon function to validate rows containing 3 result
	// sets with specific values and columns. The basic query would look like this:
	//
	// SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
	// SELECT 0 UNION SELECT 1;
	// SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;
	//
	// to distinguish testaddon cases the first string argument is put in front of
	// every error or fatal message.
	checkRows := func(desc string, rows *sql.Rows, dbt *DBTest) {
		expected := []result{
			{
				values:  [][]int{{1, 2}, {3, 4}},
				columns: []string{"col1", "col2"},
			},
			{
				values:  [][]int{{1, 2, 3}, {4, 5, 6}},
				columns: []string{"col1", "col2", "col3"},
			},
		}

		var res1 result
		for rows.Next() {
			var res [2]int
			if err := rows.Scan(&res[0], &res[1]); err != nil {
				dbt.Fatal(err)
			}
			res1.values = append(res1.values, res[:])
		}

		cols, err := rows.Columns()
		if err != nil {
			dbt.Fatal(desc, err)
		}
		res1.columns = cols

		if !reflect.DeepEqual(expected[0], res1) {
			dbt.Error(desc, "want =", expected[0], "got =", res1)
		}

		if !rows.NextResultSet() {
			dbt.Fatal(desc, "expected next result set")
		}

		// ignoring one result set

		if !rows.NextResultSet() {
			dbt.Fatal(desc, "expected next result set")
		}

		var res2 result
		cols, err = rows.Columns()
		if err != nil {
			dbt.Fatal(desc, err)
		}
		res2.columns = cols

		for rows.Next() {
			var res [3]int
			if err := rows.Scan(&res[0], &res[1], &res[2]); err != nil {
				dbt.Fatal(desc, err)
			}
			res2.values = append(res2.values, res[:])
		}

		if !reflect.DeepEqual(expected[1], res2) {
			dbt.Error(desc, "want =", expected[1], "got =", res2)
		}

		if rows.NextResultSet() {
			dbt.Error(desc, "unexpected next result set")
		}

		if err := rows.Err(); err != nil {
			dbt.Error(desc, err)
		}
	}

	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery(`DO 1;
		SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
		DO 1;
		SELECT 0 UNION SELECT 1;
		SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;`)
		defer rows.Close()
		checkRows("query: ", rows, dbt)
	})

	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		queries := []string{
			`
			DROP PROCEDURE IF EXISTS test_mrss;
			CREATE PROCEDURE test_mrss()
			BEGIN
				DO 1;
				SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
				DO 1;
				SELECT 0 UNION SELECT 1;
				SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;
			END
		`,
			`
			DROP PROCEDURE IF EXISTS test_mrss;
			CREATE PROCEDURE test_mrss()
			BEGIN
				SELECT 1 AS col1, 2 AS col2 UNION SELECT 3, 4;
				SELECT 0 UNION SELECT 1;
				SELECT 1 AS col1, 2 AS col2, 3 AS col3 UNION SELECT 4, 5, 6;
			END
		`,
		}

		defer dbt.mustExec("DROP PROCEDURE IF EXISTS test_mrss")

		for i, query := range queries {
			dbt.mustExec(query)

			stmt, err := dbt.db.Prepare("CALL test_mrss()")
			if err != nil {
				dbt.Fatalf("%v (i=%d)", err, i)
			}
			defer stmt.Close()

			for j := 0; j < 2; j++ {
				rows, err := stmt.Query()
				if err != nil {
					dbt.Fatalf("%v (i=%d) (j=%d)", err, i, j)
				}
				checkRows(fmt.Sprintf("prepared stmt query (i=%d) (j=%d): ", i, j), rows, dbt)
			}
		}
	})
}

func TestMultiResultSetNoSelect(t *testing.T) {
	runTestsWithMultiStatement(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery("DO 1; DO 2;")
		defer rows.Close()

		if rows.Next() {
			dbt.Error("unexpected row")
		}

		if rows.NextResultSet() {
			dbt.Error("unexpected next result set")
		}

		if err := rows.Err(); err != nil {
			dbt.Error("expected nil; got ", err)
		}
	})
}

// tests if rows are set in a proper state if some results were ignored before
// calling rows.NextResultSet.
func TestSkipResults(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		rows := dbt.mustQuery("SELECT 1, 2")
		defer rows.Close()

		if !rows.Next() {
			dbt.Error("expected row")
		}

		if rows.NextResultSet() {
			dbt.Error("unexpected next result set")
		}

		if err := rows.Err(); err != nil {
			dbt.Error("expected nil; got ", err)
		}
	})
}

func TestPingContext(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := dbt.db.PingContext(ctx); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextCancelExec(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := dbt.db.ExecContext(ctx, "INSERT INTO testaddon VALUES (SLEEP(1))"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query to be done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM testaddon").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}

		// Context is already canceled, so error should come before execution.
		if _, err := dbt.db.ExecContext(ctx, "INSERT INTO testaddon VALUES (1)"); err == nil {
			dbt.Error("expected error")
		} else if err.Error() != "context canceled" {
			dbt.Fatalf("unexpected error: %s", err)
		}

		// The second insert query will fail, so the table has no changes.
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM testaddon").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 {
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := dbt.db.QueryContext(ctx, "INSERT INTO testaddon VALUES (SLEEP(1))"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query to be done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM testaddon").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}

		// Context is already canceled, so error should come before execution.
		if _, err := dbt.db.QueryContext(ctx, "INSERT INTO testaddon VALUES (1)"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}

		// The second insert query will fail, so the table has no changes.
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM testaddon").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 {
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelQueryRow(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		dbt.mustExec("INSERT INTO testaddon VALUES (1), (2), (3)")
		ctx, cancel := context.WithCancel(context.Background())

		rows, err := dbt.db.QueryContext(ctx, "SELECT v FROM testaddon")
		if err != nil {
			dbt.Fatalf("%s", err.Error())
		}

		// the first row will be succeed.
		var v int
		if !rows.Next() {
			dbt.Fatalf("unexpected end")
		}
		if err := rows.Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}

		cancel()
		// make sure the driver receives the cancel request.
		time.Sleep(100 * time.Millisecond)

		if rows.Next() {
			dbt.Errorf("expected end, but not")
		}
		if err := rows.Err(); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextCancelPrepare(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := dbt.db.PrepareContext(ctx, "SELECT 1"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextCancelStmtExec(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		stmt, err := dbt.db.PrepareContext(ctx, "INSERT INTO testaddon VALUES (SLEEP(1))")
		if err != nil {
			dbt.Fatalf("unexpected error: %v", err)
		}

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := stmt.ExecContext(ctx); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query to be done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM testaddon").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelStmtQuery(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		stmt, err := dbt.db.PrepareContext(ctx, "INSERT INTO testaddon VALUES (SLEEP(1))")
		if err != nil {
			dbt.Fatalf("unexpected error: %v", err)
		}

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(250*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := stmt.QueryContext(ctx); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Wait for the INSERT query has done.
		time.Sleep(time.Second)

		// Check how many times the query is executed.
		var v int
		if err := dbt.db.QueryRow("SELECT COUNT(*) FROM testaddon").Scan(&v); err != nil {
			dbt.Fatalf("%s", err.Error())
		}
		if v != 1 { // TODO: need to kill the query, and v should be 0.
			dbt.Skipf("[WARN] expected val to be 1, got %d", v)
		}
	})
}

func TestContextCancelBegin(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		tx, err := dbt.db.BeginTx(ctx, nil)
		if err != nil {
			dbt.Fatal(err)
		}

		// Delay execution for just a bit until db.ExecContext has begun.
		defer time.AfterFunc(100*time.Millisecond, cancel).Stop()

		// This query will be canceled.
		startTime := time.Now()
		if _, err := tx.ExecContext(ctx, "INSERT INTO testaddon VALUES (SLEEP(1))"); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
		if d := time.Since(startTime); d > 500*time.Millisecond {
			dbt.Errorf("too long execution time: %s", d)
		}

		// Transaction is canceled, so expect an error.
		switch err := tx.Commit(); err {
		case sql.ErrTxDone:
			// because the transaction has already been rollbacked.
			// the database/sql package watches ctx
			// and rollbacks when ctx is canceled.
		case context.Canceled:
			// the database/sql package rollbacks on another goroutine,
			// so the transaction may not be rollbacked depending on goroutine scheduling.
		default:
			dbt.Errorf("expected sql.ErrTxDone or context.Canceled, got %v", err)
		}

		// Context is canceled, so cannot begin a transaction.
		if _, err := dbt.db.BeginTx(ctx, nil); err != context.Canceled {
			dbt.Errorf("expected context.Canceled, got %v", err)
		}
	})
}

func TestContextBeginIsolationLevel(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tx1, err := dbt.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
		})
		if err != nil {
			dbt.Fatal(err)
		}

		tx2, err := dbt.db.BeginTx(ctx, &sql.TxOptions{
			Isolation: sql.LevelReadCommitted,
		})
		if err != nil {
			dbt.Fatal(err)
		}

		_, err = tx1.ExecContext(ctx, "INSERT INTO testaddon VALUES (1)")
		if err != nil {
			dbt.Fatal(err)
		}

		var v int
		row := tx2.QueryRowContext(ctx, "SELECT COUNT(*) FROM testaddon")
		if err := row.Scan(&v); err != nil {
			dbt.Fatal(err)
		}
		// Because writer transaction wasn't commited yet, it should be available
		if v != 0 {
			dbt.Errorf("expected val to be 0, got %d", v)
		}

		err = tx1.Commit()
		if err != nil {
			dbt.Fatal(err)
		}

		row = tx2.QueryRowContext(ctx, "SELECT COUNT(*) FROM testaddon")
		if err := row.Scan(&v); err != nil {
			dbt.Fatal(err)
		}
		// Data written by writer transaction is already commited, it should be selectable
		if v != 1 {
			dbt.Errorf("expected val to be 1, got %d", v)
		}
		tx2.Commit()
	})
}

func TestContextBeginReadOnly(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (v INTEGER)")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tx, err := dbt.db.BeginTx(ctx, &sql.TxOptions{
			ReadOnly: true,
		})
		if _, ok := err.(*mysql.MySQLError); ok {
			dbt.Skip("It seems that your MySQL does not support READ ONLY transactions")
			return
		} else if err != nil {
			dbt.Fatal(err)
		}

		// INSERT queries fail in a READ ONLY transaction.
		_, err = tx.ExecContext(ctx, "INSERT INTO testaddon VALUES (1)")
		if _, ok := err.(*mysql.MySQLError); !ok {
			dbt.Errorf("expected MySQLError, got %v", err)
		}

		// SELECT queries can be executed.
		var v int
		row := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM testaddon")
		if err := row.Scan(&v); err != nil {
			dbt.Fatal(err)
		}
		if v != 0 {
			dbt.Errorf("expected val to be 0, got %d", v)
		}

		if err := tx.Commit(); err != nil {
			dbt.Fatal(err)
		}
	})
}

func TestValuerWithValueReceiverGivenNilValue(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		dbt.mustExec("CREATE TABLE testaddon (value VARCHAR(255))")
		dbt.db.Exec("INSERT INTO testaddon VALUES (?)", (*testValuer)(nil))
		// This testaddon will panic on the INSERT if ConvertValue() does not check for typed nil before calling Value()
	})
}

var _ driver.DriverContext = &mysql.MySQLDriver{}

type dialCtxKey struct{}

func TestConnectorObeysDialTimeouts(t *testing.T) {
	if !available {
		t.Skipf("MySQL server not running on %s", netAddr)
	}

	mysql.RegisterDialContext("dialctxtest", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		if !ctx.Value(dialCtxKey{}).(bool) {
			return nil, fmt.Errorf("testaddon error: query context is not propagated to our dialer")
		}
		return d.DialContext(ctx, prot, addr)
	})

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@dialctxtest(%s)/%s?timeout=30s", user, pass, addr, dbname))
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	ctx := context.WithValue(context.Background(), dialCtxKey{}, true)

	_, err = db.ExecContext(ctx, "DO 1")
	if err != nil {
		t.Fatal(err)
	}
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

func TestNewConnector(t *testing.T) {
	mycnf := configForTests(t)
	conn, err := mysql.NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(conn)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestNewConnectorFail(t *testing.T) {
	mycnf := configForTests(t)
	mycnf.Net = "UDP"
	mycnf.Addr = ""
	_, err := mysql.NewConnector(mycnf)
	if err == nil {
		t.Fatalf("should be failed in normalizing mycnf")
	}
}

type slowConnection struct {
	net.Conn
	slowdown time.Duration
}

func (sc *slowConnection) Read(b []byte) (int, error) {
	time.Sleep(sc.slowdown)
	return sc.Conn.Read(b)
}

type connectorHijack struct {
	driver.Connector
	connErr error
}

func (cw *connectorHijack) Connect(ctx context.Context) (driver.Conn, error) {
	var conn driver.Conn
	conn, cw.connErr = cw.Connector.Connect(ctx)
	return conn, cw.connErr
}

func TestConnectorTimeoutsDuringOpen(t *testing.T) {
	mysql.RegisterDialContext("slowconn", func(ctx context.Context, addr string) (net.Conn, error) {
		var d net.Dialer
		conn, err := d.DialContext(ctx, prot, addr)
		if err != nil {
			return nil, err
		}
		return &slowConnection{Conn: conn, slowdown: 100 * time.Millisecond}, nil
	})

	mycnf := configForTests(t)
	mycnf.Net = "slowconn"

	conn, err := mysql.NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	hijack := &connectorHijack{Connector: conn}

	db := sql.OpenDB(hijack)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = db.ExecContext(ctx, "DO 1")
	if err != context.DeadlineExceeded {
		t.Fatalf("ExecContext should have timed out")
	}
	if hijack.connErr != context.DeadlineExceeded {
		t.Fatalf("(*Connector).Connect should have timed out")
	}
}

// A connection which can only be closed.
type dummyConnection struct {
	net.Conn
	closed bool
}

func (d *dummyConnection) Close() error {
	d.closed = true
	return nil
}

func TestConnectorTimeoutsWatchCancel(t *testing.T) {
	var (
		cancel  func()           // Used to cancel the context just after connecting.
		created *dummyConnection // The created connection.
	)

	mysql.RegisterDialContext("TestConnectorTimeoutsWatchCancel", func(ctx context.Context, addr string) (net.Conn, error) {
		// Canceling at this time triggers the watchCancel error branch in Connect().
		cancel()
		created = &dummyConnection{}
		return created, nil
	})

	mycnf := mysql.NewConfig()
	mycnf.User = "root"
	mycnf.Addr = "foo"
	mycnf.Net = "TestConnectorTimeoutsWatchCancel"

	conn, err := mysql.NewConnector(mycnf)
	if err != nil {
		t.Fatal(err)
	}

	db := sql.OpenDB(conn)
	defer db.Close()

	var ctx context.Context
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	if _, err := db.Conn(ctx); err != context.Canceled {
		t.Errorf("got %v, want context.Canceled", err)
	}

	if created == nil {
		t.Fatal("no connection created")
	}
	if !created.closed {
		t.Errorf("connection not closed")
	}
}
