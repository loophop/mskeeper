// Go MySQL Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The Go-MySQL-Driver Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TB testing.B

func (tb *TB) check(err error) {
	if err != nil {
		tb.Fatal(err)
	}
}

func (tb *TB) checkDB(db *sql.DB, err error) *sql.DB {
	tb.check(err)
	return db
}

func (tb *TB) checkRows(rows *sql.Rows, err error) *sql.Rows {
	tb.check(err)
	return rows
}

func (tb *TB) checkStmt(stmt *sql.Stmt, err error) *sql.Stmt {
	tb.check(err)
	return stmt
}

func initDB(b *testing.B, queries ...string) *sql.DB {
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mskeeper", dsn))
	// dsn不变，则msk不变。dsn:msk <=> 1:1
	msk := MSKeeperInstance(dsn)
	if msk == nil {
		b.Fatalf("failed to get driver for %v", db.Driver())
	}
	msk.SetOptions(
		options.WithSwitch(true),
		// options.WithLogOutput(os.Stdout),
		// options.WithLogLevel(notifier.WarnLevel),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper_driver.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
	)
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			b.Fatalf("error on %q: %v", query, err)
		}
	}
	return db
}

const concurrencyLevel = 10

func BenchmarkQuery(b *testing.B) {
	tb := (*TB)(b)
	b.StopTimer()
	b.ReportAllocs()
	db := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	db.SetMaxIdleConns(concurrencyLevel)
	defer db.Close()

	stmt := tb.checkStmt(db.Prepare("SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	remain := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	defer wg.Wait()
	b.StartTimer()

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			for {
				if atomic.AddInt64(&remain, -1) < 0 {
					wg.Done()
					return
				}

				var got string
				tb.check(stmt.QueryRow(1).Scan(&got))
				if got != "one" {
					b.Errorf("query = %q; want one", got)
					wg.Done()
					return
				}
			}
		}()
	}
}

func BenchmarkExec(b *testing.B) {
	tb := (*TB)(b)
	b.StopTimer()
	b.ReportAllocs()
	db := tb.checkDB(sql.Open("mskeeper", dsn))
	db.SetMaxIdleConns(concurrencyLevel)
	defer db.Close()

	stmt := tb.checkStmt(db.Prepare("DO 1"))
	defer stmt.Close()

	remain := int64(b.N)
	var wg sync.WaitGroup
	wg.Add(concurrencyLevel)
	defer wg.Wait()
	b.StartTimer()

	for i := 0; i < concurrencyLevel; i++ {
		go func() {
			for {
				if atomic.AddInt64(&remain, -1) < 0 {
					wg.Done()
					return
				}

				if _, err := stmt.Exec(); err != nil {
					b.Fatal(err.Error())
				}
			}
		}()
	}
}

// data, but no db writes
var roundtripSample []byte

func initRoundtripBenchmarks() ([]byte, int, int) {
	if roundtripSample == nil {
		roundtripSample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
	}
	return roundtripSample, 16, len(roundtripSample)
}

func BenchmarkRoundtripTxt(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	sampleString := string(sample)
	b.ReportAllocs()
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mskeeper", dsn))
	defer db.Close()
	b.StartTimer()
	var result string
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sampleString[0:length]
		rows := tb.checkRows(db.Query(`SELECT "` + test + `"`))
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err := rows.Scan(&result)
		if err != nil {
			rows.Close()
			b.Fatalf("crashed")
		}
		if result != test {
			rows.Close()
			b.Errorf("mismatch")
		}
		rows.Close()
	}
}

func BenchmarkRoundtripBin(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	b.ReportAllocs()
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mskeeper", dsn))
	defer db.Close()
	stmt := tb.checkStmt(db.Prepare("SELECT ?"))
	defer stmt.Close()
	b.StartTimer()
	var result sql.RawBytes
	for i := 0; i < b.N; i++ {
		length := min + i
		if length > max {
			length = max
		}
		test := sample[0:length]
		rows := tb.checkRows(stmt.Query(test))
		if !rows.Next() {
			rows.Close()
			b.Fatalf("crashed")
		}
		err := rows.Scan(&result)
		if err != nil {
			rows.Close()
			b.Fatalf("crashed")
		}
		if !bytes.Equal(result, test) {
			rows.Close()
			b.Errorf("mismatch")
		}
		rows.Close()
	}
}

func BenchmarkInterpolation(b *testing.B) {
	mc := &mysqlConn{
		cfg: &Config{
			InterpolateParams: true,
			Loc:               time.UTC,
		},
		maxAllowedPacket: maxPacketSize,
		maxWriteSize:     maxPacketSize - 1,
		buf:              newBuffer(nil),
	}

	args := []driver.Value{
		int64(42424242),
		float64(math.Pi),
		false,
		time.Unix(1423411542, 807015000),
		[]byte("bytes containing special chars ' \" \a \x00"),
		"string containing special chars ' \" \a \x00",
	}
	q := "SELECT ?, ?, ?, ?, ?, ?"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := mc.interpolateParams(q, args)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkQueryContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "SELECT val FROM foo WHERE id=?"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var got string
		for pb.Next() {
			tb.check(stmt.QueryRow(1).Scan(&got))
			if got != "one" {
				b.Fatalf("query = %q; want one", got)
			}
		}
	})
}

func BenchmarkQueryContext(b *testing.B) {
	db := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContext(b, db, p)
		})
	}
}

func benchmarkExecContext(b *testing.B, db *sql.DB, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TB)(b)
	stmt := tb.checkStmt(db.PrepareContext(ctx, "DO 1"))
	defer stmt.Close()

	b.SetParallelism(p)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := stmt.ExecContext(ctx); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkExecContext(b *testing.B) {
	db := initDB(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContext(b, db, p)
		})
	}
}

// BenchmarkQueryRawBytes benchmarks fetching 100 blobs using sql.RawBytes.
// "size=" means size of each blobs.
func BenchmarkQueryRawBytes(b *testing.B) {
	var sizes []int = []int{100, 1000, 2000, 4000, 8000, 12000, 16000, 32000, 64000, 256000}
	db := initDB(b,
		"DROP TABLE IF EXISTS bench_rawbytes",
		"CREATE TABLE bench_rawbytes (id INT PRIMARY KEY, val LONGBLOB)",
	)
	defer db.Close()

	blob := make([]byte, sizes[len(sizes)-1])
	for i := range blob {
		blob[i] = 42
	}
	for i := 0; i < 100; i++ {
		_, err := db.Exec("INSERT INTO bench_rawbytes VALUES (?, ?)", i, blob)
		if err != nil {
			b.Fatal(err)
		}
	}

	for _, s := range sizes {
		b.Run(fmt.Sprintf("size=%v", s), func(b *testing.B) {
			db.SetMaxIdleConns(0)
			db.SetMaxIdleConns(1)
			b.ReportAllocs()
			b.ResetTimer()

			for j := 0; j < b.N; j++ {
				rows, err := db.Query("SELECT LEFT(val, ?) as v FROM bench_rawbytes", s)
				if err != nil {
					b.Fatal(err)
				}
				nrows := 0
				for rows.Next() {
					var buf sql.RawBytes
					err := rows.Scan(&buf)
					if err != nil {
						b.Fatal(err)
					}
					if len(buf) != s {
						b.Fatalf("size mismatch: expected %v, got %v", s, len(buf))
					}
					nrows++
				}
				rows.Close()
				if nrows != 100 {
					b.Fatalf("numbers of rows mismatch: expected %v, got %v", 100, nrows)
				}
			}
		})
	}
}

type DBTestRawB struct {
	*testing.B
	db *sql.DB
}

func (dbt *DBTestRawB) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTestRawB) mustQueryRow(query string, args ...interface{}) (row *sql.Row) {
	row = dbt.db.QueryRow(query, args...)
	if row == nil {
		dbt.fail("queryrow", query, nil)
	}
	return row
}

func (dbt *DBTestRawB) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
}

func (dbt *DBTestRawB) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func BenchmarkAKInsertMSKDriver(b *testing.B) {

	b.ResetTimer()

	runDefaultPolicyTestsImplicitBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_driver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < b.N; i++ {
			dbt.mustExec("INSERT INTO test_bench_driver VALUES (?, ?)", i, i)
		}
	})
}

func BenchmarkAKQueryRowFieldsTypeNotMatchMSKDriver(b *testing.B) {

	runDefaultPolicyTestsImplicitBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_driver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_driver VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			row := dbt.mustQueryRow("SELECT value FROM test_bench_driver where value1 = ?", i)
			var out int
			_ = row.Scan(&out)
		}
	})
}

func BenchmarkAKQueryRowMSKDriver(b *testing.B) {

	runDefaultPolicyTestsImplicitBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_driver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_driver VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			row := dbt.mustQueryRow("SELECT value FROM test_bench_driver where value = ?", i)
			var out int
			_ = row.Scan(&out)
		}
	})
}

func BenchmarkAKQueryMSKDriver(b *testing.B) {

	runDefaultPolicyTestsImplicitBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_driver` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_driver VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			rows := dbt.mustQuery("SELECT value FROM test_bench_driver where value = ?", i)
			for rows.Next() {
				var out int
				if err := rows.Scan(&out); err != nil {
					dbt.Errorf("rows.Scan() failed %v", err)
				}
			}
			rows.Close()
		}
	})
}

func runDefaultPolicyTestsImplicitBenchmark(t *testing.B, dsn string, tests ...func(dbt *DBTestRawB)) {

	db, err := sql.Open("mskeeper", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()

	// dsn不变，则msk不变。dsn:msk <=> 1:1
	msk := MSKeeperInstance(dsn)
	if msk == nil {
		t.Fatalf("failed to get driver for %v", db.Driver())
	}
	msk.SetOptions(
		options.WithSwitch(true),
		// options.WithLogOutput(os.Stdout),
		// options.WithLogLevel(notifier.WarnLevel),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper_driver.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
	)
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db.Exec("DROP TABLE IF EXISTS test_bench_driver")

	dbt := &DBTestRawB{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS test_bench_driver")
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test_bench_driver")
	}
}
