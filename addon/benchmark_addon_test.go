// Go MSKeeper Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2020 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package addon

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	// "os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

type DBTestB struct {
	*testing.B
	db *Addon
}

func (dbt *DBTestB) fail(method, query string, err error) {
	if len(query) > 300 {
		query = "[query too large to print]"
	}
	dbt.Fatalf("error on %s %s: %s", method, query, err.Error())
}

func (dbt *DBTestB) mustQueryRow(query string, args ...interface{}) (row *sql.Row) {

	row = dbt.db.QueryRow(query, args...)
	if row == nil {
		dbt.fail("queryrow", query, nil)
	}
	return row
}

func (dbt *DBTestB) mustExec(query string, args ...interface{}) (res sql.Result) {
	res, err := dbt.db.Exec(query, args...)
	if err != nil {
		dbt.fail("exec", query, err)
	}
	return res
}

func (dbt *DBTestB) mustQuery(query string, args ...interface{}) (rows *sql.Rows) {
	rows, err := dbt.db.Query(query, args...)
	if err != nil {
		dbt.fail("query", query, err)
	}
	return rows
}

type TBMSKAddon testing.B

func (tb *TBMSKAddon) check(err error) {
	if err != nil {
		tb.Fatal(err)
	}
}

func (tb *TBMSKAddon) checkDB(db *sql.DB, err error) *sql.DB {
	tb.check(err)
	return db
}

func (tb *TBMSKAddon) checkRows(rows *sql.Rows, err error) *sql.Rows {
	tb.check(err)
	return rows
}

func (tb *TBMSKAddon) checkStmt(stmt *MSKStmt, err error) *MSKStmt {
	tb.check(err)
	return stmt
}

func initDBMSKAddon(b *testing.B, queries ...string) *Addon {
	tb := (*TBMSKAddon)(b)
	rawDB := tb.checkDB(sql.Open("mysql", dsn))

	db := NewMSKeeperAddon(
		rawDB,
	)
	db.SetOptions(
		options.WithSwitch(true),
		// options.WithLogOutput(os.Stdout),
		// options.WithLogLevel(notifier.WarnLevel),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper_addon.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
	)
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			b.Fatalf("error on %q: %v", query, err)
		}
	}
	return db
}

func BenchmarkQueryMSKAddon2(b *testing.B) {
	tb := (*TBMSKAddon)(b)
	b.StopTimer()
	b.ReportAllocs()
	db := initDBMSKAddon(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	db.RawDB().SetMaxIdleConns(concurrencyLevel)
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

func BenchmarkExecMSKAddon(b *testing.B) {
	tb := (*TBMSKAddon)(b)
	b.StopTimer()
	b.ReportAllocs()
	rawDB := tb.checkDB(sql.Open("mysql", dsn))
	rawDB.SetMaxIdleConns(concurrencyLevel)
	defer rawDB.Close()

	db := NewMSKeeperAddon(
		rawDB,
	)
	db.SetOptions(
		options.WithSwitch(true),
		// options.WithLogOutput(os.Stdout),
		// options.WithLogLevel(notifier.WarnLevel),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper_addon.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
	)
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

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

func BenchmarkRoundtripTxtMSKAddon(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	sampleString := string(sample)
	b.ReportAllocs()
	tb := (*TBMSKAddon)(b)
	rawDB := tb.checkDB(sql.Open("mysql", dsn))
	defer rawDB.Close()
	db := NewMSKeeperAddon(
		rawDB,
	)
	db.SetOptions(
		options.WithSwitch(true),
		// options.WithLogOutput(os.Stdout),
		// options.WithLogLevel(notifier.WarnLevel),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper_addon.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
	)
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

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

func BenchmarkRoundtripBinMSKAddon(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	b.ReportAllocs()
	tb := (*TBMSKAddon)(b)
	rawDB := tb.checkDB(sql.Open("mysql", dsn))
	defer rawDB.Close()
	db := NewMSKeeperAddon(
		rawDB,
	)
	db.SetOptions(
		options.WithSwitch(true),
		// options.WithLogOutput(os.Stdout),
		// options.WithLogLevel(notifier.WarnLevel),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper_addon.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
	)
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

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

func benchmarkQueryContextMSKAddon(b *testing.B, db *Addon, p int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db.RawDB().SetMaxIdleConns(p * runtime.GOMAXPROCS(0))

	tb := (*TBMSKAddon)(b)
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

func BenchmarkQueryContextMSKAddon(b *testing.B) {
	db := initDBMSKAddon(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContextMSKAddon(b, db, p)
		})
	}
}

func BenchmarkExecContextMSKAddon(b *testing.B) {
	db := initDBMSKAddon(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContextMSKAddon(b, db, p)
		})
	}
}

// BenchmarkQueryRawBytes benchmarks fetching 100 blobs using sql.RawBytes.
// "size=" means size of each blobs.
func BenchmarkQueryRawBytesMSKAddon(b *testing.B) {
	var sizes []int = []int{100, 1000, 2000, 4000, 8000, 12000, 16000, 32000, 64000, 256000}
	db := initDBMSKAddon(b,
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
			db.RawDB().SetMaxIdleConns(0)
			db.RawDB().SetMaxIdleConns(1)
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

func BenchmarkAKInsertMSKAddon(b *testing.B) {

	b.ResetTimer()

	runDefaultPolicyTestsExplicitBenchmark(b, dsn, func(dbt *DBTestB) {

		dbt.mustExec("CREATE TABLE `test_bench_addon` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		for i := 0; i < b.N; i++ {
			dbt.mustExec("INSERT INTO test_bench_addon VALUES (?, ?)", i, i)
		}
	})
}

func BenchmarkAKQueryRowFieldsTypeNotMatchMSKAddon(b *testing.B) {

	runDefaultPolicyTestsExplicitBenchmark(b, dsn, func(dbt *DBTestB) {

		dbt.mustExec("CREATE TABLE `test_bench_addon` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_addon VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			row := dbt.mustQueryRow("SELECT value FROM test_bench_addon where value1 = ?", i)
			var out int
			_ = row.Scan(&out)
		}
	})
}

func BenchmarkAKQueryRowMSKAddon(b *testing.B) {

	runDefaultPolicyTestsExplicitBenchmark(b, dsn, func(dbt *DBTestB) {

		dbt.mustExec("CREATE TABLE `test_bench_addon` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_addon VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			row := dbt.mustQueryRow("SELECT value FROM test_bench_addon where value = ?", i)
			var out int
			_ = row.Scan(&out)
		}
	})
}

func BenchmarkAKQueryMSKAddon(b *testing.B) {

	runDefaultPolicyTestsExplicitBenchmark(b, dsn, func(dbt *DBTestB) {

		dbt.mustExec("CREATE TABLE `test_bench_addon` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_addon VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			rows := dbt.mustQuery("SELECT value FROM test_bench_addon where value = ?", i)
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

func runDefaultPolicyTestsExplicitBenchmark(t *testing.B, dsn string, tests ...func(dbt *DBTestB)) {

	tb := (*TBMSKAddon)(t)
	rawDB := tb.checkDB(sql.Open("mysql", dsn))
	db := NewMSKeeperAddon(
		rawDB,
	)
	db.SetOptions(
		options.WithSwitch(true),
		// options.WithLogOutput(os.Stdout),
		// options.WithLogLevel(notifier.WarnLevel),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper_addon.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
	)
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = db.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = db.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	db.Exec("DROP TABLE IF EXISTS test_bench_addon")

	dbt := &DBTestB{t, db}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS test_bench_addon")
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test_bench_addon")
	}
}
