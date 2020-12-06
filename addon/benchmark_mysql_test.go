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
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// data, but no db writes
var roundtripSample []byte

func initRoundtripBenchmarks() ([]byte, int, int) {
	if roundtripSample == nil {
		roundtripSample = []byte(strings.Repeat("0123456789abcdef", 1024*1024))
	}
	return roundtripSample, 16, len(roundtripSample)
}

const concurrencyLevel = 10

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

func initDBMySQL(b *testing.B, queries ...string) *sql.DB {
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mysql", dsn))
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			b.Fatalf("error on %q: %v", query, err)
		}
	}
	return db
}

func BenchmarkQueryMySQL2(b *testing.B) {
	tb := (*TB)(b)
	b.StopTimer()
	b.ReportAllocs()
	db := initDBMySQL(b,
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

func BenchmarkExecMySQL(b *testing.B) {
	tb := (*TB)(b)
	b.StopTimer()
	b.ReportAllocs()
	db := tb.checkDB(sql.Open("mysql", dsn))
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

func BenchmarkRoundtripTxtMySQL(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	sampleString := string(sample)
	b.ReportAllocs()
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mysql", dsn))
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

func BenchmarkRoundtripBinMySQL(b *testing.B) {
	b.StopTimer()
	sample, min, max := initRoundtripBenchmarks()
	b.ReportAllocs()
	tb := (*TB)(b)
	db := tb.checkDB(sql.Open("mysql", dsn))
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

func benchmarkQueryContextMySQL(b *testing.B, db *sql.DB, p int) {
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

func BenchmarkQueryContextMySQL(b *testing.B) {
	db := initDBMySQL(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContextMySQL(b, db, p)
		})
	}
}

func BenchmarkExecContextMySQL(b *testing.B) {
	db := initDBMySQL(b,
		"DROP TABLE IF EXISTS foo",
		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
		`INSERT INTO foo VALUES (1, "one")`,
		`INSERT INTO foo VALUES (2, "two")`,
	)
	defer db.Close()
	for _, p := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("%d", p), func(b *testing.B) {
			benchmarkQueryContextMySQL(b, db, p)
		})
	}
}

// BenchmarkQueryRawBytes benchmarks fetching 100 blobs using sql.RawBytes.
// "size=" means size of each blobs.
func BenchmarkQueryRawBytesMySQL(b *testing.B) {
	var sizes []int = []int{100, 1000, 2000, 4000, 8000, 12000, 16000, 32000, 64000, 256000}
	db := initDBMySQL(b,
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

func BenchmarkAKInsertRaw(b *testing.B) {

	b.ResetTimer()

	runRawPolicyTestsBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_mysql` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < b.N; i++ {
			dbt.mustExec("INSERT INTO test_bench_mysql VALUES (?, ?)", i, i)
		}
	})
}

func BenchmarkAKQueryRowFieldsTypeNotMatchRaw(b *testing.B) {

	runRawPolicyTestsBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_mysql` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_mysql VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			row := dbt.mustQueryRow("SELECT value FROM test_bench_mysql where value1 = ?", i)
			var out int
			_ = row.Scan(&out)
		}
	})
}

func BenchmarkAKQueryRowRaw(b *testing.B) {

	runRawPolicyTestsBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_mysql` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_mysql VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			row := dbt.mustQueryRow("SELECT value FROM test_bench_mysql where value = ?", i)
			var out int
			_ = row.Scan(&out)
		}
	})
}

func BenchmarkAKQueryRaw(b *testing.B) {

	runRawPolicyTestsBenchmark(b, dsn, func(dbt *DBTestRawB) {

		dbt.mustExec("CREATE TABLE `test_bench_mysql` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 2000; i++ {
			dbt.mustExec("INSERT INTO test_bench_mysql VALUES (?, ?)", i, i)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 格式不匹配
			rows := dbt.mustQuery("SELECT value FROM test_bench_mysql where value = ?", i)
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

func runRawPolicyTestsBenchmark(t *testing.B, dsn string, tests ...func(dbt *DBTestRawB)) {

	rawDB, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}

	defer rawDB.Close()

	rawDB.Exec("DROP TABLE IF EXISTS test_bench_mysql")

	dbt := &DBTestRawB{t, rawDB}
	for _, test := range tests {
		dbt.db.Exec("DROP TABLE IF EXISTS test_bench_mysql")
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test_bench_mysql")
	}
}
