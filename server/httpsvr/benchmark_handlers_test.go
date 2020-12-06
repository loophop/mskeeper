package httpsvr

import (
	"database/sql"
	"fmt"
	"github.com/gavv/httpexpect"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"log"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

// func BenchmarkQuery(b *testing.B) {
// 	tb := (*TB)(b)
// 	b.StopTimer()
// 	b.ReportAllocs()
// 	db := initDB(b,
// 		"DROP TABLE IF EXISTS foo",
// 		"CREATE TABLE foo (id INT PRIMARY KEY, val CHAR(50))",
// 		`INSERT INTO foo VALUES (1, "one")`,
// 		`INSERT INTO foo VALUES (2, "two")`,
// 	)
// 	db.SetMaxIdleConns(concurrencyLevel)
// 	defer db.Close()

// 	stmt := tb.checkStmt(db.Prepare("SELECT val FROM foo WHERE id=?"))
// 	defer stmt.Close()

// 	remain := int64(b.N)
// 	var wg sync.WaitGroup
// 	wg.Add(concurrencyLevel)
// 	defer wg.Wait()
// 	b.StartTimer()

// 	for i := 0; i < concurrencyLevel; i++ {
// 		go func() {
// 			for {
// 				if atomic.AddInt64(&remain, -1) < 0 {
// 					wg.Done()
// 					return
// 				}

// 				var got string
// 				tb.check(stmt.QueryRow(1).Scan(&got))
// 				if got != "one" {
// 					b.Errorf("query = %q; want one", got)
// 					wg.Done()
// 					return
// 				}
// 			}
// 		}()
// 	}
// }

func BenchmarkMySQLCheckParallel(b *testing.B) {

	recordCnt := 1
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS testapi")
	assert.True(b, err == nil, err)

	_, err = db.Exec("CREATE TABLE `testapi` ( `value` int(11), `value1` varchar(60), `value2` varchar(2),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	assert.True(b, err == nil, err)

	for i := 0; i < recordCnt; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO testapi VALUES (?, ?, ?)", i, i, fmt.Sprintf("%v", i))
		assert.True(b, err == nil, err)
	}

	testCase := []struct {
		input  string
		output policy.PolicyCode
	}{{
		input:  "select * from testapi where value = 1",
		output: policy.ErrPolicyCodeSafe,
	}, {
		input:  "update testapi set value2 = '123' where value = 1",
		output: policy.ErrPolicyCodeDataTruncate,
	}, {
		input:  "select * from testapi",
		output: policy.ErrPolicyCodeSafe,
	}, {
		input:  "update testapi set value2 = '45678'",
		output: policy.ErrPolicyCodeDataTruncate,
	}, {
		input:  fmt.Sprintf("update testapi set value1 = '%v' where value = 1", getRandomString(60*0.8+1)),
		output: policy.WarnPolicyCodeDataTruncate,
	},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 0; i < len(testCase); i++ {
				e := httpexpect.New(b, *mskserveraddr)

				// 获取 msid
				inargs := map[string]interface{}{
					"dsn": dsn,
				}
				retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

				retData.Value("data").Object().ContainsKey("msid")

				uuid := retData.Value("data").Object().Value("msid").String().Raw()

				inargs = map[string]interface{}{
					"msid":     uuid,
					"sql":      testCase[i].input,
					"duration": 1000,
				}

				retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

				log.Printf("retData %v", retData)
				retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
					ContainsKey("code").ValueEqual("code", testCase[i].output)

				retData.Value("data").Object().Value("request").Object().Value("sql").String().Equal(testCase[i].input)
			}
		}
	})

}

func BenchmarkMySQLRunParallelExecute(b *testing.B) {

	recordCnt := 1
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS testapi")
	assert.True(b, err == nil, err)

	_, err = db.Exec("CREATE TABLE `testapi` ( `value` int(11), `value1` varchar(60), `value2` varchar(2),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	assert.True(b, err == nil, err)

	for i := 0; i < recordCnt; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO testapi VALUES (?, ?, ?)", i, i, fmt.Sprintf("%v", i))
		assert.True(b, err == nil, err)
	}

	b.ResetTimer()
	var globalIdx int64 = 0
	log.Printf("only once start")

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			atomic.AddInt64(&globalIdx, 1)
			globalIdx = atomic.LoadInt64(&globalIdx)
			testCase := []struct {
				input        string
				rowsaffected int
				output       policy.PolicyCode
			}{
				{
					input:        fmt.Sprintf("update testapi set value = %v + 1, value2 = '%v'", globalIdx+100+int64(rand.Intn(1000000)), globalIdx+100),
					rowsaffected: 1,
					output:       policy.ErrPolicyCodeDataTruncate,
				},
			}
			for i := 0; i < len(testCase); i++ {
				e := httpexpect.New(b, *mskserveraddr)

				// 获取 msid
				inargs := map[string]interface{}{
					"dsn": dsn,
				}
				retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

				retData.Value("data").Object().ContainsKey("msid")

				uuid := retData.Value("data").Object().Value("msid").String().Raw()

				inargs = map[string]interface{}{
					"msid":     uuid,
					"sql":      fmt.Sprintf(testCase[i].input),
					"duration": 1000,
					"timeout":  2000000,
				}

				retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

				log.Printf("retData %v", retData)

				retData.Value("data").Object().Value("notifies").Array().Length().Equal(1)

				retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
					ContainsKey("code").ValueEqual("code", testCase[i].output)

				retData.Value("data").Object().Value("request").Object().Value("sql").String().Equal(testCase[i].input)
				// rowsaffected
				retData.Value("data").Object().Value("results").Object().Value("rows_affected").Number().Equal(testCase[i].rowsaffected)
			}
		}

	})

	log.Printf("only once end %v", globalIdx)
}

func BenchmarkMySQLRunExecute(b *testing.B) {

	recordCnt := 0
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS testapi")
	assert.True(b, err == nil, err)

	_, err = db.Exec("CREATE TABLE `testapi` ( `value` int(11), `value1` varchar(60), `value2` varchar(2),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	assert.True(b, err == nil, err)

	for i := 0; i < recordCnt; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO testapi VALUES (?, ?, ?)", i, i, fmt.Sprintf("%v", i))
		assert.True(b, err == nil, err)
	}

	b.ResetTimer()
	globalIdx := 100

	log.Printf("only once start")
	b.Run("case", func(b *testing.B) {
		globalIdx++
		testCase := []struct {
			input        string
			rowsaffected int
			output       policy.PolicyCode
		}{
			{
				input:        fmt.Sprintf("INSERT INTO testapi VALUES (%v, '%v', '%v')", globalIdx, globalIdx, globalIdx),
				rowsaffected: 1,
				output:       policy.ErrPolicyCodeDataTruncate,
			},
			{
				input:        fmt.Sprintf("update testapi set value1='%v' where value = %v ", time.Now().Nanosecond()+rand.Intn(1000), globalIdx),
				rowsaffected: 1,
				output:       policy.ErrPolicyCodeSafe,
			},
			{
				input:        fmt.Sprintf("delete from testapi where value = %v", globalIdx),
				rowsaffected: 1,
				output:       policy.ErrPolicyCodeSafe,
			},
			{
				input:        fmt.Sprintf("update testapi set value1='%v' where value = %v", time.Now().Nanosecond()+rand.Intn(1000), globalIdx),
				rowsaffected: 0,
				output:       policy.ErrPolicyCodeSafe,
			},
		}
		for i := 0; i < len(testCase); i++ {
			e := httpexpect.New(b, *mskserveraddr)

			// 获取 msid
			inargs := map[string]interface{}{
				"dsn": dsn,
			}
			retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

			retData.Value("data").Object().ContainsKey("msid")

			uuid := retData.Value("data").Object().Value("msid").String().Raw()

			inargs = map[string]interface{}{
				"msid":     uuid,
				"sql":      fmt.Sprintf(testCase[i].input),
				"duration": 1000,
				"timeout":  2000000,
			}

			retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

			log.Printf("retData %v", retData)

			retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
				ContainsKey("code").ValueEqual("code", testCase[i].output)

			retData.Value("data").Object().Value("request").Object().Value("sql").String().Equal(testCase[i].input)
			// rowsaffected
			retData.Value("data").Object().Value("results").Object().Value("rows_affected").Number().Equal(testCase[i].rowsaffected)
		}
	})

	log.Printf("only once end")
}

func BenchmarkMySQLRunQueryParallel(b *testing.B) {

	recordCnt := 1001
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS testapi")
	assert.True(b, err == nil, err)

	_, err = db.Exec("CREATE TABLE `testapi` ( `value` int(11), `value1` varchar(60), `value2` varchar(2),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	assert.True(b, err == nil, err)

	for i := 0; i < recordCnt; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO testapi VALUES (?, ?, ?)", i, i, fmt.Sprintf("%v", i))
		assert.True(b, err == nil, err)
	}

	testCase := []struct {
		input   string
		columns []map[string]string
		rows    [][]interface{}
	}{
		{
			input: "select * from testapi where value = 1",
			columns: []map[string]string{
				map[string]string{
					"name": "value",
					"type": "INT",
				},
				map[string]string{
					"name": "value1",
					"type": "VARCHAR",
				},

				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
			},
			rows: [][]interface{}{
				[]interface{}{1, "1", "1"},
			},
		},
		{
			input: "select * from testapi where value = 1 or value = 2",
			columns: []map[string]string{
				map[string]string{
					"name": "value",
					"type": "INT",
				},
				map[string]string{
					"name": "value1",
					"type": "VARCHAR",
				},

				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
			},
			rows: [][]interface{}{
				[]interface{}{1, "1", "1"},
				[]interface{}{2, "2", "2"},
			},
		},
		{
			input: "select value2, value from testapi where value < 4 and value > 0",
			columns: []map[string]string{
				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
				map[string]string{
					"name": "value",
					"type": "INT",
				},
			},
			rows: [][]interface{}{
				[]interface{}{"1", 1},
				[]interface{}{"2", 2},
				[]interface{}{"3", 3},
			},
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			for i := 0; i < len(testCase); i++ {
				e := httpexpect.New(b, *mskserveraddr)

				// 获取 msid
				inargs := map[string]interface{}{
					"dsn": dsn,
				}
				retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

				retData.Value("data").Object().ContainsKey("msid")

				uuid := retData.Value("data").Object().Value("msid").String().Raw()

				inargs = map[string]interface{}{
					"msid":     uuid,
					"sql":      testCase[i].input,
					"duration": 1000,
				}

				retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

				log.Printf("retData %v", retData)

				retData.Value("data").Object().Value("request").Object().Value("sql").String().Equal(testCase[i].input)

				// columns
				retData.Value("data").Object().Value("results").Object().Value("columns").Array().Equal(testCase[i].columns)

				// rows
				retData.Value("data").Object().Value("results").Object().Value("rows").Array().Equal(testCase[i].rows)
			}
		}
	})
}

func BenchmarkMySQLRunQueryDifferentMSKParallel(b *testing.B) {

	recordCnt := 1001
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS testapi")
	assert.True(b, err == nil, err)

	_, err = db.Exec("CREATE TABLE `testapi` ( `value` int(11), `value1` varchar(60), `value2` varchar(2),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	assert.True(b, err == nil, err)

	for i := 0; i < recordCnt; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO testapi VALUES (?, ?, ?)", i, i, fmt.Sprintf("%v", i))
		assert.True(b, err == nil, err)
	}

	testCase := []struct {
		tdsn    string
		input   string
		columns []map[string]string
		rows    [][]interface{}
	}{
		// with dsn
		{
			tdsn:  dsn,
			input: "select * from testapi where value = 1",
			columns: []map[string]string{
				map[string]string{
					"name": "value",
					"type": "INT",
				},
				map[string]string{
					"name": "value1",
					"type": "VARCHAR",
				},

				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
			},
			rows: [][]interface{}{
				[]interface{}{1, "1", "1"},
			},
		},
		{
			tdsn:  dsn,
			input: "select * from testapi where value = 1 or value = 2",
			columns: []map[string]string{
				map[string]string{
					"name": "value",
					"type": "INT",
				},
				map[string]string{
					"name": "value1",
					"type": "VARCHAR",
				},

				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
			},
			rows: [][]interface{}{
				[]interface{}{1, "1", "1"},
				[]interface{}{2, "2", "2"},
			},
		},
		{
			tdsn:  dsn,
			input: "select value2, value from testapi where value < 4 and value > 0",
			columns: []map[string]string{
				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
				map[string]string{
					"name": "value",
					"type": "INT",
				},
			},
			rows: [][]interface{}{
				[]interface{}{"1", 1},
				[]interface{}{"2", 2},
				[]interface{}{"3", 3},
			},
		},
		// with dsn2
		{
			tdsn:  dsn2,
			input: "select * from testapi where value = 1",
			columns: []map[string]string{
				map[string]string{
					"name": "value",
					"type": "INT",
				},
				map[string]string{
					"name": "value1",
					"type": "VARCHAR",
				},

				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
			},
			rows: [][]interface{}{
				[]interface{}{1, "1", "1"},
			},
		},
		{
			tdsn:  dsn2,
			input: "select * from testapi where value = 1 or value = 2",
			columns: []map[string]string{
				map[string]string{
					"name": "value",
					"type": "INT",
				},
				map[string]string{
					"name": "value1",
					"type": "VARCHAR",
				},

				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
			},
			rows: [][]interface{}{
				[]interface{}{1, "1", "1"},
				[]interface{}{2, "2", "2"},
			},
		},
		{
			tdsn:  dsn2,
			input: "select value2, value from testapi where value < 4 and value > 0",
			columns: []map[string]string{
				map[string]string{
					"name": "value2",
					"type": "VARCHAR",
				},
				map[string]string{
					"name": "value",
					"type": "INT",
				},
			},
			rows: [][]interface{}{
				[]interface{}{"1", 1},
				[]interface{}{"2", 2},
				[]interface{}{"3", 3},
			},
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			for i := 0; i < len(testCase); i++ {
				e := httpexpect.New(b, *mskserveraddr)

				// 获取 msid
				inargs := map[string]interface{}{
					"dsn": testCase[i].tdsn,
				}
				retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

				retData.Value("data").Object().ContainsKey("msid")

				uuid := retData.Value("data").Object().Value("msid").String().Raw()

				inargs = map[string]interface{}{
					"msid":     uuid,
					"sql":      testCase[i].input,
					"duration": 1000,
				}

				retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

				log.Printf("retData %v", retData)

				retData.Value("data").Object().Value("request").Object().Value("sql").String().Equal(testCase[i].input)

				// columns
				retData.Value("data").Object().Value("results").Object().Value("columns").Array().Equal(testCase[i].columns)

				// rows
				retData.Value("data").Object().Value("results").Object().Value("rows").Array().Equal(testCase[i].rows)
			}
		}
	})
}
