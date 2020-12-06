package httpsvr

import (
	// "github.com/stretchr/testify/assert"
	"log"
	"testing"

	"fmt"
	"github.com/gavv/httpexpect"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"math/rand"
	"net/http"
	"time"
)

func TestSwaggerHandlerSNormal(t *testing.T) {

	e := httpexpect.New(t, *mskserveraddr)

	inargs := map[string]interface{}{}

	e.GET("/").WithQueryObject(inargs).Expect().Status(http.StatusOK)
}

func TestPProfHandlerSNormal(t *testing.T) {

	e := httpexpect.New(t, *mskserveraddr)

	inargs := map[string]interface{}{}

	e.GET("/debug/pprof").WithQueryObject(inargs).Expect().Status(http.StatusOK)
}

func TestMySQLInitHandlerSNormal(t *testing.T) {

	runAPITests(t, dsn, 1, func(t *testing.T) {

		e := httpexpect.New(t, *mskserveraddr)

		inargs := map[string]interface{}{
			"dsn": dsn,
		}

		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")
	})
}

func TestMySQLInitHandlerSParamFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {

		e := httpexpect.New(t, *mskserveraddr)
		inargs := map[string]interface{}{}

		_apiFetchRetData("/v1/mysql/init", e, inargs, RetErrArgs)

		inargs = map[string]interface{}{
			"dsn": 323232,
		}

		_apiFetchRetData("/v1/mysql/init", e, inargs, RetErrFailToInitService)
	})
}

func TestMySQLInitTimeoutBadDBHandlerSParamFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {

		e := httpexpect.New(t, *mskserveraddr)
		inargs := map[string]interface{}{}

		inargs = map[string]interface{}{
			"dsn": "mskeeper_test:123456@tcp(111:3306)/mskeepertest?timeout=30s",
		}

		_apiFetchRetData("/v1/mysql/init", e, inargs, RetErrPingFailure)
	})
}

func TestMySQLInitTimeoutHandlerSParamFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {

		e := httpexpect.New(t, *mskserveraddr)
		inargs := map[string]interface{}{}

		inargs = map[string]interface{}{
			"dsn":     dsn,
			"timeout": 0.1, // 0.1 micros, should failed
		}

		_apiFetchRetData("/v1/mysql/init", e, inargs, RetErrPingFailure)
	})
}

func TestMySQLCheckTimeoutHandlerSParamFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)
		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()

		inargs = map[string]interface{}{
			"msid":    uuid,
			"sql":     "select * from testapi where value = ?",
			"args":    "[1]", // bad json format
			"timeout": 0.1,   // 0.1 micros, should failed
		}

		_apiFetchRetData("/v1/mysql/check", e, inargs, RetErrPingFailure)
	})
}

func TestMySQLRunTimeoutHandlerSParamFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)
		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()

		inargs = map[string]interface{}{
			"msid":    uuid,
			"sql":     "select * from testapi where value = ?",
			"args":    "[1]", // bad json format
			"timeout": 0.1,   // 0.1 micros, should failed
		}

		_apiFetchRetData("/v1/mysql/check", e, inargs, RetErrPingFailure)
	})
}

func TestMySQLCheckHandlerSArgsFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		// check sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi where value = ?",
			"args": "1]", // bad json format
		}

		_ = _apiFetchRetData("/v1/mysql/check", e, inargs, RetErrArgs)

		// check sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi where value = ?",
			"args": "1233", // bad json format
		}

		_ = _apiFetchRetData("/v1/mysql/check", e, inargs, RetErrArgs)

		// check sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi where value = ?",
			"args": "", // bad json format, skip
		}

		_ = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		// check sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi where value = ?",
		}

		_ = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)
	})
}

func TestMySQLRunHandlerSArgsFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		// run sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi where value = ?",
			"args": "1]", // bad json format
		}

		_ = _apiFetchRetData("/v1/mysql/run", e, inargs, RetErrArgs)

		// run sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi where value = ?",
			"args": "1233", // bad json format
		}

		_ = _apiFetchRetData("/v1/mysql/run", e, inargs, RetErrArgs)

		// run sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi",
			"args": "", // bad json format, skip
		}

		_ = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		// run sql
		inargs = map[string]interface{}{
			"msid": retData.Value("data").Object().Value("msid").String().Raw(),
			"sql":  "select * from testapi",
		}

		_ = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)
	})
}

func TestMySQLCheckHandlerSDuraFailure(t *testing.T) {
	runAPITests(t, dsn, 1001, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()

		// check sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value in (select value from testapi)",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		log.Printf("-----retData %v", retData)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeRowsInvolve)

		retData.Value("data").Object().Value("notifies").Array().Element(1).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeAllTableScan)
	})
}

func TestMySQLRunHandlerSDuraFailure(t *testing.T) {
	runAPITests(t, dsn, 1001, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()

		// run sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		// run sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		// run sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value in (select value from testapi)",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("-----retData %v", retData)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeRowsInvolve)

		retData.Value("data").Object().Value("notifies").Array().Element(1).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeAllTableScan)
	})
}

func TestMySQLCheckHandlerSMSIDFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		// uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// check sql
		inargs = map[string]interface{}{
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetErrFailToGetService)

		// check sql
		inargs = map[string]interface{}{
			"msid": "1212not exists",
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetErrFailToGetService)

	})
}

func TestMySQLRunHandlerSMSIDFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		// uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// check sql
		inargs = map[string]interface{}{
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetErrFailToGetService)

		// check sql
		inargs = map[string]interface{}{
			"msid": "1212not exists",
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetErrFailToGetService)

	})
}

func TestMySQLRunHandlerSQLFailure(t *testing.T) {
	runAPITests(t, dsn, 1001, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// run sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi111 where value = ?", // table not exists
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetErrRunFailure)

		// run sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where va3232lue = 32323?", // syntax failure
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetErrRunFailure)

	})
}

func TestMySQLCheckHandlerSQLFailure(t *testing.T) {
	runAPITests(t, dsn, 1001, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// check sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi111 where value = ?", // table not exists
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		// check sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where va3232lue = 32323?", // syntax failure
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

	})
}

func TestMySQLCheckHandlerSQLCheckFailure(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) {
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// uuid := retData.Value("data").Object().Value("msid").String().Raw()
		// check sql
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "explain select * from testapi111 where value = ?", // table not exists
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetErrCheckFailure)
	})
}

func TestMySQLCheckHandlerSNormal(t *testing.T) {
	runAPITests(t, dsn, 1100, func(t *testing.T) { // +100是为了兼容8.0的优化器 1100*0.1=110>100 of policy
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()

		// check sql 1
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		// check sql 2
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value1 = ?",
			"args": "[1]",
		}

		// log.Printf("111111111111111111111111111111111111111111113232323232323retData %v", retData)
		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		code1 := policy.PolicyCode(retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").Value("code").Number().Raw())

		// log.Printf("111111111111111111111111111111111111111111113232323232323retData %v", retData)

		// 由于8.0和5.6对于类型不匹配的优化成都不同，8.0的rowaffected是200，5.6时2000，所以此处满足任意一个即可
		if !(code1 == policy.ErrPolicyCodeRowsInvolve || code1 == policy.ErrPolicyCodeAllTableScan) {
			t.Fatalf("code(%v) not match", code1)
		}
		// check sql 3
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "update testapi set value2 = '123' where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeDataTruncate)

		// check sql 4
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  fmt.Sprintf("update testapi set value1 = '%v' where value = 1", getRandomString(60*0.8+1)), // little large than uplimit of overflow
		}

		retData = _apiFetchRetData("/v1/mysql/check", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.WarnPolicyCodeDataTruncate)
	})
}

func TestMySQLRunCRUD(t *testing.T) {
	runAPITests(t, dsn, 1, func(t *testing.T) { // +100是为了兼容8.0的优化器 1100*0.1=110>100 of policy

		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn": dsn,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()

		// Drop Table testapi if any
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "DROP TABLE IF EXISTS testapi",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		// Create Table
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "CREATE TABLE testapi (value BOOL)",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		// Test for unexpected data
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "SELECT * FROM testapi",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		// Create Data
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "INSERT INTO testapi VALUES (1)",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows_affected").ValueEqual("rows_affected", 1)

		// Read
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "SELECT value FROM testapi",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows").Value("rows").Array().Element(0).
			Array().Element(0).Number().Equal(1)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows").Value("rows").Array().Length().Equal(1)
		retData.Value("data").Object().Value("results").Object().ContainsKey("rows").Value("rows").Array().Element(0).
			Array().Length().Equal(1)

		// Update
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "UPDATE testapi SET value = ? WHERE value = ?",
			"args": "[false, true]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows_affected").ValueEqual("rows_affected", 1)

		// Check Update
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "SELECT value FROM testapi",
		}
		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("----------------retData %v", retData)

		// !no notifies since in silent period!

		// retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
		// 	ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows").Value("rows").Array().Element(0).
			Array().Element(0).Number().Equal(0)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows").Value("rows").Array().Length().Equal(1)
		retData.Value("data").Object().Value("results").Object().ContainsKey("rows").Value("rows").Array().Element(0).
			Array().Length().Equal(1)

		// Delete
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "DELETE FROM testapi WHERE value = ?",
			"args": "[false]",
		}
		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows_affected").ValueEqual("rows_affected", 1)

		// Check for unexpected rows

		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "DELETE FROM testapi",
		}
		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		retData.Value("data").Object().Value("results").Object().ContainsKey("rows_affected").ValueEqual("rows_affected", 0)
	})
}

func TestMySQLRunHandlerSNormal(t *testing.T) {
	runAPITests(t, dsn, 1100, func(t *testing.T) { // +100是为了兼容8.0的优化器 1100*0.1=110>100 of policy
		e := httpexpect.New(t, *mskserveraddr)

		// 获取 msid
		inargs := map[string]interface{}{
			"dsn":      dsn,
			"duration": 1000,
		}
		retData := _apiFetchRetData("/v1/mysql/init", e, inargs, RetSuccess)

		retData.Value("data").Object().ContainsKey("msid")

		uuid := retData.Value("data").Object().Value("msid").String().Raw()

		// run sql 1
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeSafe)

		// run sql 2
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select * from testapi where value1 = ?",
			"args": "[1]",
		}

		// log.Printf("111111111111111111111111111111111111111111113232323232323retData %v", retData)
		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		code1 := policy.PolicyCode(retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").Value("code").Number().Raw())

		// log.Printf("111111111111111111111111111111111111111111113232323232323retData %v", retData)

		// 由于8.0和5.6对于类型不匹配的优化成都不同，8.0的rowaffected是200，5.6时2000，所以此处满足任意一个即可
		if !(code1 == policy.ErrPolicyCodeRowsInvolve || code1 == policy.ErrPolicyCodeAllTableScan) {
			t.Fatalf("code(%v) not match", code1)
		}
		// run sql 3
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "update testapi set value2 = '123' where value = ?",
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeDataTruncate)

		// run sql 4
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  fmt.Sprintf("update testapi set value1 = '%v' where value = 1", getRandomString(60*0.8+1)), // little large than uplimit of overflow
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.WarnPolicyCodeDataTruncate)

		// run sql 5
		inargs = map[string]interface{}{
			"msid": uuid,
			"sql":  "select value, sleep(2) from testapi where value = ?", // sleep(1.01s)                                                // 1s
			"args": "[1]",
		}

		retData = _apiFetchRetData("/v1/mysql/run", e, inargs, RetSuccess)

		log.Printf("retData %v", retData)
		retData.Value("data").Object().Value("notifies").Array().Element(0).Object().
			ContainsKey("code").ValueEqual("code", policy.ErrPolicyCodeExeCost)

	})
}

func _apiFetchRetData(url string, e *httpexpect.Expect, args map[string]interface{}, expectRet int) *httpexpect.Object {

	retData := e.POST(url).WithQueryObject(args).Expect().JSON().Object()

	log.Printf("_apiFetchRetData: retData %v", retData)

	retData.ContainsKey("ret").ValueEqual("ret", expectRet)

	log.Printf("_apiFetchRetData(%v, %v): retData %v", url, args, retData)
	return retData
}

func getRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

// func _apiMySQLCheck(e *httpexpect.Expect, args map[string]interface{}, ret int) bool {

// 	retData := e.POST("/v1/mysql/check").WithQueryObject(args).Expect().JSON().Object()
// 	log.Printf("_apiMySQLCheck: retData %v", retData)

// 	r := retData.ContainsKey("ret").Value("ret").Number().Raw()

// 	return int(r) == ret
// }
