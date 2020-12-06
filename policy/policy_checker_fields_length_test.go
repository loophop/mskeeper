package policy

import (
	"database/sql"
	"fmt"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/sqlparser"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestRawPolicyFieldsLengthOpening(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("DROP TABLE IF EXISTS test")

		dbt.mustExec("CREATE TABLE `test` (`value` varchar(2) DEFAULT NULL,`value2` varchar(26) DEFAULT NULL,`value1` int(11) unsigned DEFAULT NULL,`value3` mediumtext,`value4` blob, KEY `value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test VALUES (?, ?, ?, ?, ?)", i, i, i, i, i)
		}

		dbt.mustExec("CREATE TABLE `test_policy` (`value` varchar(2) DEFAULT NULL,`value2` varchar(26) DEFAULT NULL,`value1` int(11) unsigned DEFAULT NULL,`value3` mediumtext,`value4` blob, KEY `value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?, ?, ?, ?)", i, i, i, i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		// var godamnv int = 5
		// err := npc.Check("update test_policy set value1 = 1234", godamnv)

		err := npc.Check("INSERT INTO test_policy(value, value1) VALUES ('123', 123123)")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}
		// godamnv = 1234
		// err = npc.Check("update test_policy set value1 = ?", godamnv)
		godamnv := 41122
		// err := npc.Check("INSERT INTO test_policy(value, value1) VALUES (11111, ?)", godamnv)
		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (?, 222)", "123")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (?, 222)", "12")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (2147483649, ?)", "abcabcabc")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy(value, value2) VALUES (?, 'abcdefghijklmnopqrstuvwxyz111')", godamnv)
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// for insert selet case
		err = npc.Check("INSERT INTO test_policy(value, value1) (select value, value1 from test_policy limit 2);")
		if err != nil {
			dbt.Errorf("should be ok since insert from select, ignore")
		}

		// For Update select case
		err = npc.Check("UPDATE test_policy, (select * from test_policy) as subtest set test_policy.value = '1' where test_policy.value1 = subtest.value1")
		if err != nil {
			dbt.Errorf("should be ok since update from select, ignore")
		}

		// For Update (table)
		err = npc.Check("UPDATE (test_policy) set value = '123'")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		// For Update (table)
		err = npc.Check("UPDATE (((test_policy))) set value = '123'")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		// For Update (table1, table2)
		err = npc.Check("UPDATE (test, test_policy) set test_policy.value = '123'")
		if err != nil {
			dbt.Errorf("should be ok since Update (table1, table2), ignore")
		}

		// For Update ((table1, table2))
		err = npc.Check("UPDATE ((test, test_policy)) set test_policy.value = '123'")
		if err != nil {
			dbt.Errorf("should be ok since Update (table1, table2), ignore")
		}

		// For Update (table1, table2)
		err = npc.Check("UPDATE (test_policy), (test) set test_policy.value = '1', test.value='123'")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}
		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		// For Update table join table
		err = npc.Check("update test_policy join test on test_policy.value1 = test.value1 set test_policy.value = '123';")
		if err != nil {
			dbt.Errorf("should be ok since update table join table, ignore")
		}

		// For Update (table join table)
		err = npc.Check("update (test_policy join test on test_policy.value1 = test.value1) set test_policy.value = '123';")
		if err != nil {
			dbt.Errorf("should be ok since Update (table join table), ignore")
		}

		dbt.mustExec("DROP TABLE IF EXISTS test_policy")

		dbt.mustExec("DROP TABLE IF EXISTS test")

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthMultiplyLinesOfInsert(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)
		logmsk.MSKLog().SetLevel(logmsk.DebugLevel)

		dbt.mustExec("CREATE TABLE `test_policy` (`value` varchar(2) DEFAULT NULL,`value2` varchar(26) DEFAULT NULL,`value1` int(11) unsigned DEFAULT NULL,`value3` mediumtext,`value4` blob, KEY `value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?, ?, ?, ?)", i, i, i, i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		// var godamnv int = 5
		// err := npc.Check("update test_policy set value1 = 1234", godamnv)

		err := npc.Check("INSERT INTO test_policy(value, value1) VALUES ('1', 123123),('123', 123123)")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Error(err)
		}

		godamnv := 41122
		// err := npc.Check("INSERT INTO test_policy(value, value1) VALUES (11111, ?)", godamnv)
		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (?, 222),(?, 222)", "1", "123")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (1, ?), (2147483649, ?)",
			"abcabcabc", "abcabcabc")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("INSERT INTO test_policy(value, value2) VALUES (?, 'abcdefghijklmnopqrstuvwxyz111'),(?, 'abcdefghijklmnopqrstuvwxyz111')",
			"1", godamnv)
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

// // MySQL 二进制表示：  X'31313131' = blob(1111)
// func TestRawPolicyFieldsLengthBin(t *testing.T) {
// 	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

// 		logmsk.MSKLog().SetOutput(os.Stdout)

// 		dbt.mustExec("CREATE TABLE `test_policy` (`value` bit(32) DEFAULT NULL, `value1` bit(4) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
// 		for i := 0; i < 100; i++ {
// 			dbt.mustExec("INSERT INTO test_policy VALUES (?,?)", i, i)
// 		}
// 		// X'776566776566776566776566776566776566'
// 		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
// 		// var godamnv int = 5
// 		// err := npc.Check("update test_policy set value1 = 1234", godamnv)
// 		err := npc.Check("INSERT INTO test_policy(value, value1) VALUES (123333, 15)")
// 		if err != nil {
// 			dbt.Error(err)
// 		}
// 		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (123333, ?)", 15)
// 		if err != nil {
// 			dbt.Error(err)
// 		}
// 		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (123333, ?)", 16)
// 		if err == nil {
// 			dbt.Errorf("should be failed since truncated!!!")
// 		}
// 		err = npc.Check("INSERT INTO test_policy(value, value1) VALUES (?, ?)", 4294967296, 1)
// 		if err == nil {
// 			dbt.Errorf("should be failed since truncated!!!")
// 		}
// 		err = npc.Check("DROP TABLE IF EXISTS test_policy")
// 		if err != nil {
// 			dbt.Errorf("no sql can be check")
// 		}

// 		logmsk.MSKLog().SetOutput(ioutil.Discard)
// 	})
// }

func GetRandomString(l int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyz"
	bytes := []byte(str)
	result := []byte{}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < l; i++ {
		result = append(result, bytes[r.Intn(len(bytes))])
	}
	return string(result)
}

func TestRawPolicyFieldsLengthBlob(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value` blob,`value1` tinyblob,`value2` mediumblob,`value3` longblob ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		// var godamnv int = 5
		// err := npc.Check("update test_policy set value1 = 1234", godamnv)
		err := npc.Check("INSERT INTO test_policy VALUES (X'31313131', '31313131',B'1111',123)")
		if err != nil {
			dbt.Error(err)
		}
		// Blob
		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value) VALUES ('%v')", GetRandomString(int(65535*DataTruncationUplimit))))
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check(fmt.Sprintf("update test_policy set value = '%v')", GetRandomString(int(65535*DataTruncationUplimit))))
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value) VALUES ('%v')", GetRandomString(int(65535*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check(fmt.Sprintf("update test_policy set value = '%v'", GetRandomString(int(65535*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy(value) VALUES (?)", GetRandomString(int(65535*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("update test_policy set value = ? where 1", GetRandomString(int(65535*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// TinyBlob
		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value1) VALUES ('%v')", GetRandomString(int(255*DataTruncationUplimit))))
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value1 = '%v'", GetRandomString(int(255*DataTruncationUplimit))))
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value1) VALUES ('%v')", GetRandomString(int(255*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value1 = '%v'", GetRandomString(int(255*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy(value1) VALUES (?)", GetRandomString(int(255*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy set value1 =?", GetRandomString(int(255*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// MediumnBlob
		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value2) VALUES ('%v')", GetRandomString(int(16777215*DataTruncationUplimit))))
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value2 = '%v'", GetRandomString(int(16777215*DataTruncationUplimit))))
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value2) VALUES ('%v')", GetRandomString(int(16777215*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value2 = '%v'", GetRandomString(int(16777215*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy(value2) VALUES (?)", GetRandomString(int(16777215*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy set value2 = ?", GetRandomString(int(16777215*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// LongBlob
		// sorry for LongBlob, 4GB data needed
		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthText(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value` TEXT,`value1` tinytext,`value2` mediumtext,`value3` longtext ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		// var godamnv int = 5
		// err := npc.Check("update test_policy set value1 = 1234", godamnv)
		err := npc.Check("INSERT INTO test_policy VALUES (X'31313131', '31313131',B'1111',123)")
		if err != nil {
			dbt.Fatal(err)
		}
		// TEXT
		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value, value2) VALUES ('%v', '%v')", GetRandomString(int(65535*DataTruncationUplimit)), GetRandomString(int(65535*DataTruncationUplimit))))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value = '%v', value2 = '%v'", GetRandomString(int(65535*DataTruncationUplimit)), GetRandomString(int(65535*DataTruncationUplimit))))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value) VALUES ('%v')", GetRandomString(int(65535*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy SET value = '%v'", GetRandomString(int(65535*DataTruncationUplimit+1))))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy(value) VALUES (?)", GetRandomString(int(65535*DataTruncationUplimit+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy set value = ?", GetRandomString(int(65535*DataTruncationUplimit+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// TinyTEXT
		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value1) VALUES ('%v')", GetRandomString(int(255*DataTruncationUplimit))))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy SET value1 = '%v'", GetRandomString(int(255*DataTruncationUplimit))))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value1) VALUES ('%v')", GetRandomString(int(255*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value1 = '%v'", GetRandomString(int(255*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy(value1) VALUES (?)", GetRandomString(int(255*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy set value1 = ?", GetRandomString(int(255*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// MediumnTEXT
		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value2) VALUES ('%v')", GetRandomString(int(16777215*DataTruncationUplimit))))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value2 = '%v'", GetRandomString(int(16777215*DataTruncationUplimit))))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check(fmt.Sprintf("INSERT INTO test_policy(value2) VALUES ('%v')", GetRandomString(int(16777215*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check(fmt.Sprintf("UPDATE test_policy set value2 = '%v'", GetRandomString(int(16777215*DataTruncationUplimit)+1)))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy(value2) VALUES (?)", GetRandomString(int(16777215*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy set value2 = ?", GetRandomString(int(16777215*DataTruncationUplimit)+1))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// LongTEXT
		// sorry for LongTEXT, 4GB data needed
		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthInt(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value1` tinyint(4) DEFAULT NULL,`value1unsigned` tinyint(11) unsigned DEFAULT NULL,`value2` smallint(6) DEFAULT NULL,`value2unsigned` smallint(6) unsigned DEFAULT NULL,`value3` mediumint(9) DEFAULT NULL,`value3unsigned` mediumint(9) unsigned DEFAULT NULL,`value` int(11) DEFAULT NULL,`valueunsigned` int(11) unsigned DEFAULT NULL,`value4` bigint(11) DEFAULT NULL,`value4unsigned` bigint(20) unsigned DEFAULT NULL,KEY `value1` (`valueunsigned`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		// Normal case
		err := npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check("UPDATE test_policy set value1 = -128, value1unsigned = 255, value2=-32768, value2unsigned = 65535, value3=-8388608, value3unsigned=16777215, value = ?, valueunsigned = ?, value4=?, value4unsigned=?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check("INSERT INTO test_policy VALUES (1, 1, 1, 1, 1, 1, 1, 1, 1, 1)")
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check("UPDATE test_policy SET value1 = 1, value1unsigned =1, value2=1, value2unsigned = 1, value3=1, value3unsigned=1, value = 1, valueunsigned = 1, value4=1, value4unsigned=1")
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check("INSERT INTO test_policy VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)")
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check("UPDATE test_policy SET value1 = 0, value1unsigned =0, value2=0, value2unsigned = 0, value3=0, value3unsigned=0, value = 0, valueunsigned = 0, value4=0, value4unsigned=0")
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check("INSERT INTO test_policy VALUES (-1, 1, -1, 1, -1, 1, -1, 1, -1, 1)")
		if err != nil {
			dbt.Fatal(err)
		}

		err = npc.Check("UPDATE test_policy SET value1 = -1, value1unsigned =1, value2=-1, value2unsigned = 1, value3=-1, value3unsigned=1, value = -1, valueunsigned = 1, value4=-1, value4unsigned=1")
		if err != nil {
			dbt.Fatal(err)
		}

		// TinyInt signed
		err = npc.Check("INSERT INTO test_policy VALUES (-129, 255, -32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -129, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Fatalf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy VALUES (128, 255, -32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = 128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// TinyInt unsigned
		err = npc.Check("INSERT INTO test_policy VALUES (-129, 255, -32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -129, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Fatalf("should be failed since truncated!!!")
		}

		// ShortInt signed
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32769, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32769, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, 32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// ShortInt unsigned
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65536, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65536, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Fatalf("should be failed since truncated!!!")
		}

		// MediumnInt signed
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388609, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388609, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, 8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// MediumnInt unsigned
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777216, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777216, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// Int signed
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32-1, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32-1, math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			-(math.MinInt32), math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			-(math.MinInt32), math.MaxUint32, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// Case inspired from nnsecure, @machine
		err = npc.Check("UPDATE test_policy SET value = 1|-1")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// Int unsigned
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777215, ?, ?, ?, ?)",
			math.MinInt32, math.MaxUint32+1, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=?, value4unsigned=?",
			math.MinInt32, math.MaxUint32+1, int64(math.MinInt64), uint64(math.MaxUint64))
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// BigInt signed
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777215, ?, ?, -9223372036854775809, 18446744073709551615)",
			math.MinInt32, math.MaxUint32)
		if err == nil {
			dbt.Fatalf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=-9223372036854775809, value4unsigned=18446744073709551615",
			math.MinInt32, math.MaxUint32)
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777215, ?, ?, 9223372036854775808, 18446744073709551615)",
			math.MinInt32, math.MaxUint32)
		if err == nil {
			dbt.Fatalf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=9223372036854775808, value4unsigned=18446744073709551615",
			math.MinInt32, math.MaxUint32)
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		// BigInt unsigned
		err = npc.Check("INSERT INTO test_policy VALUES (-128, 255, -32768, 65535, -8388608, 16777215, ?, ?, 9223372036854775807, 18446744073709551616)",
			math.MinInt32, math.MaxUint32)
		if err == nil {
			dbt.Fatalf("should be failed since truncated!!!")
		}

		err = npc.Check("UPDATE test_policy SET value1 = -128, value1unsigned =255, value2=-32768, value2unsigned =65535, value3=-8388608, value3unsigned=16777215, value =?, valueunsigned =?, value4=9223372036854775807, value4unsigned=18446744073709551616",
			math.MinInt32, math.MaxUint32)
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

type SS []string

func TestCheckValueLengthBy(t *testing.T) {
	pcfl := NewPolicyCheckerFieldsLength()

	columnSlice := []string{"field1", "field2", "field3", "field4"}
	valueSlice := []*sqlparser.SQLVal{
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewIntVal([]byte("255")),
		sqlparser.NewStrVal([]byte("1"))}

	var columnTypeValueMap ColumnMap = make(ColumnMap)
	columnTypeValueMap["field1"] =
		&ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "int(11)", Valid: true}}

	columnTypeValueMap["field2"] =
		&ColumnRecord{Field: sql.NullString{String: "field2", Valid: true}, Type: sql.NullString{String: "tinyint(10)", Valid: true}}

	columnTypeValueMap["field3"] =
		&ColumnRecord{Field: sql.NullString{String: "field2", Valid: true}, Type: sql.NullString{String: "varchar(10)", Valid: true}}

	args := []interface{}{"1", "2", "3"}

	var argIdx int
	argIdx = 0
	err := pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args, &argIdx)
	log.Printf("err %v", err)
	if err == nil {
		t.Fatalf("checkValueLengthBy (  args0[0].([]interface{}) ) should fail")
	}

	argIdx = 0
	args1 := []string{"1", "2", "3"}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args1, &argIdx)
	log.Printf("err %v", err)
	if err == nil {
		t.Fatalf("checkValueLengthBy ( args1.([]interface{}) ) should fail")
	}

	valueSlice = []*sqlparser.SQLVal{
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewIntVal([]byte("255")),
		sqlparser.NewValArg([]byte("11"))}

	argIdx = 0
	args2 := []interface{}{"1", "2", "3"}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)
	if err == nil {
		t.Fatalf("checkValueLengthBy ( len(argsSlice) 3 != argCnt 1  ) should fail")
	}

	valueSlice = []*sqlparser.SQLVal{
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewValArg([]byte("11"))}

	argIdx = 0
	args2 = []interface{}{"1"}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)
	if err == nil {
		t.Fatalf("checkValueLengthBy (  len(columnSlice) 3 != len(valueSlice) 2  ) should fail")
	}

	valueSlice = []*sqlparser.SQLVal{
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewIntVal([]byte("128")),
		sqlparser.NewValArg([]byte("11"))}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)

	argIdx = 0
	valueSlice = []*sqlparser.SQLVal{
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewIntVal([]byte("128")),
		sqlparser.NewValArg([]byte("11")),
		sqlparser.NewIntVal([]byte("128"))}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)

	// "128" of field2 is larger than tinyint's max signed int 127
	if err != ErrFieldDataTruncated {
		t.Fatalf("checkValueLengthBy ( field2 's data should be truncated ) should fail")
	}

	argIdx = 0
	valueSlice = []*sqlparser.SQLVal{
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewValArg([]byte("11")),
		sqlparser.NewValArg([]byte("11"))}

	args2 = []interface{}{"11"}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)
	//  len(argsSlice) < argCnt
	if err == nil {
		t.Fatalf("checkValueLengthBy ( field2 's data should be truncated ) should fail")
	}

	// test continue branch
	/*
	   columnRecord, ok := columnTypeValueMap[columnSlice[i]]
	   if !ok {
	           continue
	   }
	*/
	argIdx = 0
	valueSlice = []*sqlparser.SQLVal{
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewIntVal([]byte("123")),
		sqlparser.NewValArg([]byte("11")),
		sqlparser.NewIntVal([]byte("123"))}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)

	log.Printf("err = 1111 %v", err)
	if err != nil {
		t.Fatalf("should be ok %v", err)
	}

	argIdx = 0
	valueSlice = []*sqlparser.SQLVal{
		sqlparser.NewStrVal([]byte("111")),
		sqlparser.NewIntVal([]byte("127")),
		sqlparser.NewStrVal([]byte("123456789")), // defaultly 0.8
		sqlparser.NewIntVal([]byte("123"))}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)

	if err != WarnFieldDataMayTruncated {
		t.Fatalf("checkValueLengthBy ( field3 's data should be truncated ) should fail")
	}

	argIdx = 0
	valueSlice = []*sqlparser.SQLVal{
		nil,
		sqlparser.NewIntVal([]byte("127")),
		sqlparser.NewStrVal([]byte("12345678")),
		sqlparser.NewIntVal([]byte("123"))}
	err = pcfl.checkValueLengthBy(columnSlice, valueSlice, columnTypeValueMap, args2, &argIdx)
	log.Printf("err %v", err)

	if err != nil {
		t.Fatalf("should be ok %v", err)
	}
}

func TestCheckIfMySQLTruncate(t *testing.T) {
	// func checkIfMySQLTruncate(cr *ColumnRecord, sqlV *sqlparser.SQLVal, value []byte) bool
	pcfl := NewPolicyCheckerFieldsLength()

	cr := &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "int(11)", Valid: true}}
	sqlV := sqlparser.NewIntVal([]byte("123"))
	value := []byte("123")

	truncation := pcfl.checkIfMySQLTruncate(cr, sqlV, value)
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "tinyint(11)", Valid: true}}
	sqlV = sqlparser.NewIntVal([]byte("1")) // meaningless
	value = []byte("256")                   // truncated
	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)
	if truncation == NoTruncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "tinyint((a)", Valid: true}}
	sqlV = sqlparser.NewIntVal([]byte("1")) // meaningless
	value = []byte("256")
	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)

	// bad hex val
	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "varchar(11)", Valid: true}}
	sqlV = sqlparser.NewHexVal([]byte("!!@@@@@"))
	value = []byte("123")

	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	// unsupported field type GEOMETRY
	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "geometry", Valid: true}}
	sqlV = sqlparser.NewIntVal([]byte("1")) // meaningless
	value = []byte("1.2.3.4")

	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	// no truncation because of bad format of columnrecord 1
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "tinyint*8389@@@)", Valid: true}}
	sqlV = sqlparser.NewIntVal([]byte("1")) // meaningless
	value = []byte("256")
	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)

	// no truncation because of bad format of columnrecord 2
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	// bad format for ParseInt
	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "tinyint(10)", Valid: true}}
	sqlV = sqlparser.NewIntVal([]byte("1")) // meaningless
	value = []byte("-25a")
	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)

	// unknown type, no truncation
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	// bad format for ParseUint
	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "tinyint(10)", Valid: true}}
	sqlV = sqlparser.NewIntVal([]byte("1")) // meaningless
	value = []byte("25a")
	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)

	// unknown type, no truncation
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	// nil SQLVAl
	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "tinyint(10)", Valid: true}}
	value = []byte("25a")
	truncation = pcfl.checkIfMySQLTruncate(cr, nil, value)

	// nil SQLVal, no truncation
	if truncation == Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}

	// unsigned truncation path
	cr = &ColumnRecord{Field: sql.NullString{String: "field1", Valid: true}, Type: sql.NullString{String: "tinyint(11) unsigned", Valid: true}}
	sqlV = sqlparser.NewIntVal([]byte("1")) // meaningless
	value = []byte("256")                   // truncated
	truncation = pcfl.checkIfMySQLTruncate(cr, sqlV, value)
	if truncation != Truncated {
		t.Fatalf("checkIfMySQLTruncate(%v, %v, %v) failed", cr, sqlV, value)
	}
}

func TestRawPolicyFieldsLengthBinary(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value` varbinary(10) DEFAULT NULL,`value1` binary(20) DEFAULT NULL,`value2` binary(255) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		// var godamnv int = 5
		// err := npc.Check("update test_policy set value1 = 1234", godamnv)

		// Normal case
		err := npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'3132333435363738', X'31323334353637383930313233343536', X'333333');")
		if err != nil {
			dbt.Error(err)
		}

		// value1 will be truncated， 20 * DataTruncationUplimit(0.8) = 16
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'313131', X'3132333435363738393031323334353637', X'333333');")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ := err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		// value will be truncated， 10 * DataTruncationUplimit(0.8) = 8
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'313233343536373839', X'3132333435363738393031323334353637383930', X'333333');")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthChar(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value` varchar(10) DEFAULT NULL,`value1` char(20) DEFAULT NULL,`value2` char(255) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		// var godamnv int = 5
		// err := npc.Check("update test_policy set value1 = 1234", godamnv)

		// Normal case
		err := npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'3132333435363738', X'31323334353637383930313233343536', X'333333');")
		if err != nil {
			dbt.Error(err)
		}

		// value1 will be truncated， 20 * DataTruncationUplimit(0.8) = 16
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'313131', X'3132333435363738393031323334353637', X'333333');")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ := err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		// value1 will be truncated， 20 * DataTruncationUplimit(0.8) = 16
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES ('111', '12345678901234567', X'333333');")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		// value will be truncated， 10 * DataTruncationUplimit(0.8) = 8
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'313233343536373839', X'3132333435363738393031323334353637383930', X'333333');")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		// value will be truncated， 10 * DataTruncationUplimit(0.8) = 8
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES ('123456789', X'3132333435363738393031323334353637383930', X'333333');")
		if err == nil {
			dbt.Errorf("should be failed since truncated!!!")
		}

		log.Printf("err ==== %v", err)
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthDataTruncUplimit(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value` varchar(10) DEFAULT NULL,`value1` char(20) DEFAULT NULL,`value2` char(255) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		// normal
		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		err := npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'3132333435363738', X'31323334353637383930313233343536', X'333333');")
		if err != nil {
			dbt.Error(err)
		}

		// bad param, use default value 0.8
		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsLength("abc"), dbt.db)
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'3132333435363738', X'31323334353637383930313233343536', X'333333');")
		if err != nil {
			dbt.Error(err)
		}

		// bad param, use default value 0.8
		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsLength("a"), dbt.db)
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'3132333435363738', X'31323334353637383930313233343536', X'333333');")
		if err != nil {
			dbt.Error(err)
		}

		// bad param, use default value 0.8
		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(1.1, 32323), dbt.db)
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'3132333435363738', X'31323334353637383930313233343536', X'333333');")
		if err != nil {
			dbt.Error(err)
		}

		// defautly dataTruncateUplimit = 0.8
		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'313233343536373839', X'31323334353637383930313233343536', X'333333');")
		pe, _ := err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Error(err)
		}

		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(0.5), dbt.db)
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`, `value2`) VALUES (X'313233343536', X'31323334353637383930313233343536', X'333333');")
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Error(err)
		}

		// 调高了，0.6*10 = 6 >= len(X'313233343536')
		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(0.6), dbt.db)
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`) VALUES (X'31323334353637', 212);")
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Error(err)
		}

		// 调高了，(0.6+0.1)*10 = 7 < len(X'3132333435363738')
		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(0.7), dbt.db)
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`) VALUES (X'3132333435363738', 212);")
		pe, _ = err.(*PolicyError)
		if pe.Code != WarnPolicyCodeDataTruncate {
			dbt.Error(err)
		}

		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`) VALUES (X'3132333435363738393031', 212);")
		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Error(err)
		}

		// should be ok 0.7 <= 0.7
		err = npc.Check("INSERT INTO `test_policy` (`value`, `value1`) VALUES (X'31323334353637', 212);")
		if err != nil {
			dbt.Error(err)
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthFromNNSecure(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)
		logmsk.MSKLog().SetLevel(logmsk.DebugLevel)
		dbt.mustExec("DROP TABLE IF EXISTS inform_report_record")
		dbt.mustExec("DROP TABLE IF EXISTS user_report_record")
		dbt.mustExec("DROP TABLE IF EXISTS idfa_blacklist")

		dbt.mustExec("CREATE TABLE `idfa_blacklist` (" +
			"`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
			"`clientid` varchar(16) NOT NULL DEFAULT '1001'," +
			"`system` varchar(20) NOT NULL DEFAULT '0' COMMENT 'ios/android'," +
			"`zoneid` int(11) NOT NULL DEFAULT '0'," +
			"`idfa` varchar(128) NOT NULL DEFAULT '' COMMENT 'androidid or idfa'," +
			"`banned` int(11) NOT NULL DEFAULT '0' COMMENT '拦截次数'," +
			"`creator` varchar(10) NOT NULL DEFAULT '' COMMENT '创建者'," +
			"`mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
			"`ctime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'," +
			"PRIMARY KEY (`id`)," +
			"UNIQUE KEY `uniq_idx_idfa` (`idfa`)," +
			"KEY `idx_ctime` (`ctime`)," +
			"KEY `idx_creator` (`creator`)," +
			"KEY `idx_idfa` (`idfa`)," +
			"KEY `id_app` (`clientid`,`system`,`zoneid`)" +
			") ENGINE=InnoDB AUTO_INCREMENT=13659635 DEFAULT CHARSET=utf8;")

		dbt.mustExec("CREATE TABLE `inform_report_record` (" +
			"`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '唯一id'," +
			"`client_id` varchar(64) NOT NULL DEFAULT '1019' COMMENT '游戏id'," +
			"`role_id` varchar(128) NOT NULL COMMENT '举报人id'," +
			"`role_name` varchar(128) NOT NULL DEFAULT '' COMMENT '举报人角色名'," +
			"`target_id` varchar(128) NOT NULL COMMENT '被举报id'," +
			"`target_name` varchar(128) NOT NULL DEFAULT '' COMMENT '被举报角色名'," +
			"`status` int(11) NOT NULL DEFAULT '0' COMMENT '处理状态'," +
			"`is_ignore` int(11) NOT NULL DEFAULT '0' COMMENT '是否无效'," +
			"`message` varchar(128) NOT NULL DEFAULT '' COMMENT '被举报发言'," +
			"`url` text NOT NULL, " +
			"`reason` int(11) NOT NULL DEFAULT '-1' COMMENT '举报原因'," +
			"`extra` varchar(1024) NOT NULL COMMENT '其他信息'," +
			"`mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			"`ctime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'," +
			"UNIQUE KEY `id` (`id`)," +
			"KEY `idx_clientid_roleid` (`client_id`,`role_id`)," +
			"KEY `idx_clientid_targetid` (`client_id`,`target_id`)," +
			"KEY `idx_clientid_reason` (`client_id`,`reason`)," +
			"KEY `idx_clientid_status` (`client_id`,`status`)" +
			") ENGINE=InnoDB AUTO_INCREMENT=3434 DEFAULT CHARSET=utf8mb4;")

		dbt.mustExec("CREATE TABLE `user_report_record` (" +
			"`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '唯一id'," +
			"`client_id` varchar(64) NOT NULL DEFAULT '1019' COMMENT '游戏id'," +
			"`role_id` varchar(128) NOT NULL COMMENT '角色id'," +
			"`role_name` varchar(128) NOT NULL COMMENT '角色名'," +
			"`credit` double NOT NULL DEFAULT '100'," +
			"`extra` varchar(1024) NOT NULL COMMENT '其他信息'," +
			"`mtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			"`ctime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'," +
			"UNIQUE KEY `id` (`id`)," +
			"UNIQUE KEY `idx_clientid_roleid` (`client_id`,`role_id`)," +
			"KEY `idx_clientid_credit` (`client_id`,`credit`)," +
			"KEY `idx_clientid_mtime` (`client_id`,`mtime`)" +
			") ENGINE=InnoDB AUTO_INCREMENT=180 DEFAULT CHARSET=utf8mb4;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)
		err := npc.Check("UPDATE inform_report_record SET status = ? WHERE client_id = ? AND status = ? AND target_id IN (SELECT role_id FROM user_report_record WHERE client_id = ? AND credit >= ? AND credit < 100 AND ctime >= ?)",
			2, 1012, 0, 1012, 90, time.Now())
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check("UPDATE inform_report_record SET status = ? WHERE client_id = ? AND status = ? AND target_id IN (SELECT role_id FROM user_report_record WHERE client_id = ? AND credit >= ? AND credit < 100 AND ctime >= ?)",
			2, 1012, 0, 1012, 90, time.Now())
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check("INSERT INTO idfa_blacklist (clientid, system, zoneid, idfa, creator, ctime) VALUES (?, ?, ?, ?, ?, now())",
			1012, "ios", 0, "LlSbhoIVAvniwbQrSP2uGArnrYASsLVB", "nnsecure")
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check("UPDATE idfa_blacklist SET banned = banned+1 WHERE idfa = ?",
			"LlSbhoIVAvniwbQrSP2uGArnrYASsLVB")
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check("INSERT INTO inform_report_record(client_id, role_id, role_name, target_id, target_name, status, message, url, reason, extra, mtime, ctime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())",
			"1012", 123456, "name", 654321, "eman", 0, "", "", 0, "")
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check("UPDATE audit_object SET status = status | ?, result = ? WHERE task_id = ?",
			"1012", 123456, "name", 654321, "eman", 0, "", "", 0, "")
		if err != nil {
			dbt.Error(err)
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthUpdateExpr(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value3` tinyint(11) DEFAULT NULL,`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		dbt.mustExec("DROP TABLE IF EXISTS test")

		dbt.mustExec("CREATE TABLE `test` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)

		err := npc.Check("update test_policy set value2 = !(123|?) where value1 = 1", 3)
		if err != nil {
			dbt.Error(err)
		}

		err = npc.Check("update test_policy set value2 = (123|1234) where value1 = 1")
		if err == nil {
			dbt.Error(err)
		}

		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("update test_policy set value = 1|-1 where value1 = 1")
		if err == nil {
			dbt.Error(err)
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("update test_policy set value3=-1|1")
		if err == nil {
			dbt.Error(err)
		}

		err = npc.Check("update test_policy set value3=?|?", -1, 1)
		if err == nil {
			dbt.Error(err)
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("update test_policy set value3=0xff")
		if err == nil {
			dbt.Error(err)
		}

		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeDataTruncate {
			dbt.Errorf("code check of Data Truncation not covered")
		}

		err = npc.Check("update test_policy set value3=?+2+?+!4", 1, 3)
		if err != nil {
			dbt.Fatal(err)
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthBadInsertSQL(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value3` tinyint(11) DEFAULT NULL,`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		dbt.mustExec("DROP TABLE IF EXISTS test")

		dbt.mustExec("CREATE TABLE `test` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)

		err := npc.Check("INSERT INTO `test_policy_none` VALUES(?,?)", 1, 2)
		// should be failed in show columns
		if err != nil {
			dbt.Fatal(err)
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthBadInsertBadValues(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value3` tinyint(11) DEFAULT NULL,`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		dbt.mustExec("DROP TABLE IF EXISTS test")

		dbt.mustExec("CREATE TABLE `test` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)

		err := npc.Check("INSERT INTO `test_policy` VALUES(?,?,?,:eeee:)", 1, 2, 3)
		// should be failed in show columns
		if err != nil {
			dbt.Fatal(err)
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthBadUpdateSQL(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value3` tinyint(11) DEFAULT NULL,`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		dbt.mustExec("DROP TABLE IF EXISTS test")

		dbt.mustExec("CREATE TABLE `test` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)

		err := npc.Check("UPDATE `test_policy` SET value = ?")
		// should be failed in show columns
		if err != nil {
			dbt.Fatal(err)
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyFieldsLengthBadUpdate2SQL(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value3` tinyint(11) DEFAULT NULL,`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		dbt.mustExec("DROP TABLE IF EXISTS test")

		dbt.mustExec("CREATE TABLE `test` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,`value2` varchar(2) DEFAULT '') ENGINE=InnoDB DEFAULT CHARSET=utf8;")

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsLength(), dbt.db)

		err := npc.Check("UPDATE `test_policy` SET value = (?+2+?+!4)")
		// should be failed in show columns
		if err != nil {
			dbt.Fatal(err)
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}
