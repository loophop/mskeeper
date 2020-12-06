package driver

import (
	"testing"
)

func TestFindInkw1(t *testing.T) {

	/*
		"EXPLAIN",
		"DROP",
		"ALTER",
		"CREATE",
		"RENAME",
		"TRUNCATE",
		"LOCK",
		"UNLOCK",
		"CALL",
		"USE",
		"SHOW",
	*/
	validSQL := []struct {
		input  string
		output bool
	}{{
		input:  "EXPLAIN",
		output: true,
	}, {
		input:  " EXPLAIN",
		output: false,
	}, {
		input:  "SHOW",
		output: true,
	}, {
		input:  " SHOW",
		output: false,
	}, {
		input:  "UPDATE",
		output: false,
	}, {
		input:  "INSERT",
		output: false,
	}, {
		input:  "SELECT",
		output: false,
	},
	}

	for _, testCase := range validSQL {
		str := testCase.input
		res := findIn(str, keywords1)
		if res != testCase.output {
			t.Fatalf("findIn failed for %v with res %v", str, res)
		}
	}

}

func TestFindInkw2(t *testing.T) {

	/*
		"EXPLAIN",
		"DROP",
		"ALTER",
		"CREATE",
		"RENAME",
		"TRUNCATE",
		"LOCK",
		"UNLOCK",
		"CALL",
		"USE",
		"SHOW",
	*/
	validSQL := []struct {
		input  string
		output bool
	}{{
		input:  "EXPLAIN",
		output: true,
	}, {
		input:  " EXPLAIN",
		output: false,
	}, {
		input:  "DROP",
		output: true,
	}, {
		input:  "ALTER",
		output: true,
	}, {
		input:  "CREATE",
		output: true,
	}, {
		input:  "CREATE   ",
		output: false,
	}, {
		input:  "RENAME",
		output: true,
	}, {
		input:  "TRUNCATE",
		output: true,
	}, {
		input:  "LOCK",
		output: true,
	}, {
		input:  "UNLOCK",
		output: true,
	}, {
		input:  "CALL",
		output: true,
	}, {
		input:  "CA  LL",
		output: false,
	}, {
		input:  "USE",
		output: true,
	}, {
		input:  "SHOW",
		output: true,
	}, {
		input:  "UPDATE",
		output: false,
	}, {
		input:  "INSERT",
		output: false,
	}, {
		input:  "SELECT",
		output: false,
	}, {
		input:  "SET",
		output: true,
	},
	}

	for _, testCase := range validSQL {
		str := testCase.input
		res := findIn(str, keywords2)
		if res != testCase.output {
			t.Fatalf("findIn failed for %v with res %v", str, res)
		}
	}

}

func TestParseKeyWordFromSQL(t *testing.T) {

	sql := "select * from user"
	res := parseKeyWordFromSQL(sql)
	expect := "select"
	if res != expect {
		t.Fatalf("parseKeyWordFromSQL failed for %v with res %v", sql, res)
	}

	sql = "update user set value = 1"
	res = parseKeyWordFromSQL(sql)
	expect = "update"
	if res != expect {
		t.Fatalf("parseKeyWordFromSQL failed for %v with res %v", sql, res)
	}

	sql = "delete from user where value = 1"
	res = parseKeyWordFromSQL(sql)
	expect = "delete"
	if res != expect {
		t.Fatalf("parseKeyWordFromSQL failed for %v with res %v", sql, res)
	}

	sql = "show"
	res = parseKeyWordFromSQL(sql)
	expect = "show"
	if res != expect {
		t.Fatalf("parseKeyWordFromSQL failed for %v with res %v", sql, res)
	}

	sql = "drop"
	res = parseKeyWordFromSQL(sql)
	expect = "drop"
	if res != expect {
		t.Fatalf("parseKeyWordFromSQL failed for %v with res %v", sql, res)
	}
}

func TestCheckIfSQLExplainLike(t *testing.T) {

	validSQL := []struct {
		input  string
		output bool
	}{{
		input:  "EXPLAIN select * from user",
		output: true,
	}, {
		input:  "   EXPLAIN    select * from user2",
		output: true,
	}, {
		input:  "SHOW columns from user",
		output: true,
	}, {
		input:  " SHOW columns from user2",
		output: true,
	}, {
		input:  "UPDATE user set value =1",
		output: false,
	}, {
		input:  "INSERT user values(1,2,3)",
		output: false,
	}, {
		input:  "SELECT * from user",
		output: false,
	},
	}

	for _, testCase := range validSQL {
		str := testCase.input
		res := checkIfSQLExplainLike(str)
		if res != testCase.output {
			t.Fatalf("findIn failed for %v with res %v", str, res)
		}
	}

}

func TestCheckIfSQLHardcore(t *testing.T) {

	validSQL := []struct {
		input  string
		output bool
	}{{
		input:  "EXPLAIN select * from user",
		output: true,
	}, {
		input:  " EXPLAIN   select * from user",
		output: true,
	}, {
		input:  "DROP table if exists user",
		output: true,
	}, {
		input:  "   DROP     table if exists user",
		output: true,
	}, {
		input:  "ALTER table XXXX",
		output: true,
	}, {
		input:  "CREATE table user",
		output: true,
	}, {
		input:  "create table user",
		output: true,
	}, {
		input:  "CREATE   table user  ",
		output: true,
	}, {
		input:  "RENAME table into table2",
		output: true,
	}, {
		input:  "TRUNCATE table user",
		output: true,
	}, {
		input:  "     TRUNCATE table user",
		output: true,
	}, {
		input:  "LOCK table user",
		output: true,
	}, {
		input:  "lock table user",
		output: true,
	}, {
		input:  "UNLOCK table user",
		output: true,
	}, {
		input:  "CALL procedure proc1",
		output: true,
	}, {
		input:  "   CALL procedure proc2",
		output: true,
	}, {
		input:  "USE table1",
		output: true,
	}, {
		input:  "show columns from table1",
		output: true,
	}, {
		input:  "SHOW columns from table1",
		output: true,
	}, {
		input:  "UPDATE user set value =1 where value2 = 2",
		output: false,
	}, {
		input:  "   update      user set value =1 where value2 = 2",
		output: false,
	}, {
		input:  "insert into user select * from user_backup",
		output: false,
	}, {
		input:  "INSERT into user2 values(1,2,3),(1,2,3)",
		output: false,
	}, {
		input:  "SELECT * from user",
		output: false,
	}, {
		input:  " select * from user",
		output: false,
	}, {
		input:  "",
		output: false,
	}, {
		input:  "SET GLOBAL local_infile=1; ",
		output: true,
	}, {
		input:  "!!!132342342342",
		output: false,
	}, {
		input:  "!!!132342342342",
		output: false,
	},
	}

	for _, testCase := range validSQL {
		str := testCase.input
		res := checkIfSQLHardcore(str)
		if res != testCase.output {
			t.Fatalf("findIn failed for %v with res %v", str, res)
		}
	}

}
