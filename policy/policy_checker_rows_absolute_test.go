package policy

import (
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

func TestRawPolicyRowsAbsoluteFail(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		logmsk.MSKLog().SetOutput(os.Stdout)

		dbt.mustExec("CREATE TABLE `test_policy` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,KEY `value` (`value`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 102; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerRowsAbsolute(100), dbt.db)
		err := npc.Check("insert into test_policy select * from test_policy where test_policy.value != 1 and test_policy.value in (select value from test_policy);")
		/*
		   mysql> explain insert into test_policy select * from test_policy where test_policy.value != 1 and test_policy.value in (select value from test_policy);
		   +----+-------------+-------------+------------+------+---------------+-------+---------+--------------------------------+------+----------+--------------------------------------+
		   | id | select_type | table       | partitions | type | possible_keys | key   | key_len | ref                            | rows | filtered | Extra                                |
		   +----+-------------+-------------+------------+------+---------------+-------+---------+--------------------------------+------+----------+--------------------------------------+
		   |  1 | INSERT      | test_policy | NULL       | ALL  | NULL          | NULL  | NULL    | NULL                           | NULL |     NULL | NULL                                 |
		   |  1 | SIMPLE      | test_policy | NULL       | ALL  | value         | NULL  | NULL    | NULL                           |  101 |    99.01 | Using where; Using temporary         |
		   |  1 | SIMPLE      | test_policy | NULL       | ref  | value         | value | 5       | mskeepertest.test_policy.value |    1 |   100.00 | Using index; FirstMatch(test_policy) |
		   +----+-------------+-------------+------------+------+---------------+-------+---------+--------------------------------+------+----------+--------------------------------------+
		*/
		log.Printf("err ==== %v", err)
		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeRowsAbs {
			dbt.Errorf("rows absolutely not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		err = npc.Check("select * from test_policy Union select * from test_policy where value < 1034")
		log.Printf("err ==== %v", err)
		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeRowsAbs {
			dbt.Errorf("rows absolutely not covered")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)

	})
}

func TestRawPolicyRowsAbsoluteNormal(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		logmsk.MSKLog().SetOutput(os.Stdout)
		dbt.mustExec("CREATE TABLE `test_policy` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,KEY `value` (`value`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 99; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerRowsAbsolute(100), dbt.db)
		err := npc.Check("insert into test_policy select * from test_policy where test_policy.value in (select value from test_policy);")

		log.Printf("err ==== %v", err)
		if err != nil {
			dbt.Errorf("rows absolutely should not cover this")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyRowsAbsoluteNormalWithBadRowNumber(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		logmsk.MSKLog().SetOutput(os.Stdout)
		dbt.mustExec("CREATE TABLE `test_policy` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,KEY `value` (`value`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 99; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerRowsAbsolute(100), dbt.db)
		// Because of Union returns a NULL row field on explain.
		err := npc.Check("insert into test_policy select * from test_policy where test_policy.value in (select value from test_policy union select value from test_policy);")

		log.Printf("err ==== %v", err)
		if err != nil {
			dbt.Errorf("rows absolutely should not cover this")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}
