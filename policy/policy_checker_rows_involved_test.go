package policy

import (
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestRawPolicyRowsInvolved(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		npc := NewPolicyCheckWraper(NewPolicyCheckerRowsInvolved(), dbt.db)

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11) DEFAULT NULL, `value1` int(11) DEFAULT NULL, KEY `value1` (`value1`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 3103; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		// hit the index, its ok
		err := npc.Check("select * from test_policy where value1 < 1000 limit 1")
		if err != nil {
			dbt.Errorf("rows involved 1/3 illegally trigged")
		}

		pes := NewPolicyErrorSafe(123, 1*time.Millisecond)
		log.Printf("pes %v", pes)
		if pes.Code != ErrPolicyCodeSafe {
			dbt.Errorf("pes.Code(%v) != ErrPolicyCodeSafe ", pes.Code)
		}

		// hit the index, however the rows larger than 1/3 all rows
		err = npc.Check("select * from test_policy where value1 < 1100")
		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeRowsInvolve {
			dbt.Errorf("rows involved 1/3 not 105 trigged")
		}

		// no index, all table scan
		// 3103 * 1/3 = 1031 > 1000 in 5.7
		// 3103 * 1/3 = 3031 > 1000
		err = npc.Check("select * from test_policy where value > 20")
		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeRowsInvolve {
			dbt.Errorf("rows involved 1/3 not trigged")
		}
	})
}

func TestRawPolicyRowsInvolvedDerivedUnionTable(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		logmsk.MSKLog().SetOutput(os.Stdout)

		npc := NewPolicyCheckWraper(NewPolicyCheckerRowsInvolved(), dbt.db)

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11) DEFAULT NULL, `value1` int(11) DEFAULT NULL, KEY `value1` (`value1`) ) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 3103; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		// hit the index, its ok
		err := npc.Check("select * from test_policy where value1 < 20 limit 1")
		if err != nil {
			dbt.Errorf("rows involved 1/3 illegally trigged")
		}

		// hit the index, however the rows larger than 1/3 all rows
		err = npc.Check("select * from test_policy where value1 > 1034")
		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeRowsInvolve {
			dbt.Errorf("rows involved 1/3 not 35 trigged")
		}

		// hit the index and the rows is less than 1/3
		err = npc.Check("select * from test_policy where value1 < 1000 Union select * from test_policy where value1 < 1000")
		if err != nil {
			dbt.Errorf("rows involved 1/3 illegally trigged")
		}

		// hit the index, however the rows larger than 1/3 all rows for both of select
		err = npc.Check("select * from test_policy where value1 < 1034 Union select * from test_policy where value < 1034")
		pe, _ = err.(*PolicyError)
		log.Printf("err ==== %v", err)
		if pe.Code != ErrPolicyCodeRowsInvolve {
			dbt.Errorf("rows involved 1/3 not 1034 trigged")
		}

		// no index, all table scan
		err = npc.Check("select * from test_policy where value < 10")
		pe, _ = err.(*PolicyError)
		if pe.Code != ErrPolicyCodeRowsInvolve {
			dbt.Errorf("rows involved 1/3 not trigged")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}

		logmsk.MSKLog().SetOutput(ioutil.Discard)
	})
}

func TestRawPolicyRowsInvolvedBadTableName(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {
		logmsk.MSKLog().SetOutput(os.Stdout)
		dbt.mustExec("CREATE TABLE `test_policy` (`value` int(11) DEFAULT NULL,`value1` int(11) DEFAULT NULL,KEY `value` (`value`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 99; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerRowsInvolved(), dbt.db)

		/* Because of <subquery2>
		+----+--------------+-------------+------------+--------+---------------------+---------------------+---------+-------------------------+-------+----------+------------------------------+
		| id | select_type  | table       | partitions | type   | possible_keys       | key                 | key_len | ref                     | rows  | filtered | Extra                        |
		+----+--------------+-------------+------------+--------+---------------------+---------------------+---------+-------------------------+-------+----------+------------------------------+
		|  1 | INSERT       | test        | NULL       | ALL    | NULL                | NULL                | NULL    | NULL                    |  NULL |     NULL | NULL                         |
		|  1 | SIMPLE       | test        | NULL       | ALL    | NULL                | NULL                | NULL    | NULL                    | 10157 |   100.00 | Using where; Using temporary |
		|  1 | SIMPLE       | <subquery2> | NULL       | eq_ref | <auto_distinct_key> | <auto_distinct_key> | 5       | mskeepertest.test.value |     1 |   100.00 | NULL                         |
		|  2 | MATERIALIZED | test        | NULL       | ALL    | NULL                | NULL                | NULL    | NULL                    | 10157 |   100.00 | NULL                         |
		+----+--------------+-------------+------------+--------+---------------------+---------------------+---------+-------------------------+-------+----------+------------------------------+
		*/
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
