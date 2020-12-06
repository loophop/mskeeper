package policy

import (
	"log"
	"testing"
)

func TestRawPolicyFieldsType(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 3001; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		var godamnv int = 5
		err := npc.Check("select * from test_policy where value1 = ?", godamnv)

		log.Printf("err ==== %v", err)
		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeAllTableScan {
			dbt.Errorf("fields type not match(all table scan) not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
	})
}

func TestRawPolicyFieldsTypeOK(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 101; i++ {
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		var godamnv string = "5"
		err := npc.Check("select * from test_policy where value1 = ?", godamnv)

		log.Printf("err ==== %v", err)
		if err != nil {
			dbt.Errorf("fields type not match(all table scan) not covered")
		}

		npc = NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		err = npc.Check("select * from test_policy where test_policy.value1 = '111' union select * from test_policy where test_policy.value1 = '222';")
		log.Printf("err ==== %v", err)
		if err != nil {
			dbt.Errorf("fields type not match(all table scan) not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
	})
}

// 全表扫描有where语句大于100行
func TestRawPolicyFieldsTypeAllWithWhere(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1010; i++ { //
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		err := npc.Check("select * from test_policy where test_policy.value1 = 111 union select * from test_policy where test_policy.value1 = '222';")

		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeAllTableScan {
			dbt.Errorf("fields type not match(all table scan) not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
	})
}

// 全表扫描有where语句小于100行
func TestRawPolicyFieldsTypeAllWithWhereLessThanCriterion1(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 99; i++ { //
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		err := npc.Check("select * from test_policy where test_policy.value > 0;")
		if err != nil {
			dbt.Errorf("should not be catched.")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
	})
}

// 全表扫描有where语句小于100行
func TestRawPolicyFieldsTypeAllWithWhereLessThanCriterion2(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 99; i++ { //
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		err := npc.Check("select * from test_policy where test_policy.value > 0;")
		if err != nil {
			dbt.Errorf("should not be catched.")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
	})
}

// 全表扫描没有where语句, 但是行数不超过1000行
func TestRawPolicyFieldsTypeAllWithNotWhereOK(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 101; i++ { //
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		err := npc.Check("select * from test_policy")
		if err != nil {
			dbt.Errorf("should not be catched.")
		}
	})
}

// 全表扫描没有where语句, 但是行数超过1000行
func TestRawPolicyFieldsTypeAllWithNotWhereNG(t *testing.T) {
	runRawPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE `test_policy` ( `value` int(11), `value1` varchar(60), KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
		for i := 0; i < 1001; i++ { //
			dbt.mustExec("INSERT INTO test_policy VALUES (?, ?)", i, i)
		}

		npc := NewPolicyCheckWraper(NewPolicyCheckerFieldsType(), dbt.db)
		err := npc.Check("select * from test_policy")
		if err == nil {
			dbt.Errorf("should be catched.")
		}
		log.Printf("err ==== %v", err)
		pe, _ := err.(*PolicyError)
		if pe.Code != ErrPolicyCodeAllTableScan {
			dbt.Errorf("fields type not match(all table scan) not covered")
		}

		err = npc.Check("DROP TABLE IF EXISTS test_policy")
		if err != nil {
			dbt.Errorf("no sql can be check")
		}
	})
}
