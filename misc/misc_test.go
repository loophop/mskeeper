package misc

import (
	// "database/sql"

	// "log"
	"testing"
)

func TestMiscDeferPanic(t *testing.T) {
	defer PrintPanicStack()
	defer func(i int) {

		var b int = i
		b++

		if b != 2 {
			t.Errorf("panic cover not work")
		}

	}(1)

	panic("ooo")
}

func TestMiscMD5String(t *testing.T) {

	//func MD5String(str string, args ...interface{}) string {
	md1 := MD5String("select * from test where value = 1")
	md2 := MD5String("select * from test where value = 2")
	md3 := MD5String("Aelect * from test where value = 2")
	md4 := MD5String("Aelect * from test where value = ? and value1 = ?", 1, 2)
	md5 := MD5String("Aelect * from test where value = ? and value1 = ?", 3, 4)
	md6 := MD5String("Aelect * from test where s = ? and i = ?", "3", 5)
	md7 := MD5String("Aelect * from test where s = ? and i = ?", 3, 5)

	md8 := MD5String("Aelect * from test where s = ? and i = ?", 11)
	md9 := MD5String("Aelect * from test where s = ? and i = ?", nil, 11)
	if md1 == md2 || md2 == md3 || md1 == md3 {
		t.Errorf("md5 failed")
	}

	if md4 == md5 {
		t.Errorf("md5 failed")
	}

	if md6 == md7 {
		t.Errorf("md5 failed")
	}

	if md8 != md9 {
		t.Errorf("md5 failed")
	}
}

func TestMiscTrimConsecutiveSpaces(t *testing.T) {

	originSQL := "select * from test"
	sql := TrimConsecutiveSpaces(originSQL)
	if sql != originSQL {
		t.Errorf("sql %v changed", sql)
	}

	badSQL1 := "\n" + originSQL + "\n"
	sql = TrimConsecutiveSpaces(badSQL1)
	if sql != originSQL {
		t.Errorf("sql %v not filtered properly", sql)
	}

	badSQL2 := "    \n\t\t\t" + originSQL + "\n\n\n\n\t"
	sql = TrimConsecutiveSpaces(badSQL2)
	if sql != originSQL {
		t.Errorf("sql %v not filtered properly", sql)
	}

	badSQL3 := "  select    \n\n\n *   from \n\n test\t\t\t\t"
	sql = TrimConsecutiveSpaces(badSQL3)
	if sql != originSQL {
		t.Errorf("sql %v not filtered properly", sql)
	}
}

func TestMiscfilterInnerSelectFor55Minus(t *testing.T) {

	srcSql := "insert into user select from user limit 1"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL := FilterInnerSelectFor55Minus(srcSql)

	if resSQL != "select from user limit 1" {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "    insert    into     \t  \n       MARKETING_JUMP_MANAGE (ID,JUMP_CHANNEL,JUMP_CLASS, STATE, CREAT_TIME, UPDATE_TIME, CREAT_NAME, UPDATE_NAME, SHOW_STATE, POSITION_NAME, POSITION_CODE, COMPONENT_ID, COMPONENT_NAME, CUSTOM, JUMP_NAME,jump_channel_name) " +
		" SELECT SEQ_SUBSET_MESSAGE.NEXTVAL ,b.* from" +
		" ( " +
		" SELECT '60',JUMP_CLASS, STATE, CREAT_TIME, UPDATE_TIME, CREAT_NAME, UPDATE_NAME, SHOW_STATE, POSITION_NAME, POSITION_CODE, COMPONENT_ID, COMPONENT_NAME, CUSTOM, JUMP_NAME, 'XX渠道'" +
		"  FROM MARKETING_JUMP_MANAGE c " +
		"  where c.jump_channel='2'" +
		" )b;"

	selectSQL :=
		"SELECT SEQ_SUBSET_MESSAGE.NEXTVAL ,b.* from" +
			" ( " +
			" SELECT '60',JUMP_CLASS, STATE, CREAT_TIME, UPDATE_TIME, CREAT_NAME, UPDATE_NAME, SHOW_STATE, POSITION_NAME, POSITION_CODE, COMPONENT_ID, COMPONENT_NAME, CUSTOM, JUMP_NAME, 'XX渠道'" +
			"  FROM MARKETING_JUMP_MANAGE c " +
			"  where c.jump_channel='2'" +
			" )b;"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)
	if resSQL != TrimConsecutiveSpaces(selectSQL) {
		t.Errorf("filter failed for %v result ---------------- %v", srcSql, resSQL)
	}

	srcSql = "    INSERT     INTO a(field1,field2)     \t\t   SELECT * FROM(SELECT f1,f2 FROM b JOIN c) AS tb"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != "SELECT * FROM(SELECT f1,f2 FROM b JOIN c) AS tb" {
		t.Errorf("filter failed for %v", srcSql)
	}

	// 顺便比较个字符串看看
	if "3" > "4" || "1" > "9" || "5" < "5" {
		t.Errorf("string compare failed")
	}

	srcSql = "Insert score select s.id,'6',(Select avg(score) from score where course_id='2')  \t\t  from student s  where s.id not in (Select student_id from score where course_id='3');"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != "select s.id,'6',(Select avg(score) from score where course_id='2') from student s where s.id not in (Select student_id from score where course_id='3');" {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "     SELECT vale1, value2 into Table2 from Table1"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != resSQL {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "    insert     into       user values(1,2,3)"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != srcSql {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "insert into user values('select * from',2,'insert into')     \t    \n"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != srcSql {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "INSERT INTO `audit_object` (`id`, `client_id`, `activity_id`, `object_type`, `role_id`, `task_id`, `seed`, `context`, `mode`, `status`, `callback`, `callback3rd`, `extra`, `manual`, `result`, `bad_reason`, `auditor`, `mtime`, `ctime`, `audit_time`) VALUES (1, '1012', 0, 1, 0, \"select * from audit_object-1r54A6\", 'select * from audit_object', '一二三四五六七八九十一二三四五六七八九十一二三四五六七八九十一二三四五六七八九十一二三四五六七八九十	', 1, -1, '', 'https://apiactivity.nikkigames.cn:12111//v1/audit/ali/callback', '', '', 'null', '', '', '2019-08-02 15:03:36', '2019-08-02 15:03:36', '1970-01-01 00:00:00');"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)
	if resSQL != srcSql {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "select 1 from user"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != srcSql {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "select"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != srcSql {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "do"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != srcSql {
		t.Errorf("filter failed for %v", srcSql)
	}

	srcSql = "INSERT INTO `test_policy` select * from test_policy limit 2 on duplicate key update value1 = 2;"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != ("select * from test_policy limit 2") {
		t.Errorf("filter failed for %v", resSQL)
	}

	srcSql = "on duplicate key update value1 = 2;"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != "" {
		t.Errorf("filter failed for %v", resSQL)
	}

	srcSql = "INSERT INTO `test_policy` select * from test_policy limit 2 on duplicate key update value2 = 1, value1=2;"
	srcSql = TrimConsecutiveSpaces(srcSql)
	resSQL = FilterInnerSelectFor55Minus(srcSql)

	if resSQL != ("select * from test_policy limit 2") {
		t.Errorf("filter failed for %v", resSQL)
	}

}

func TestCountQuestionMark(t *testing.T) {

	str := "Aelect * from test where s = ? and i = ?"
	cnt := CountQuestionMark(str)
	if cnt != 2 {
		t.Errorf("cnt failed for %v", str)
	}

	str = "?|323|\"23\""
	cnt = CountQuestionMark(str)
	if cnt != 1 {
		t.Errorf("cnt failed for %v", str)
	}

	str = `323|\'?23\'`
	cnt = CountQuestionMark(str)
	if cnt != 0 {
		t.Errorf("cnt failed for %v", str)
	}

	str = `323|'"?23"?'`
	cnt = CountQuestionMark(str)
	if cnt != 0 {
		t.Errorf("cnt failed for %v", str)
	}

	str = `323|'"?23"?'|?|'233'&?`
	cnt = CountQuestionMark(str)
	if cnt != 2 {
		t.Errorf("cnt failed for %v", str)
	}
}

func TestReplaceColonMark(t *testing.T) {

	validSQL := []struct {
		input  string
		output string
	}{{
		input:  "select * from test where s = :v1 and i = :v2",
		output: "select * from test where s = ? and i = ?",
	}, {
		input:  "select * from test where s = ':1212:v1' and i = :v2",
		output: "select * from test where s = ':1212:v1' and i = ?",
	}, {
		input:  "update test   set s = (':1212:v1', i = :v2",
		output: "update test   set s = (':1212:v1', i = ?",
	}, {
		input:  "update test   set s = ':1212:v1', i = 1",
		output: "update test   set s = ':1212:v1', i = 1",
	}, {
		input:  "select binary value2 from test where value1 = '2:' limit 1",
		output: "select binary value2 from test where value1 = '2:' limit 1",
	}, {
		input:  "select * from test where value1 = :v1 limit 1   ",
		output: "select * from test where value1 = ? limit 1   ",
	}, {
		input:  `select * from test where value1 = ":v1" limit 1   `,
		output: `select * from test where value1 = ":v1" limit 1   `,
	}, {
		input:  `select * from test where value1 = :v1 and value2="abc"|:v123`,
		output: `select * from test where value1 = ? and value2="abc"|?`,
	},
	}

	for _, testCase := range validSQL {
		str := testCase.input
		res := ReplaceColonMark(str)
		if res != testCase.output {
			t.Fatalf("replace failed for %v with res %v", str, res)
		}

	}
}
