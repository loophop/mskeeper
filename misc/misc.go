package misc

import (
	"bytes"
	"crypto/md5"
	// "database/sql/driver"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"runtime"
	"strings"
)

func PrintPanicStack() {
	if x := recover(); x != nil {
		panicInfo := fmt.Sprintf("[PANIC] %v\n", x)
		for i := 0; i < 10; i++ {
			funcName, file, line, ok := runtime.Caller(i)
			if ok {
				panicInfo += fmt.Sprintf("[PANIC] frame %v:[func:%v,file:%v,line:%v]\n",
					i, runtime.FuncForPC(funcName).Name(), file, line)
			}
		}
		log.Println(panicInfo)

	}
}

func MD5String(str string, args ...interface{}) string {
	h := md5.New()
	_, _ = h.Write([]byte(str))
	argsStr := ""
	for _, arg := range args {
		if arg == nil {
			continue
		}
		argsStr += fmt.Sprintf("%v %v", reflect.TypeOf(arg).Name(), arg)
	}
	_, _ = h.Write([]byte(argsStr))

	return hex.EncodeToString(h.Sum(nil))
}

// 去掉sql中的连续空格
func TrimConsecutiveSpaces(sql string) string {
	outputStrBuf := bytes.NewBufferString("")
	spaceFlag := false
	for i := 0; i < len(sql); i++ {
		r := sql[i]
		if r == '\n' || r == '\t' {
			r = ' '
		}
		if r == ' ' {
			if !spaceFlag {
				outputStrBuf.WriteByte(r)
				spaceFlag = true
			}
			// skip consecutive spaces
		} else {
			outputStrBuf.WriteByte(r)
			spaceFlag = false
		}
	}

	return strings.TrimSpace(outputStrBuf.String())
}

// https://dev.mysql.com/doc/refman/8.0/en/insert.html
// 由于 mysql 5.5- 不支持 explain insert select形式，这样可能会将其中隐含的 select航母给忽略了
// 这个函数试图抽取select，仅仅限于 insert select。
func FilterInnerSelectFor55Minus(insertQuery string) string {
	resQuery := insertQuery
	uInsertQuery := strings.ToUpper(insertQuery)
	if len(uInsertQuery) < 6 {
		return resQuery
	}
	idxInsert := strings.Index(uInsertQuery[0:6], "INSERT")
	idxSelect := strings.Index(uInsertQuery, "SELECT")

	if idxInsert != -1 && idxSelect != -1 {
		if strings.ContainsAny(uInsertQuery[0:idxSelect], "'\"") {
			// != -1, should be pattern insert select,
			idxSelect = -1
		}
	}
	var begin, end int
	end = len(insertQuery)
	begin = 0
	if idxSelect != -1 {
		begin = idxSelect
	}
	idxDup := strings.LastIndex(uInsertQuery, "ON DUPLICATE KEY")
	if idxDup != -1 {
		if idxDup > 0 && uInsertQuery[idxDup-1] == ' ' {
			idxDup -= 1
		}
		end = idxDup
	}

	resQuery = insertQuery[begin:end]
	return resQuery
}

// 统计表达式中，非字符的?个数
func CountQuestionMark(str string) int {

	// in string
	var cnt int
	var instr1 bool = false // stand for '
	var instr2 bool = false // stand for "

	for i := 0; i < len(str); i++ {
		if str[i] == '\'' && !instr2 {
			instr1 = !instr1
		}
		if str[i] == '"' && !instr1 {
			instr2 = !instr2
		}

		if !instr1 && !instr2 && str[i] == '?' {
			cnt++
		}
	}

	return cnt
}

// 统计正规化后表达式中，用?替换:vx
func ReplaceColonMark(str string) string {

	// in string
	var instr1 bool = false // stand for '
	var instr2 bool = false // stand for "

	var result []byte
	for i := 0; i < len(str); i++ {
		if str[i] == '\'' && !instr2 {
			instr1 = !instr1
		}
		if str[i] == '"' && !instr1 {
			instr2 = !instr2
		}

		if !instr1 && !instr2 && str[i] == ':' {
			idxSpace := strings.IndexAny(str[i:], " ,)(")
			result = append(result, '?')
			if idxSpace != -1 {
				i = i + idxSpace - 1
			} else {
				i = len(str) - 1
			}
			continue
		}
		result = append(result, str[i])
	}

	return string(result)
}
