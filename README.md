mskeeper是基于explain的mysql慢查询监控插件，可用于协助Golang上的mysql安全开发，其分别可通过插件或驱动的方式导入。

## Features:
1.  对上层SQL执行透明，可无痛导入mskeeper插件。
2.  默认支持Policy的可扩展，目前支持:[触发告警的Policies]
3.  告警通知Notifier可扩展，可复合（by NotifierMux）
4.  可在线修改参数配置(by options)
5.  SQL语句分析队列的过载保护，默认10240的队列，超出则丢弃，防止OOM。
6.  异步SQL分析，同步检查需配合刷新（by mskeeper.Flush)
7.  mskeeper系统日志可热插拔导出(with option LogOutput)
8.  相同签名SQL一小时内排重处理，防止SQL分析队列溢出
9.  SQL白名单机制，对于已知的SQL重度操作，例如一次性加载的SQL配置表等可通过白名单机制忽略(with option SQLWhiteLists)

## Policies:
1. NewPolicyCheckerRowsAbsolute(maxRows): 操作影响的行数 > maxRows 
2. NewPolicyCheckerRowsInvolved(): 操作影响的行数 > 1/3 总行数（count(1)) && 操作影响的行数 > 1000
3. NewPolicyCheckerFieldsType(): 操作数类型不匹配等导致的全表扫描策略
4. DefaultMaxExecTime：SQL执行超过3s
5. NewPolicyCheckerFieldsLength(): 字段发生截断（例如Text被截断为65535字节），目前支持整数(tinyint, smallint, mediumint, int, bigint)、blob（tinyblob, mediumblob, blob, longblob, binary, varbinary）以及字符串(char, varchar, tinytext, mediumtext, text, longtext)等，其他类型直接PASS。
6. NewPolicyCheckerFieldsLength(args ...interface{}): 长度截断上限可配置，通过设置比例args=0.9，可调整默认为0.8的截断比例上限至0.9。

相应的告警错误码, ErrPolicyCodeSafe 表示该SQL无告警，可过滤查看。

```go
const (
	ErrPolicyCodeSafe          PolicyCode = 5200  // Safely
	ErrPolicyCodeExeCost       PolicyCode = 5201  // Violate Policy 4
	ErrPolicyCodeRowsAbs       PolicyCode = 5202  // Violate Policy 1
	ErrPolicyCodeRowsInvolve   PolicyCode = 5203  // Violate Policy 2
	ErrPolicyCodeAllTableScan  PolicyCode = 5204  // Violate Policy 3
	ErrPolicyCodeDataTruncate  PolicyCode = 5205  // Violate Policy 5
	WarnPolicyCodeDataTruncate PolicyCode = 5206  // Violate Policy 6
)
```
## Configurations: 
1.  mysql扫描日志输出到文件mskeeper.log(只有告警)，msk库自身不输出日志(推荐!) 
```
	safeDB := addon.NewMSKeeperAddon(
		db,
		options.WithSwitch(true),
		options.WithNotifier(notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel)),
		options.WithSQLWhiteLists("select * from charge_config"),
	)
```
2.  mysql扫描日志输出到文件mskeeper.log(全部日志类型)，msk库自身日志输出到屏幕，设置5s最大的SQL执行时间（默认3s），忽略client_config加载SQL 
```
	safeDB := addon.NewMSKeeperAddon(
		db,
		options.WithSwitch(true),
		options.WithMaxExecTime(5*time.Second),
		options.WithNotifier(notifier.NewNotifierMux(notifier.NewNotifierLog("./mskeeper.log"),
			notifier.NewDefaultNotifier())),
		options.WithLogOutput(os.Stdout),
		options.WithSQLWhiteLists("select * from client_config"), // 忽略长度略长的表格加载（此处是 10000*0.3 = 3000)
	)
```

3.  mysql扫描日志输出到文件mskeeper.log同时打印到屏幕(全部日志类型), msk库自身不输出日志，忽略client_config和 charge_config加载SQL
```
	safeDB := addon.NewMSKeeperAddon(
		db,
		options.WithSwitch(true),
		options.WithMaxExecTime(5*time.Second),
		options.WithNotifier(notifier.NewNotifierMux(notifier.NewNotifierLog("./mskeeper.log"),
			notifier.NewDefaultNotifier())),
		options.WithSQLWhiteLists("select * from client_config"), // 忽略长度略长的表格加载（此处是 10000*0.3 = 3000)
		options.WithSQLWhiteLists("select * from charge_config"),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.InfoLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.InfoLevel))),
	)
```

更多选项及通知类型参考：[Options](https://github.com/loophop/mskeeper/blob/master/options.go), [Notifiers](https://github.com/loophop/mskeeper/tree/master/notifier)

## LogLevels:

1.  可通过notifier的成员函数 

```
SetLogLevel(level Level) Notifier

```
来设定扫描结果的日志级别。 

例如，notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel) 则表示只输出告警日志到 mskeeper.log

2.  可通过mskeeper的Option，修改mskeeper自身的日志级别 

```
mskeeper.SetOption(WithLogLevel(log.InfoLevel))

```

其中，mskeeper统一支持的日志级别如下： 

```
const (
	// PanicLevel level, highest level of severity. Logs and then calls panic with the
	// message passed to Debug, Info, ...
	PanicLevel Level = iota
	// FatalLevel level. Logs and then calls `logger.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	FatalLevel
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel
)
```

## Samples:  

[完整示例插件](https://github.com/loophop/mskeeper/blob/master/sample/sample_addon.go) [完整示例Driver](https://github.com/loophop/mskeeper/blob/master/sample/sample_driver.go) 
[扫描日志示例](https://github.com/loophop/mskeeper/blob/master/sample/mskeeper_session_log.txt) 

1.  插件方式导入 
```go
package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"gitlab.papegames.com/fringe/mskeeper/addon"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"log"
	"time"
)

func main() {

	db, err := sql.Open("mysql", "use:password@(127.0.0.1:3306)/nnsecuretest?parseTime=true&loc=Local&charset=utf8")
	if err != nil {
		panic(err)
	}

	safeDB := addon.NewMSKeeperAddon(
		db,
		options.WithSwitch(true),
		options.WithMaxExecTime(5*time.Second),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
		options.WithLogOutput(os.Stdout),
		options.WithLogLevel(notifier.WarnLevel),
		// options.WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)

	defer safeDB.Close()

	// 总影响字段数检测策略（此处10000行）
	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	// 字段影响相对行数检测策略
	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	// 字段类型不匹配检测策略
	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	// 字段溢出检测策略
	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerFieldsLength(0.9))

	//  Query
	query = "select system, zoneid, idfa from idfa_blacklist where idfa like '8f49%%'"
	rows, err := safeDB.Query(query)
	if err != nil {
		log.Printf("safeDB.QueryRow failed %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var system, zoneid, idfa string
		if err := rows.Scan(&system, &zoneid, &idfa); err != nil {
			log.Printf("rows.Scan failed %v", err)
			return
		}
		log.Printf("demo system %v zoneid %v idfa %v", system, zoneid, idfa)
	}
}

```

2.  Driver方式导入 
```go
package main

import (
	"database/sql"
	"gitlab.papegames.com/fringe/mskeeper/mysql"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"log"
	"time"
)

func main() {

	dsn := "use:password@(127.0.0.1:3306)/nnsecuretest?parseTime=true&loc=Local&charset=utf8"
	db, err := sql.Open("mskeeper",dsn)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	msk := mysql.MSKeeperInstance(dsn)
	if msk == nil {
		panic("failed to fetch mskeeper instance")
	}

	msk.SetOptions(
		options.WithSwitch(true),
		options.WithMaxExecTime(5*time.Second),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
		options.WithLogOutput(os.Stdout),
		options.WithLogLevel(notifier.WarnLevel),
		// options.WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)

	// 总影响字段数检测策略（此处10000行）
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	// 字段影响相对行数检测策略
	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	// 字段类型不匹配检测策略
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	// 字段溢出检测策略
	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength(0.9))

	//  Query
	query = "select system, zoneid, idfa from idfa_blacklist where idfa like '8f49%%'"
	rows, err := db.Query(query)
	if err != nil {
		log.Printf("db.QueryRow failed %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var system, zoneid, idfa string
		if err := rows.Scan(&system, &zoneid, &idfa); err != nil {
			log.Printf("rows.Scan failed %v", err)
			return
		}
		log.Printf("demo system %v zoneid %v idfa %v", system, zoneid, idfa)
	}
}

```
## Benchmark:

性能损失的范围在 10-30%，其中Raw表示没有导入mskeeper，Addon表示插件方式导入mskeeper而Driver则表示驱动方式导入的结果。 

```go
go test -bench=Benchmar -run=Benchmar -benchmem -memprofile memprofile.out -cpuprofile profile.out ./benchmark
goos: darwin
goarch: amd64

pkg: gitlab.papegames.com/fringe/mskeeper/benchmark
BenchmarkInsertRaw-8                             	    1222	    964741 ns/op	  113754 B/op	    2784 allocs/op
BenchmarkQueryRowFieldsTypeNotMatchRaw-8         	     542	   1929880 ns/op	  249530 B/op	    6536 allocs/op
BenchmarkQueryRowRaw-8                           	    2186	    512465 ns/op	   62745 B/op	    1549 allocs/op
BenchmarkQueryRaw-8                              	    2703	    555609 ns/op	   61998 B/op	    1537 allocs/op
BenchmarkInsertMSKDriver-8                       	     769	   1538808 ns/op	  183344 B/op	    4560 allocs/op
BenchmarkQueryRowFieldsTypeNotMatchMSKDriver-8   	     507	   2591830 ns/op	  406669 B/op	    9778 allocs/op
BenchmarkQueryRowMSKDriver-8                     	    1904	    850138 ns/op	  127252 B/op	    3146 allocs/op
BenchmarkQueryMSKDriver-8                        	    1491	   1034049 ns/op	  130930 B/op	    3341 allocs/op
BenchmarkInsertMSKAddon-8                        	     781	   1532743 ns/op	  138627 B/op	    3986 allocs/op
BenchmarkQueryRowFieldsTypeNotMatchMSKAddon-8    	     320	   3295417 ns/op	   66682 B/op	     903 allocs/op
BenchmarkQueryRowMSKAddon-8                      	    1581	    777248 ns/op	   18736 B/op	     286 allocs/op
BenchmarkQueryMSKAddon-8                         	    1599	    697725 ns/op	   18160 B/op	     278 allocs/op

```

## FAQ
**Q: 测试环境是什么？** 

A: Go的bin环境及MySQL数据库
1. Go version: 1.13.4 
2. MySQL: MySQL Community Server 5.6.47、5.7.25、5.7.28、8.0.18 
3. MySQL DB: 五个库 mskeepertest, mskeepertestmysql, mskeepertestdriver, mskeepertestaddon, mskeepertestpolicy，分别用于测试server、mysql、driver、addon以及policy包  
4. MySQL Setting: SET GLOBAL local_infile=1; 打开本地加载开关; SET GLOBAL sql_mode=""; 关闭sql的类型检查

基本上，mskeeper 可以适配5.5-8.0的所有MySQL版本，但是有例外: 
单元测试过程中，发现 5.7.25、5.7.28和 mysql事务的context超时机制无法兼容！ 
因为超时的取消动作，会导致事务无法释放，进而锁死目标table的meta！这一现象，在 5.6.47、8.0.18中暂无复现。 
 
**Q: 是否可以在正式服中跑？** 

A:原则上可以，但是如果保险起见，建议在测试开发环境，特别是 压测环境 中扫描并监控日志输出！ 


## Base版本

mskeeper的base driver是[mysql driver 1.5.0](https://github.com/go-sql-driver/mysql/commit/681ffa848bae8d0d7b1a452963c0705b132308f7)

## Restful-API

/v1/mysql/init, /v1/mysql/check 以及/v1/mysql/run 三个resultful-api，分别用于远程初始化mskeeper实例，sql检测以及sql运行并检测。 


