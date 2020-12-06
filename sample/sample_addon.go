package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"gitlab.papegames.com/fringe/mskeeper/addon"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"log"
	"os"
	"time"
)

func main() {

	db, err := sql.Open("mysql", "root:19825026@(192.168.0.86:3306)/nnsecure?parseTime=true&loc=Local&charset=utf8")
	if err != nil {
		panic(err)
	}

	// safeDB := mysql_keeper.NewMySQLSafeGuard(db)
	safeDB := addon.NewMSKeeperAddon(
		db,
		options.WithSwitch(true),
		options.WithMaxExecTime(5*time.Second),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
		options.WithLogOutput(os.Stdout),
		options.WithLogLevel(notifier.WarnLevel),                 // 设置mskeeper自身的日志级别
		options.WithSQLWhiteLists("select * from client_config"), // 忽略长度略长的表格加载（此处是 10000*0.3 = 3000)
		options.WithSQLWhiteLists("select * from charge_config"),
		// mskeeper.WithNotifier(
		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
	)

	defer safeDB.Close()

	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	safeDB.Flush()

	//  QueryRow
	var any string
	var query string = `select system from idfa_blacklist where idfa = "d6beb261c20ac213"`
	if err := safeDB.QueryRow(query).Scan(&any); err != nil {
		log.Printf("safeDB.QueryRow failed %v", err)
	}
	safeDB.Flush()

	query = "select system from idfa_blacklist where system = ? and idfa = '36a5e97e3a241d02';"
	if err := safeDB.QueryRow(query, "android").Scan(&any); err != nil {
		log.Printf("safeDB.QueryRow failed %v", err)
		return
	}
	safeDB.Flush()
	log.Printf("demo any %v", any)

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

	// Exec
	query = "insert ignore into idfa_blacklist select * from idfa_blacklist where idfa like '7%%'"
	result, err := safeDB.Exec(query)
	if err != nil {
		log.Printf("safeDB.Exec failed %v", err)
		return
	}
	log.Printf("safeDB.Exec result %v ", result)
	safeDB.Flush()

	// Transaction
	tx, err := safeDB.Begin()
	if err != nil {
		log.Printf("safeDB.Begin failed %v", err)
		return
	}
	exec1 := "insert ignore into idfa_blacklist select * from idfa_blacklist where idfa like '6%%'"
	if _, err := tx.Exec(exec1); err != nil {
		_ = tx.Rollback()
		log.Printf("safeDB.Exec failed %v", err)
		return
	}

	exec2 := "UPDATE idfa_blacklist SET clientid = ? WHERE clientid = ? "
	if _, err := tx.Exec(exec2, "1019", "1019"); err != nil {
		_ = tx.Rollback()
		log.Printf("safeDB.Exec failed %v", err)
		return
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("safeDB.Commit failed %v", err)
		return
	}

	// 分析忽略，参考kwfilter.go中的忽略SQL关键字
	_, _ = safeDB.Exec("DROP TABLE test123")

	safeDB.Flush()
}
