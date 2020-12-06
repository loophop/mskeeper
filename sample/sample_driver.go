package main

// import (
// 	"database/sql"
// 	"gitlab.papegames.com/fringe/mskeeper/mysql"
// 	"gitlab.papegames.com/fringe/mskeeper/notifier"
// 	"gitlab.papegames.com/fringe/mskeeper/options"
// 	"gitlab.papegames.com/fringe/mskeeper/policy"
// 	"log"
// 	"os"
// 	"time"
// )

// func main() {

// 	dsn := "root:19825026@(192.168.0.86:3306)/nnsecure?parseTime=true&loc=Local&charset=utf8"
// 	db, err := sql.Open("mskeeper", dsn)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer db.Close()

// 	db.SetMaxOpenConns(5)
// 	db.SetMaxOpenConns(0)

// 	msk := mysql.MSKeeperInstance(dsn)
// 	if msk == nil {
// 		panic("failed to fetch mskeeper instance")
// 	}
// 	// Normally, it's unnecessary to ClearStatus since msk should have called only once
// 	// msk.ClearStatus()
// 	msk.SetOptions(
// 		options.WithSwitch(true),
// 		options.WithMaxExecTime(5*time.Second),
// 		options.WithNotifier(notifier.NewNotifierMux(
// 			notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
// 			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
// 		options.WithLogOutput(os.Stdout),
// 		options.WithLogLevel(notifier.WarnLevel),                 // 设置mskeeper自身的日志级别
// 		options.WithSQLWhiteLists("select * from client_config"), // 忽略长度略长的表格加载（此处是 10000*0.3 = 3000)
// 		options.WithSQLWhiteLists("select * from charge_config"),
// 	)

// 	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
// 	_ = msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
// 	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
// 	_ = msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

// 	msk.Flush()

// 	//  QueryRow
// 	var any string
// 	var query string = `select system from idfa_blacklist where idfa = "d6beb261c20ac213"`
// 	if err := db.QueryRow(query).Scan(&any); err != nil {
// 		log.Printf("db.QueryRow failed %v", err)
// 	}
// 	msk.Flush()

// 	query = "select system from idfa_blacklist where system = ? and idfa = '36a5e97e3a241d02';"
// 	if err := db.QueryRow(query, "android").Scan(&any); err != nil {
// 		log.Printf("db.QueryRow failed %v", err)
// 		return
// 	}

// 	msk.Flush()
// 	log.Printf("demo any %v", any)

// 	for i := 0; i < 100; i++ {
// 		//  Query
// 		query = "select system, zoneid, idfa from idfa_blacklist where idfa like '21FD2F72-%%'"
// 		rows, err := db.Query(query)
// 		if err != nil {
// 			log.Printf("db.QueryRow failed %v", err)
// 			return
// 		}
// 		defer rows.Close()

// 		for rows.Next() {
// 			var system, zoneid, idfa string
// 			if err := rows.Scan(&system, &zoneid, &idfa); err != nil {
// 				log.Printf("rows.Scan failed %v", err)
// 				return
// 			}
// 			// log.Printf("demo system %v zoneid %v idfa %v", system, zoneid, idfa)
// 		}
// 	}

// 	// Exec
// 	query = "insert ignore into idfa_blacklist select * from idfa_blacklist where idfa like '7%%'"
// 	result, err := db.Exec(query)
// 	if err != nil {
// 		log.Printf("db.Exec failed %v", err)
// 		return
// 	}
// 	log.Printf("db.Exec result %v ", result)
// 	msk.Flush()

// 	// Transaction
// 	tx, err := db.Begin()
// 	if err != nil {
// 		log.Printf("db.Begin failed %v", err)
// 		return
// 	}
// 	exec1 := "insert ignore into idfa_blacklist select * from idfa_blacklist where idfa like '7%%'"
// 	if _, err := tx.Exec(exec1); err != nil {
// 		_ = tx.Rollback()
// 		log.Printf("db.Exec failed %v", err)
// 		return
// 	}

// 	exec2 := "UPDATE idfa_blacklist SET clientid = ? WHERE clientid = ? "
// 	if _, err := tx.Exec(exec2, "1019", "1019"); err != nil {
// 		_ = tx.Rollback()
// 		log.Printf("db.Exec failed %v", err)
// 		return
// 	}

// 	err = tx.Commit()
// 	if err != nil {
// 		log.Printf("db.Commit failed %v", err)
// 		return
// 	}

// 	// 分析忽略，mskeeper忽略的操作关键字，请参考kwfilter.go中的定义
// 	_, _ = db.Exec("DROP TABLE test123")

// 	msk.Flush()
// }
