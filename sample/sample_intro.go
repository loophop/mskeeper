package main

/*

	技术分享的例子

*/

// import (
// 	"database/sql"
// 	_ "github.com/go-sql-driver/mysql"
// 	"gitlab.papegames.com/fringe/mskeeper"
// 	"gitlab.papegames.com/fringe/mskeeper/notifier"
// 	"gitlab.papegames.com/fringe/mskeeper/policy"
// 	"log"
// 	"os"
// )

// func main() {

// 	db, err := sql.Open("mskeeper", "mskeeper_test:123456@(192.168.0.86:3306)/nnsecure?parseTime=true&loc=Local&charset=utf8")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// safeDB := mysql_keeper.NewMySQLSafeGuard(db)
// 	safeDB := mskeeper.New(
// 		db,
// 		mskeeper.WithSwitch(true),
// 		mskeeper.WithNotifier(notifier.NewNotifierMux(
// 			notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
// 			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
// 		mskeeper.WithLogOutput(os.Stdout),
// 		// mskeeper.WithNotifier(
// 		//	notifier.NewNotifierDingDing("ff4142c4a70a7890a51349913daaed4d60e2d40b478fa0e712d4e4fc213ad1f5")),
// 	)

// 	defer safeDB.Close()

// 	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
// 	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
// 	_ = safeDB.AttachPolicy(policy.NewPolicyCheckerFieldsType())

// 	// safeDB.Flush()

// 	//  Case 1
// 	// query := "SELECT id, clientid, system, zoneid, idfa FROM idfa_blacklist WHERE id >= 10"
// 	// rows, err := safeDB.Query(query)
// 	// if err != nil {
// 	// 	log.Printf("safeDB.QueryRow failed %v", err)
// 	// 	return
// 	// }
// 	// defer rows.Close()

// 	// for rows.Next() {
// 	// 	var system, clientid, zoneid, idfa string
// 	// 	var id int
// 	// 	if err := rows.Scan(&id, &clientid, &system, &zoneid, &idfa); err != nil {
// 	// 		log.Printf("rows.Scan failed %v", err)
// 	// 		return
// 	// 	}
// 	// 	// log.Printf("demo system %v zoneid %v idfa %v", system, zoneid, idfa)
// 	// }
// 	// safeDB.Flush()

// 	// Case 2
// 	// query := "SELECT task_id FROM audit_object WHERE mode IN (1,3) AND status NOT IN (-1,1,3) AND ctime > '2020-04-02 19:28:18' order by ctime desc limit 100"
// 	// rows, err := safeDB.Query(query)
// 	// if err != nil {
// 	// 	log.Printf("safeDB.QueryRow failed %v", err)
// 	// 	return
// 	// }
// 	// defer rows.Close()

// 	// for rows.Next() {
// 	// 	var task_id string
// 	// 	if err := rows.Scan(&task_id); err != nil {
// 	// 		log.Printf("rows.Scan failed %v", err)
// 	// 		return
// 	// 	}
// 	// 	// log.Printf("demo system %v zoneid %v idfa %v", system, zoneid, idfa)
// 	// }

// 	// safeDB.Flush()
// 	// Case 3
// 	query := "SELECT task_id FROM audit_object WHERE client_id = '1012';"
// 	rows, err := safeDB.Query(query)
// 	if err != nil {
// 		log.Printf("safeDB.QueryRow failed %v", err)
// 		return
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		var task_id string
// 		if err := rows.Scan(&task_id); err != nil {
// 			log.Printf("rows.Scan failed %v", err)
// 			return
// 		}
// 		// log.Printf("demo system %v zoneid %v idfa %v", system, zoneid, idfa)
// 	}
// 	safeDB.Flush()
// }
