package addon

import (
	// "bytes"
	//"crypto/tls"
	// "database/sql"
	// "log"
	// "database/sql/driver"
	// "encoding/json"
	// "errors"
	// "fmt"
	// "github.com/go-sql-driver/mysql"
	// "gitlab.papegames.com/fringe/mskeeper/notifier"
	// "gitlab.papegames.com/fringe/mskeeper/policy"
	// "io"
	// "io/ioutil"
	// "log"
	// "net"
	// "os"
	// "reflect"
	// "strings"
	"testing"
	// "time"
)

func TestPolicyPrepareStmt(t *testing.T) {
	runDefaultPolicyTests(t, dsn+"&columnsWithAlias=true", func(dbt *DBTest) {

		dbt.mustExec("CREATE TABLE testaddon (value int, value1 int)")
		for i := 0; i < 100; i++ {
			dbt.mustExec("INSERT INTO testaddon VALUES (?, ?)", i, i)
		}

		stmt, err := dbt.db.Prepare(`update testaddon set value = 0 where value1 = 10`)
		if err != nil {
			dbt.Fatal(err)
		}

		stmt.Exec()

		stmt, err = dbt.db.Prepare("SELECT value from testaddon where value1 = 10")
		if err != nil {
			dbt.Fatal(err)
		}

		rows, err := stmt.Query()
		if err != nil {
			dbt.Fatal(err)
		}
		defer rows.Close()

		var v int = -1
		for rows.Next() {

			if err := rows.Scan(&v); err != nil {
				dbt.Fatal(err)
			}
		}
		if v != 0 {
			dbt.Fatalf("v != 0, v = %v", v)
		}
	})
}
