// 针对api的测试, 详见各个*_test.go文件, 目前还不太全, 慢慢补

package httpsvr

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/server/service"
	"log"
	"net"
	"net/http/httptest"
	"os"
	"testing"
)

var (
	user      string
	pass      string
	prot      string
	addr      string
	dbname    string
	dsn       string
	dsn2      string
	netAddr   string
	available bool
)

var mskserveraddr = flag.String("mskeeper.server", "", "if non-empty, httptest.NewServer serves on this address and blocks")

// See https://github.com/go-sql-driver/mysql/wiki/Testing
func init() {

	// get environment variables
	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}
	user = env("MYSQL_TEST_USER", "root")
	pass = env("MYSQL_TEST_PASS", "19825026")
	prot = env("MYSQL_TEST_PROT", "tcp")
	addr = env("MYSQL_TEST_ADDR", "localhost:3306")
	dbname = env("MYSQL_TEST_DBNAME", "mskeepertest")
	netAddr = fmt.Sprintf("%s(%s)", prot, addr)
	dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s", user, pass, netAddr, dbname)
	dsn2 = fmt.Sprintf("%s:%s@%s/%s?timeout=25s", user, pass, netAddr, dbname)
	c, err := net.Dial(prot, addr)
	if err == nil {
		available = true
		c.Close()
	}
}

var server *httptest.Server

func TestMain(m *testing.M) {
	flag.Parse()

	// log
	os.Mkdir("logs", os.ModePerm)

	logmsk.MSKLog().SetOutput(os.Stdout)

	service.Init("../langs")

	router := RegisterHTTPSHandler()

	server = httptest.NewServer(router)

	if *mskserveraddr == "" {
		*mskserveraddr = server.URL
	}
	log.Printf("TestMain mskserveraddr %v, server.URL %v", mskserveraddr, server.URL)

	defer server.Close()

	os.Exit(m.Run())

	log.Printf("TestMain after")
}

func runAPITests(t *testing.T, dsn string, recordCnt int, tests ...func(t *testing.T)) {

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	db.SetMaxOpenConns(1000)

	_, err = db.Exec("DROP TABLE IF EXISTS testapi")
	assert.True(t, err == nil, err)

	_, err = db.Exec("CREATE TABLE `testapi` ( `value` int(11), `value1` varchar(60), `value2` varchar(2),  KEY `idx_value` (`value`), KEY `idx_value1` (`value1`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	assert.True(t, err == nil, err)

	for i := 0; i < recordCnt; i++ { // 超过1000行，过了策略的最低值
		_, err = db.Exec("INSERT INTO testapi VALUES (?, ?, ?)", i, i, fmt.Sprintf("%v", i))
		assert.True(t, err == nil, err)
	}

	for _, test := range tests {
		test(t)
		// _, err = db.Exec("DROP TABLE IF EXISTS testapi")
		// assert.True(t, err == nil, "should be ok")
	}
}
