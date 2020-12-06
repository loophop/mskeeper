package service

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "gitlab.papegames.com/fringe/mskeeper/mysql"
	"net"
	"os"
	"testing"
)

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
	c, err := net.Dial(prot, addr)
	if err == nil {
		available = true
		c.Close()
	}

}

func TestServiceInit(t *testing.T) {

	err := Init("123+123")
	assert.True(t, err != nil, "sql.open failed")

	err = Init("../langs", -1)
	assert.True(t, err != nil, "lru failed")
}
