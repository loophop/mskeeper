package service

import (
	"errors"
	lru "github.com/hashicorp/golang-lru"
	mskdriver "gitlab.papegames.com/fringe/mskeeper/driver"
	logmsk "gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"time"
)

const (
	MaxSessionClient = 100     // 一个msk server实例最大的msk instance数 by dsn
	MaxPingTimeout   = 5000000 // DB连接最大的超时时间，unit: microsec
)

var (
	ErrSessionNotExists      = errors.New("MSKeeper session not exists")
	ErrStatementNotSupported = errors.New("MSKeeper sql statement not supported")
)

var (
	sessionmap *lru.Cache
)

func Init(langPath string, maxClient ...interface{}) error {
	var err error

	uplimit := MaxSessionClient
	// Parse uplimit
	for i := 0; i < len(maxClient); i++ {
		u, ok := maxClient[i].(int)
		if ok {
			uplimit = u
			break
		}
	}

	sessionmap, err = lru.New(uplimit)
	if err != nil {
		logmsk.MSKLog().Errorf("sessionmap init failed (%v)\n", err)
		return err
	}
	if err := initLocales(langPath); err != nil {
		logmsk.MSKLog().Errorf("Locales failed (%v)\n", err)
		return err
	}
	return nil
}

func GetMSKSession(uuid string) (Servicer, error) {
	session, ok := sessionmap.Get(uuid)
	if !ok {
		return nil, ErrSessionNotExists
	}
	return session.(Servicer), nil
}

func addMSKSession(uuid string, sess Servicer) error {
	sessionmap.Add(uuid, sess)
	return nil
}

type Servicer interface {
	CheckSQL(req ServiceCheckSQLRequest, res *ServiceCheckSQLResponse) error
	RunSQL(req ServiceRunSQLRequest, res *ServiceRunSQLResponse) error
	PingDB(req ServicePingDBRequest, res *ServicePingDBResponse) error
	GetUUID() string
	GetMSKeeper() *mskdriver.MSKeeper
}

type ServiceCheckSQLRequest struct {
	MSID  string        `json:"msid"`
	SQL   string        `json:"sql"`
	Args  []interface{} `json:"args,omitempty"`
	STime time.Time     `json:"stime"`
}

type ServiceCheckSQLResponse struct {
	Request  ServiceCheckSQLRequest `json:"request,omitempty"`
	Notifies []policy.PolicyError   `json:"notifies,omitempty"`
}

type ServiceRunSQLRequest struct {
	MSID    string        `json:"msid"`
	SQL     string        `json:"sql"`
	Args    []interface{} `json:"args,omitempty"`
	Timeout float64       `json:"timeout"`
}

type RunColumn struct {
	Name     string `json:"name"`
	DataType string `json:"type"`
}

type ServiceRunSQLResponse struct {
	Request ServiceRunSQLRequest `json:"request,omitempty"`
	Result  struct {
		Columns      []RunColumn     `json:"columns,omitempty"`
		Rows         [][]interface{} `json:"rows,omitempty"`
		RowsAffected int64           `json:"rows_affected"`
	}
	Notifies []policy.PolicyError `json:"notifies,omitempty"`
	MSID     string               `json:"msid"`
	SQL      string               `json:"sql"`
	Args     []interface{}        `json:"args"`
}

type ServicePingDBRequest struct {
	Timeout float64 `json:"timeout"` // milliseconds
}

type ServicePingDBResponse struct {
	Request ServicePingDBRequest `json:"request,omitempty"`
}
