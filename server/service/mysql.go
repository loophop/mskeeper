package service

import (
	"database/sql"
	"fmt"
	uuid "github.com/satori/go.uuid"
	mskdriver "gitlab.papegames.com/fringe/mskeeper/driver"
	"gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/misc"
	"gitlab.papegames.com/fringe/mskeeper/mysql"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"gitlab.papegames.com/fringe/mskeeper/sqlparser"
	// syslog "log"
	"context"
	"os"
	"reflect"
	"time"
)

const (
	MaxConnForEachDSN = 2 // 每个dns对应的最大DB链接数
)

type MySQLService struct {
	msk  *mskdriver.MSKeeper
	msid string
}

func (msqls *MySQLService) GetMSKeeper() *mskdriver.MSKeeper {
	return msqls.msk
}

func NewMySQLService(dsn string, dura int64) (*MySQLService, error) {
	msid := uuid.NewV3(uuid.UUID{}, misc.MD5String(dsn)).String()

	timeDura := time.Duration(dura) * time.Millisecond
	if timeDura <= 0 {
		timeDura = policy.DefaultMaxExecTime
	}

	sess, err := GetMSKSession(msid)
	if err == nil && sess.(*MySQLService) != nil {
		mss := sess.(*MySQLService)
		mss.msk.SetOptions(options.WithMaxExecTime(timeDura))
		return sess.(*MySQLService), nil
	}

	msservice := &MySQLService{msid: msid}
	// Verify the dsn and register the driver
	db, err := sql.Open("mskeeper", dsn)
	if err != nil {
		return msservice, err
	}
	defer db.Close()

	// dsn should have been registered successfully when sql.Open
	msservice.msk = mysql.MSKeeperInstance(dsn)
	msservice.msk.Flush()
	msservice.msk.ClearStatus() // 单元的时候应为dsn唯一，为了防止单元相互影响，初始化一下。

	// ignore exists
	_ = os.Mkdir("./logs", os.ModePerm)

	msservice.msk.SetOptions(
		options.WithSwitch(false), // 异步check队列关闭
		options.WithMaxExecTime(timeDura),
		options.WithNotifier(notifier.NewNotifierMux(
			notifier.NewNotifierLog(fmt.Sprintf("./logs/mskeeper_%v.log", msid)).SetLogLevel(notifier.InfoLevel),
			notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))),
		options.WithLogOutput(os.Stdout),
		options.WithLogLevel(notifier.WarnLevel),       // 设置mskeeper自身的日志级别
		options.WithSQLWhiteLists("SELECT version();"), // skip the sql that used by mskeeper itself
		options.WithMaxSilentPeriod(1*time.Second),     // restfulapi 暂时只需要瞬时排重
		options.WithSQLCacheSize(0),
	)

	_ = msservice.msk.AttachPolicy(policy.NewPolicyCheckerRowsAbsolute(10000))
	_ = msservice.msk.AttachPolicy(policy.NewPolicyCheckerRowsInvolved())
	_ = msservice.msk.AttachPolicy(policy.NewPolicyCheckerFieldsType())
	_ = msservice.msk.AttachPolicy(policy.NewPolicyCheckerFieldsLength())

	_ = addMSKSession(msid, msservice)

	return msservice, nil
}

func (this *MySQLService) RunSQL(req ServiceRunSQLRequest, res *ServiceRunSQLResponse) (reterr error) {

	stmt, err := sqlparser.Parse(req.SQL)
	if err != nil {
		log.MSKLog().Errorf("MySQLService:RunSQL(%v, %v) sqlparser.Parse failed with err %v",
			req, res, err)
		reterr = err
		return
	}
	if req.Timeout <= 0 {
		req.Timeout = MaxPingTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout*float64(time.Microsecond)))
	defer cancel()
	res.Notifies = []policy.PolicyError{}

	defer func(startTime time.Time) {
		chkReq := ServiceCheckSQLRequest{
			SQL:   req.SQL,
			MSID:  req.MSID,
			Args:  req.Args,
			STime: startTime,
		}
		chkRes := &ServiceCheckSQLResponse{Request: chkReq}
		err := this.CheckSQL(chkReq, chkRes)
		if err != nil {
			log.MSKLog().Infof("MySQLService:RunSQL(%v, %v) CheckSQL(%v, %v) failed with err %v",
				req, res, chkReq, chkRes, err)
			return
		}

		res.Notifies = chkRes.Notifies
	}(time.Now())
	switch stmt.(type) {
	case *sqlparser.Select, *sqlparser.OtherRead, *sqlparser.Show:
		res.Result.RowsAffected = 0

		// select with result
		// nargs, _ := mysql.Converter{}.ConvertValues(req.Args)
		rows, err := this.msk.RawDB().QueryContext(ctx, req.SQL, req.Args...)
		if err != nil {
			log.MSKLog().Errorf("MySQLService:RunSQL(%v, %v) Query failed with err %v",
				req, res, err)
			reterr = err
			return
		}
		defer rows.Close()
		columnNames, _ := rows.Columns()
		res.Result.Columns = make([]RunColumn, len(columnNames))
		tt, _ := rows.ColumnTypes()
		types := make([]reflect.Type, len(columnNames))
		for i, tp := range tt {
			types[i] = tp.ScanType()
			res.Result.Columns[i] = RunColumn{Name: tp.Name(), DataType: tp.DatabaseTypeName()}
		}
		columns := make([]interface{}, len(columnNames))
		for i := range columns {
			columns[i] = reflect.New(types[i]).Interface()
		}
		// syslog.Printf("columnNames %v", columnNames)

		res.Result.Rows = make([][]interface{}, 0)
		for rows.Next() {

			err := rows.Scan(columns...)
			if err != nil {
				log.MSKLog().Errorf("MySQLService:RunSQL(%v, %v) rows.Scan with err %v",
					req, res, err)
				reterr = err
				return
			}
			var row []interface{} = make([]interface{}, 0)
			for _, columnValue := range columns {
				value, _ := mysql.Converter{}.ConvertValue(columnValue)
				// []byte to string
				if reflect.ValueOf(columnValue).Elem().Kind() == reflect.Slice {
					row = append(row, string(value.([]byte)))
				} else {
					row = append(row, value)
				}
			}
			// syslog.Printf("row %v ", row)
			res.Result.Rows = append(res.Result.Rows, row)
		}
		return rows.Err()

	case *sqlparser.Begin, *sqlparser.Rollback, *sqlparser.Commit:
		// not support
		err := ErrStatementNotSupported
		log.MSKLog().Warnf("MySQLService:RunSQL(%v, %v) statement not supported",
			req, res)
		reterr = err
		return
	default:
		// execute sql
		result, err := this.msk.RawDB().ExecContext(ctx, req.SQL, req.Args...)
		if err != nil {
			log.MSKLog().Errorf("MySQLService:RunSQL(%v, %v) Execute failed with err %v",
				req, res, err)
			reterr = err
			return
		}
		ra, _ := result.RowsAffected()
		res.Result.RowsAffected = ra
	}

	return nil
}

func (this *MySQLService) CheckSQL(req ServiceCheckSQLRequest, res *ServiceCheckSQLResponse) error {

	res.Notifies = []policy.PolicyError{}
	rawerrors := []error{}
	nargs, _ := mysql.Converter{}.ConvertValues(req.Args)
	err := this.msk.SyncProcess(
		req.STime, req.SQL, nargs, &rawerrors)
	if err != nil {
		log.MSKLog().Infof("MySQLService:CheckSQL(%v, %v) failed %v",
			req, res, err)
		return err
	}
	for _, rawerror := range rawerrors {
		pe, _ := rawerror.(*policy.PolicyError)
		res.Notifies = append(res.Notifies, *pe)
	}

	return nil
}

func (this *MySQLService) PingDB(req ServicePingDBRequest, res *ServicePingDBResponse) error {

	if req.Timeout <= 0 {
		req.Timeout = MaxPingTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(req.Timeout*float64(time.Microsecond)))
	defer cancel()
	err := this.msk.RawDB().PingContext(ctx)
	if err != nil {
		log.MSKLog().Errorf("MySQLService:PingDB(%v, %v): failed(%v) to ping MySQL instance",
			req, res, err)
		return err
	}

	return nil
}

func (this *MySQLService) GetUUID() string {
	return this.msid
}
