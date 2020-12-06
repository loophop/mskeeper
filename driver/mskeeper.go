// Go MSKeeper Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2020 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package driver

import (
	"bytes"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"

	"gitlab.papegames.com/fringe/mskeeper/log"
	"math"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
	"gitlab.papegames.com/fringe/mskeeper/misc"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
)

var (
	ErrMSKeeperClosed    = errors.New("MSKeeper is not open")
	ErrMSKeeperSQLIgnore = errors.New("sql is ignored")
)

const (
	MaxNumberOfShowWarningInfos      = 10
	NumberOfErrorsCached             = 100
	MaxTimeoutSecondsForFlush        = 5
	MaxTimeoutOfMSKeeperChannelWrite = 10 * time.Second
	MaxMSKConnections                = 2
	MaxMSKIdleConnections            = 1
	MySQLKeepAlivePeriod             = 1 * time.Hour
)

type MSKeeperInter interface {
	AfterProcess(t time.Time, query string, args []sqldriver.Value)
	AttachPolicy(policy policy.PolicyChecker) error
	ResetOptions(opts *options.Options)
	ResyncInfoQueue()
	ResyncPingTimer()
	SetOption(o options.Option)
	GetOptions() *options.Options
	SetOptions(opts ...options.Option)
	GetErr() []NotifyInfo
	Flush() error
	SyncProcess(t time.Time, query string, args []sqldriver.Value, reterrors *[]error) error
	ClearErr()
	HasErr(errCode policy.PolicyCode) bool
	RawDB() *sql.DB
	ClearPolicies()
}

type MSKeeper struct {
	db *sql.DB

	opts       *options.Options
	pcs        []policy.PolicyChecker
	lastestErr []NotifyInfo
	ch         chan *mskeeperInfo
	sigmap     *lru.Cache
	wg         sync.WaitGroup
	pingTimer  *time.Timer
	lock       sync.RWMutex
}

// type MSKeeperWarnInfo struct {
// 	Level   string
// 	Code    string
// 	Message string
// }

// func NewMSKeeperWarnInfo() *MSKeeperWarnInfo {
// 	return &MSKeeperWarnInfo{}
// }

type mskeeperInfo struct {
	// before bool
	cost  time.Duration
	query string
	args  []interface{}
}

type NotifyInfo struct {
	err error
	lvl notifier.Level
}

func NewMSKeeperInstance(db *sql.DB, opts ...options.Option) *MSKeeper {
	return newMSKDB(db, opts...)
}

// Addon way of mskeeper
func newMSKDB(db *sql.DB, opts ...options.Option) *MSKeeper {

	msg := &MSKeeper{
		pcs:  []policy.PolicyChecker{},
		opts: options.NewOptions(opts...),
	}
	msg.ch = make(chan *mskeeperInfo, msg.opts.Capacity)
	if options.FetchSQLCacheSize(msg.opts) > 0 {
		msg.sigmap, _ = lru.New(options.FetchSQLCacheSize(msg.opts))
	}
	msg.clearErr()

	msg.db = db

	go msg.process()

	// it's necessary for addon ?
	fap := options.FetchKeepAlivePeriod(msg.opts)
	go msg.keepAliveLoop(fap)

	return msg
}

// Driver way of mskeeper
func NewMSK(connector sqldriver.Connector, opts ...options.Option) *MSKeeper {

	msg := &MSKeeper{
		pcs:  []policy.PolicyChecker{},
		opts: options.NewOptions(opts...),
	}
	msg.ch = make(chan *mskeeperInfo, msg.opts.Capacity)
	if options.FetchSQLCacheSize(msg.opts) > 0 {
		msg.sigmap, _ = lru.New(options.FetchSQLCacheSize(msg.opts))
	}
	msg.clearErr()

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(MaxMSKConnections)
	db.SetMaxIdleConns(MaxMSKIdleConnections)

	msg.db = db
	go msg.process()

	fap := options.FetchKeepAlivePeriod(msg.opts)
	go msg.keepAliveLoop(fap)

	return msg
}

func (msk *MSKeeper) ResyncPingTimer() {
	fap := options.FetchKeepAlivePeriod(msk.opts)
	_ = msk.pingTimer.Reset(fap)
}

func (msk *MSKeeper) keepAliveLoop(period time.Duration) {
	msk.pingTimer = time.NewTimer(period)

	for {
		<-msk.pingTimer.C

		err := msk.RawDB().Ping()
		if err != nil {
			log.MSKLog().Infof("MSKeeper:KeepAliveLoop failed %v in pinging db", err)
		} else {
			log.MSKLog().Infof("MSKeeper:KeepAliveLoop at %v with period %v", time.Now(), period)
		}

		fap := options.FetchKeepAlivePeriod(msk.opts)
		_ = msk.pingTimer.Reset(fap)
	}
}

func (msk *MSKeeper) closeCh() {
	defer func() {
		if err := recover(); err != nil {
			log.MSKLog().Infof("MSKeeper:closeCh recover failed %v", err)
		}
	}()

	close(msk.ch) // panic if ch is closed
}

func (msk *MSKeeper) ClearStatus() {
	log.MSKLog().Infof("MSKeeper:ClearStauts")

	msk.ResetOptions(options.DefaultOptions())
	msk.ClearErr()
	msk.ClearPolicies()
	msk.ClearSigs()

	msk.ResyncInfoQueue()
}

func (msk *MSKeeper) ResetOptions(opts *options.Options) {
	msk.opts = opts
}

func (msk *MSKeeper) ResyncInfoQueue() {
	msk.closeCh()
	msk.ch = make(chan *mskeeperInfo, msk.opts.Capacity)

	go msk.process()
}

func (msk *MSKeeper) SetOption(o options.Option) {
	o(msk.opts)
}

func (msk *MSKeeper) GetOptions() *options.Options {
	return msk.opts
}

func (msk *MSKeeper) SetOptions(opts ...options.Option) {
	for _, o := range opts {
		o(msk.opts)
	}
}
func (msk *MSKeeper) GetErr() []NotifyInfo {
	return msk.lastestErr
}

func (msk *MSKeeper) recordLastestErr(errs []NotifyInfo) {

	msk.lastestErr = append(msk.lastestErr, errs...)
	if len(msk.lastestErr) > 0 {
		start := int(math.Max(float64(len(msk.lastestErr)-NumberOfErrorsCached), 0.0))
		msk.lastestErr = msk.lastestErr[start:len(msk.lastestErr)]
	}
}

func (msk *MSKeeper) clearErr() {
	msk.lastestErr = make([]NotifyInfo, 0)
}

func (msk *MSKeeper) ClearErr() {
	msk.clearErr()
}

func (msk *MSKeeper) HasErr(errCode policy.PolicyCode) bool {
	return msk.hasErr(errCode)
}

func (msk *MSKeeper) hasErr(errCode policy.PolicyCode) bool {
	for i := 0; i < len(msk.lastestErr); i++ {
		err, ok := msk.lastestErr[i].err.(*policy.PolicyError)
		if ok && err.Code == errCode {
			return true
		}
	}
	return false
}

func (msqlsg *MSKeeper) RawDB() *sql.DB {
	return msqlsg.db
}

func (msqlsg *MSKeeper) AttachPolicy(policy policy.PolicyChecker) error {
	msqlsg.pcs = append(msqlsg.pcs, policy)
	return nil
}

func (msqlsg *MSKeeper) ClearPolicies() {
	msqlsg.pcs = []policy.PolicyChecker{}
}

func (msqlsg *MSKeeper) ClearSigs() {
	if options.FetchSQLCacheSize(msqlsg.opts) > 0 {
		msqlsg.sigmap.Purge()
	}
}

// func (msqlsg *MSKeeper) beforeProcess(query string, args ...interface{}) {
// 	if !FetchSwitch(msqlsg.opts) {
// 		return
// 	}

// 	select {
// 	case msqlsg.ch <- &mskeeperInfo{
// 		before: true,
// 		query:  query,
// 		args:   append([]interface{}{}, args...)}:
// 	default:
// 	}
// }
func (msqlsg *MSKeeper) SyncProcess(t time.Time, query string, args []sqldriver.Value, reterrors *[]error) error {
	defer misc.PrintPanicStack()

	job := msqlsg.precheckOfJob(t, query, args)
	if job == nil {
		log.MSKLog().Infof("MSKeeper:SyncProcess(%v, %v, %v) job ignored", t, query, args)
		return ErrMSKeeperSQLIgnore
	}
	*reterrors = msqlsg.policiesCheck(job)

	// syslog.Printf("MSKeeper:SyncProcess(%v, %v, %v)", query, args, reterrors)
	return nil
}

func (msqlsg *MSKeeper) precheckOfJob(t time.Time, query string, args []sqldriver.Value) *mskeeperInfo {

	defer misc.PrintPanicStack()

	// 过滤自身或无需记录操作explain型语句
	if hc := checkIfSQLExplainLike(query); hc {
		// log.MSKLog().Infof("MSKeeper:AfterProcess skip explain like sql %v", query)
		return nil
	}
	defer log.MSKLog().Infof("MSKeeper:precheckOfJob(%v, %v, %v) started", t, query, args)

	// 去掉连续、前后缀空格（包括\t\n)
	query = misc.TrimConsecutiveSpaces(query)

	// 不带告警的纯SQL签名，不会影响同样SQL的告警触发，只是防止快速同样的SQL导致channel满。
	sqlsig := misc.MD5String(query, args)
	if msqlsg.sigmapUpdate(sqlsig) {
		log.MSKLog().Infof("MSKeeper:precheckOfJob skip of query %v args %v since sigmapUpdate %v return true",
			query, args, sqlsig)
		return nil
	}
	inWhiteList := options.CheckIfInSQLWhiteLists(msqlsg.opts, query)
	if inWhiteList {
		log.MSKLog().Infof("MSKeeper:precheckOfJob skip of query %v args %v since whitelist",
			query, args)
		return nil
	}
	iargs := []interface{}{}
	for i := 0; i < len(args); i++ {
		iargs = append(iargs, args[i])
	}

	// will be done in 1, finished checking; 2, channel queue was full
	msqlsg.wg.Add(1)
	return &mskeeperInfo{
		query: query,
		cost:  time.Since(t),
		args:  iargs}
}

func (msqlsg *MSKeeper) AfterProcess(t time.Time, query string, args []sqldriver.Value) {

	if !options.FetchSwitch(msqlsg.opts) {
		return
	}

	job := msqlsg.precheckOfJob(t, query, args)
	if job == nil {
		return
	}
	func() {
		defer func() {
			if err := recover(); err != nil {
				log.MSKLog().Warnf("MSKeeper:AfterProcess queue closed, when query %v", query)
				msqlsg.wg.Done()
			}
		}()
		select {
		case msqlsg.ch <- job:
		default:
			msqlsg.wg.Done()
			// 处理队列满，则丢弃
			log.MSKLog().Warnf("MSKeeper:AfterProcess queue %v was full, query %v check skipped",
				len(msqlsg.ch), query)
		}
	}()
	// syslog.Printf("+++++++++++++++++++++++AfterProcess %v %v", query, args)
}

// ！！！！ 单元测试或需要hook某一句SQL结果的时候，可以用。！！！！
// 通常情况下不需要调用
func (msqlsg *MSKeeper) Flush() error {
	defer misc.PrintPanicStack()

	start := time.Now()
	time.Sleep(100 * time.Millisecond)
	for len(msqlsg.ch) > 0 {
		if time.Since(start) > MaxTimeoutSecondsForFlush*time.Second {
			func() {
				defer misc.PrintPanicStack()
				close(msqlsg.ch) // 单元发现的问题，如果msqlsg.ch已关闭，则close时候会panic。用闭包抑制panic，从而使得error可以正确返回
			}()
			log.MSKLog().Warn("MSKeeper:Flush timed out")
			// msk.ch = make(chan *mskeeperInfo, msk.opts.Capacity)
			// go msk.process()
			return errors.New("MSKeeper:Flush timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
	msqlsg.wg.Wait()
	log.MSKLog().Infof("MSKeeper:Flush finished time %0.1vs elapesd", time.Since(start).Seconds())
	return nil
}

// return true: if the sql's signature has been updated(out of silent period)
func (msqlsg *MSKeeper) sigmapUpdate(errsig string) bool {
	if msqlsg.sigmap == nil || options.FetchSQLCacheSize(msqlsg.opts) <= 0 {
		return false
	}

	msp := options.FetchMaxSilentPeriod(msqlsg.opts)
	s, ok := msqlsg.sigmap.Get(errsig)

	// log.Printf("[DEBUG] msqlsg sigmaplen %v sigmap %v s %v, ok %v msp %v", len(msqlsg.sigmap), msqlsg.sigmap, s, ok, msp)
	if ok {
		stime, okk := s.(time.Time)
		if time.Since(stime).Nanoseconds() < msp.Nanoseconds() && okk { // okk == false 表示时间格式不对，刷新一下
			// already notified in last period
			log.MSKLog().Infof("MSKeeper:sigmapUpdate skip of errsig %v, since time.Since(stime).Seconds() %v < MaxSilentPeriod %v",
				errsig, time.Since(stime).Seconds(), msp.Seconds())
			return true
		} else {
			// period has expired
			msqlsg.sigmap.Add(errsig, time.Now())
		}
	} else {
		msqlsg.sigmap.Add(errsig, time.Now())
	}

	// 可以上报
	return false
}

// 周期内（比如1小时），相同SQL query的告警只显示一次
func (msqlsg *MSKeeper) notify(sql string, notifs []NotifyInfo, args ...interface{}) {

	var errcontent string
	for i := 0; i < len(notifs); i++ {
		errStrBuf := bytes.NewBufferString("")
		errStrBuf.WriteString(sql)
		errStrBuf.WriteString("|")
		errMSK, _ := notifs[i].err.(*policy.PolicyError)
		errStrBuf.WriteString(errMSK.Code.String() + "|")

		errcontent = errStrBuf.String()
		errsig := misc.MD5String(errcontent, args)

		if !msqlsg.sigmapUpdate(errsig) {
			// 非周期内重复告警，则继续上报。
			msqlsg.opts.Notifier.Notify(notifs[i].lvl, sql, []error{notifs[i].err}, args)
		}
	}
}

// // Get lastest n(at most) records of mysql warnings
// // REF: Out-of-Range and Overflow Handling
// // https://dev.mysql.com/doc/refman/8.0/en/out-of-range-and-overflow.html
// /*
// 	+---------+------+---------------------------------------------+
// 	| Level   | Code | Message                                     |
// 	+---------+------+---------------------------------------------+
// 	| Warning | 1264 | Out of range value for column 'i1' at row 1 |
// 	| Warning | 1264 | Out of range value for column 'i2' at row 1 |
// 	+---------+------+---------------------------------------------+
// */
// func (msqlsg *MSKeeper) GetLatestWarnings(n int) ([]MSKeeperWarnInfo, error) {
// 	if n < 0 || n > MaxNumberOfShowWarningInfos {
// 		n = MaxNumberOfShowWarningInfos
// 	}

// 	cmd := fmt.Sprintf("SHOW WARNINGS LIMIT %v;", n)

// 	records, err := MakeWarningRecords(msqlsg.db, cmd)
// 	if err != nil {
// 		log.MSKLog().Warnf("MSKeeper.GetLatestWarnings(%+v) hardcore sql skipped",
// 			n)
// 		return records, err
// 	}

// 	return records, err
// }

func (msqlsg *MSKeeper) policiesCheck(info *mskeeperInfo) []error {
	msqlsg.lock.Lock()
	defer msqlsg.lock.Unlock()

	notifies := make([]NotifyInfo, 0)
	rawerrors := make([]error, 0)

	var explainRecords []policy.ExplainRecord
	var err error
	var execTime time.Duration

	// 过滤不需要做解析的语句, 例如 DROP TABLE
	if hc := checkIfSQLHardcore(info.query); hc {
		log.MSKLog().Infof("MSKeeper:policiesCheck checkIfSQLHardcore skip sql %v", info.query)
		goto SAFE_SQL
	}

	execTime = options.FetchMaxExecTime(msqlsg.opts)
	explainRecords, err = policy.MakeExplainRecords(msqlsg.RawDB(), info.query, policy.MaxTimeoutOfExplain, info.args)
	if err == nil {
		for _, pc := range msqlsg.pcs {
			err := pc.Check(msqlsg.RawDB(), explainRecords, info.query, info.args)
			if err != nil && !strings.Contains(err.Error(), "1146") { // 1146 table deleted by other routine
				log.MSKLog().Warnf("MSKeeper.policiesCheck(%+v) pc.Check(%v, %v, %v) error %v",
					info.query, explainRecords, info.query, info.args, err)
				notifies = append(notifies, NotifyInfo{err: err, lvl: getNotifyLevelByPolicyCode(err)})
				rawerrors = append(rawerrors, err)
			}
		}
	}

	if info.cost > execTime {
		err := policy.NewPolicyError(policy.ErrPolicyCodeExeCost,
			fmt.Sprintf("Too much time spent in execution sql: cost(%0.3vms) > msqlsg.opts.MaxExecTime(%v)",
				float64(info.cost.Nanoseconds())/float64(1000000), execTime))
		notifies = append(notifies, NotifyInfo{err: err, lvl: getNotifyLevelByPolicyCode(err)})
		rawerrors = append(rawerrors, err)
	}
SAFE_SQL:
	if len(notifies) <= 0 {
		maxRows := policy.MaxRowsFromExplainRecords(explainRecords)
		errSuccess := policy.NewPolicyErrorSafe(maxRows, info.cost)
		notifies = append(notifies, NotifyInfo{err: errSuccess, lvl: getNotifyLevelByPolicyCode(errSuccess)})
		rawerrors = append(rawerrors, errSuccess)
	}

	msqlsg.recordLastestErr(notifies)
	msqlsg.notify(info.query, notifies, info.args)

	log.MSKLog().Infof("MSKeeper.policiesCheck(%+v, %v) execution time limit(%v) cost %v with notifies %v",
		info.query, info.args, execTime, info.cost, notifies)
	msqlsg.wg.Done()

	return rawerrors
}

func (msqlsg *MSKeeper) process() {
	log.MSKLog().Infof("MSKeeper:process() started")

	defer misc.PrintPanicStack()
	s := time.Now()
	for info := range msqlsg.ch {
		_ = msqlsg.policiesCheck(info)
	}
	log.MSKLog().Infof("MSKeeper.process() ended, took %vs",
		time.Since(s).Seconds())
}

func getNotifyLevelByPolicyCode(err error) notifier.Level {
	var lvl notifier.Level
	perror, ok := err.(*policy.PolicyError)
	if !ok {
		lvl = notifier.WarnLevel
	} else {
		switch perror.Code {
		case policy.ErrPolicyCodeSafe:
			lvl = notifier.InfoLevel
		case policy.WarnPolicyCodeDataTruncate:
			lvl = notifier.WarnLevel
		default:
			lvl = notifier.ErrorLevel
		}
	}
	return lvl
}
