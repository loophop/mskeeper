package httpsvr

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/server/service"
	// syslog "log"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type GinWrapper func(*gin.Context, http.ResponseWriter, *http.Request)

// type HTTPWrapperGin struct {
// 	handle http.HandlerFunc
// }

// func NewHTTPWrapperGin(handle http.HandlerFunc) *HTTPWrapperGin {
// 	return &HTTPWrapperGin{handle: handle}
// }

// func (httpwg *HTTPWrapperGin) HandleFunc(c *gin.Context) {
// 	httpwg.handle(c.Writer, c.Request)
// }
// func NewHTTPWrapperGin(handle http.HandlerFunc) gin.HandlerFunc {
// 	return func(c *gin.Context) {
// 		handle(c.Writer, c.Request)
// 	}
// }

// @Title 初始化mskeeper的会话
// @Description 通过dsn初始化mskeeper实例, dsn规则 https://www.cnblogs.com/diegodu/p/5586861.html
// @Produce json
// @Accept x-www-form-urlencoded
// @Param dsn formData string true "mysql dsn, eg. mskeeper_test:123456@tcp(192.168.0.86:3306)/mskeepertest?timeout=30s"
// @Param duration formData int false "SQL执行时间的告警上限，单位：ms, defaultly 3s"
// @Param timeout formData float64 false "DB连接的超时时间, 单位:micros, defaultly 5s"
// @Success 200 {object} RetData
// @Router /mysql/init [post]
func MySQLInitHandlerS(c *gin.Context) {
	ret := NewRet()
	defer c.JSON(http.StatusOK, ret)

	timeoutstr := c.Request.FormValue("timeout")
	durastr := c.Request.FormValue("duration")

	dsn := c.Request.FormValue("dsn") // mysql driver的dsn，用于生成UUID以及打开数据库
	if dsn == "" {
		log.MSKLog().Errorf("MySQLInitHandlerS(%v): failed to parse dsn %v",
			dsn, dsn)
		ret.Error(RetErrArgs, service.Tr(service.GetLanguage(c.Request), "dsn_empty"))
		return
	}

	dura, _ := strconv.ParseInt(durastr, 10, 64)
	timeout, _ := strconv.ParseFloat(timeoutstr, 64)
	sess, err := service.NewMySQLService(dsn, dura)
	if err != nil {
		log.MSKLog().Errorf("MySQLInitHandlerS(%v): failed(%v) to init MySQL Service",
			dsn, err)
		ret.Error(RetErrFailToInitService, err.Error())
		return
	}

	requestDB := service.ServicePingDBRequest{Timeout: timeout}
	responseDB := &service.ServicePingDBResponse{Request: requestDB}
	err = sess.PingDB(requestDB, responseDB)
	if err != nil {
		log.MSKLog().Errorf("MySQLInitHandlerS(%v): failed(%v) to ping MySQL instance",
			dsn, err)
		ret.Error(RetErrPingFailure, err.Error())
		return
	}

	ret.SetData("msid", sess.GetUUID())
}

// @Title 检测SQL的安全性
// @Description check sql through mskeeper
// @Produce json
// @Accept x-www-form-urlencoded
// @Param msid formData string true "Init API返回的MSID"
// @Param sql formData string true "执行的SQL"
// @Param args formData string false "SQL执行的参数，eg. [1,2,'333',4.44]"
// @Param timeout formData float64 false "DB连接的超时时间, 单位:micros, defaultly 5s"
// @Success 200 {object} RetData
// @Router /mysql/check [post]
func MySQLCheckHandlerS(c *gin.Context) {
	ret := NewRet()
	defer c.JSON(http.StatusOK, ret)

	msid := c.Request.FormValue("msid")
	sql := c.Request.FormValue("sql")
	argstr := c.Request.FormValue("args")
	timeoutstr := c.Request.FormValue("timeout")

	var args []interface{}
	if argstr != "" {
		err := json.Unmarshal([]byte(argstr), &args)
		if err != nil {
			log.MSKLog().Errorf("MySQLCheckHandlerS(%v, %v, %v): failed(%v) parse args %v",
				msid, sql, argstr, err, argstr)
			ret.Error(RetErrArgs, err.Error())
			return
		}
	}
	timeout, _ := strconv.ParseFloat(timeoutstr, 64)

	sess, err := service.GetMSKSession(msid)
	if err != nil {
		log.MSKLog().Errorf("MySQLCheckHandlerS(%v, %v, %v): failed(%v) to fetch MySQL Service",
			msid, sql, argstr, err)
		ret.Error(RetErrFailToGetService, err.Error())
		return
	}

	// 由于CheckSQL本身不检查DB的连通性，就是即使SQL有问题但是DB不同，结果也是SafeSQL
	// 因而为了提高体验，增加了PingDB以区别SafeSQL和DB Fail这两种情况
	requestDB := service.ServicePingDBRequest{Timeout: timeout}
	responseDB := &service.ServicePingDBResponse{Request: requestDB}
	err = sess.PingDB(requestDB, responseDB)
	if err != nil {
		log.MSKLog().Errorf("MySQLCheckHandlerS(%v, %v, %v): failed(%v) to connect DB",
			msid, sql, argstr, err)
		ret.Error(RetErrPingFailure, err.Error())
		return
	}

	request := service.ServiceCheckSQLRequest{SQL: sql, MSID: msid, Args: args, STime: time.Now()}
	response := &service.ServiceCheckSQLResponse{Request: request}
	err = sess.CheckSQL(request, response)
	if err != nil {
		log.MSKLog().Errorf("MySQLCheckHandlerS(%v, %v, %v): failed(%v) check sql",
			msid, sql, argstr, err)
		ret.Error(RetErrCheckFailure, err.Error())
		return
	}

	ret.SetData("request", response.Request)
	ret.SetData("notifies", response.Notifies)
}

// @Title 在给定的dsn上跑SQL
// @Description run the sql by dsn passed by /v1/mysql/init
// @Produce json
// @Accept x-www-form-urlencoded
// @Param msid formData string true "Init API返回的MSID"
// @Param sql formData string true "执行的SQL"
// @Param args formData string false "SQL执行的参数，eg. [1,2,'333',4.44]"
// @Param timeout formData int false "DB连接的超时时间, 单位:micros, defaultly 5s"
// @Success 200 {object} RetData
// @Router /mysql/run [post]
func MySQLRunHandlerS(c *gin.Context) {
	ret := NewRet()
	defer c.JSON(http.StatusOK, ret)
	stime := time.Now()

	msid := c.Request.FormValue("msid")
	sql := c.Request.FormValue("sql")
	timeoutstr := c.Request.FormValue("timeout")
	argstr := c.Request.FormValue("args")

	var args []interface{}
	if argstr != "" {
		err := json.Unmarshal([]byte(argstr), &args)
		if err != nil {
			log.MSKLog().Errorf("MySQLRunHandlerS(%v, %v, %v, %v): failed(%v) parse args %v",
				msid, sql, timeoutstr, argstr, err, argstr)
			ret.Error(RetErrArgs, err.Error())
			return
		}
	}

	timeout, _ := strconv.ParseFloat(timeoutstr, 64)

	sess, err := service.GetMSKSession(msid)
	if err != nil {
		log.MSKLog().Errorf("MySQLRunHandlerS(%v, %v, %v, %v): failed(%v) to fetch MySQL Service",
			msid, sql, timeoutstr, argstr, err)
		ret.Error(RetErrFailToGetService, err.Error())
		return
	}

	request := service.ServiceRunSQLRequest{SQL: sql, MSID: msid, Args: args, Timeout: timeout}
	response := &service.ServiceRunSQLResponse{Request: request}
	err = sess.RunSQL(request, response)
	if err != nil {
		log.MSKLog().Errorf("MySQLRunHandlerS(%v, %v, %v, %v): failed(%v) run sql",
			msid, sql, timeoutstr, argstr, err)
		ret.Error(RetErrRunFailure, err.Error())
		return
	}

	ret.SetData("request", response.Request)
	ret.SetData("notifies", response.Notifies)
	ret.SetData("results", response.Result)
	ret.SetData("taking", fmt.Sprintf("%.1fms", time.Since(stime).Seconds()*1000))
}
