package httpsvr

import (
	"time"
)

const (
	RetSuccess = 0

	RetErrCommon            = 1000 // 通用错误
	RetErrServerTimeout     = 1001 // 服务器处理超时（一般会处理完成、必要情况下需要rollback处理）
	RetErrFailToInitService = 1002 // mskeeper服务初始化失败
	RetErrFailToGetService  = 1003 // mskeeper服务获取失败
	RetErrCheckFailure      = 1004 // sql检测异常
	RetErrArgs              = 1005 // 参数错误
	RetErrRunFailure        = 1006 // sql运行失败
	RetErrPingFailure       = 1007 // 数据库连接异常
)

type RetModel map[string]interface{}

type RetData struct {
	Data RetModel `json:"data,omitempty"`
	Ret  uint32   `json:"ret"`
	Msg  string   `json:"msg,omitempty"`
	Time int64    `json:"time"`
}

func NewRet() *RetData {
	return &RetData{
		Ret:  RetSuccess,
		Time: time.Now().Unix(),
		Data: make(RetModel),
	}
}

func (r *RetData) SetData(key string, value interface{}) {
	r.Data[key] = value
}

func (r *RetData) Error(ret uint32, msg string) {
	r.Ret = ret
	r.Msg = msg
}
