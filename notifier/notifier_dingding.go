package notifier

import (
	"fmt"
	"gitlab.papegames.com/fringe/mskeeper/log"
	"net/http"
	"strings"
)

type NotifierDingDing struct {
	accessToken string
	level       Level
}

func NewNotifierDingDing(accessToken string) *NotifierDingDing {

	notifier := &NotifierDingDing{}
	notifier.accessToken = accessToken

	return notifier
}

func (nl *NotifierDingDing) Notify(level Level, sql string, errors []error, args ...interface{}) {
	log.MSKLog().Infof("NotifierDingDing:Notify(%v, %v, %v)", sql, args, errors)

	if level > nl.level {
		return
	}
	for i := 0; i < len(errors); i++ {
		sendDingMsg(fmt.Sprintf("[mskeeper] Error=%v Level=%v SQL=%v PARAM=%v", errors[i], level, sql, args), nl.accessToken)
	}
}

func (nl *NotifierDingDing) SetLogLevel(level Level) Notifier {

	nl.level = level
	return nl
}

func sendDingMsg(msg string, accessToken string) {
	//请求地址模板
	webHook := fmt.Sprintf(`https://oapi.dingtalk.com/robot/send?access_token=%v`, accessToken)
	content := `{"msgtype": "text",
		"text": {"content": "` + msg + `"}
	}`

	req, err := http.NewRequest("POST", webHook, strings.NewReader(content))
	if err != nil {
		log.MSKLog().Errorf("sendDingMsg(%v, %v) failed %v", msg, accessToken, err)
		return
	}

	client := &http.Client{}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	resp, err := client.Do(req)

	if err != nil {
		log.MSKLog().Errorf("sendDingMsg(%v, %v) failed %v", msg, accessToken, err)
		return
	}
	defer resp.Body.Close()
}
