package notifier

import (
	"github.com/sirupsen/logrus"
	"gitlab.papegames.com/fringe/mskeeper/log"
	"os"
)

type NotifierLog struct {
	log *logrus.Logger
}

func NewNotifierLog(logFile string) *NotifierLog {

	notifier := &NotifierLog{}
	notifier.log = logrus.New()

	var file, err = os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.MSKLog().Errorf("NewNotifierLog failed to open file %v", logFile)
		return nil
	}

	notifier.log.SetOutput(file)
	notifier.log.SetFormatter(&logrus.TextFormatter{})

	return notifier
}

func (nl *NotifierLog) SetLogLevel(level Level) Notifier {
	nl.log.SetLevel(level)
	return nl
}

func (nl *NotifierLog) Notify(level Level, sql string, errors []error, args ...interface{}) {

	for i := 0; i < len(errors); i++ {
		nl.log.WithFields(logrus.Fields{
			"SQL":   sql,
			"PARAM": args,
		}).Log(level, errors[i])
	}
}
