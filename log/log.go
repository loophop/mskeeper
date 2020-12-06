package log

import (
	"github.com/sirupsen/logrus"
	"io/ioutil"
)

var logmsk = logrus.New()

type Level = logrus.Level

const (
	// PanicLevel level, highest level of severity. Logs and then calls panic with the
	// message passed to Debug, Info, ...
	PanicLevel Level = iota
	// FatalLevel level. Logs and then calls `logger.Exit(1)`. It will exit even if the
	// logging level is set to Panic.
	FatalLevel
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel
)

func MSKLog() *logrus.Logger {
	return logmsk
}

// 默认不输出任何日志
func init() {
	logmsk = logrus.New()
	// var file, err = os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	// if err != nil {
	// 	log.Printf("NewNotifierLog failed to open file %v", logFile)
	// 	return nil
	// }
	logmsk.SetOutput(ioutil.Discard)
	logmsk.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
}
