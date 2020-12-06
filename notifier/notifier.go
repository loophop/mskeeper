package notifier

import (
	"github.com/sirupsen/logrus"
	"gitlab.papegames.com/fringe/mskeeper/log"
)

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

type Notifier interface {
	Notify(lvl Level, sql string, errors []error, args ...interface{})
	SetLogLevel(level Level) Notifier
}

type DefaultNotifier struct {
	level Level
}

func NewDefaultNotifier() *DefaultNotifier {
	return &DefaultNotifier{}
}

func (dn *DefaultNotifier) SetLogLevel(level Level) Notifier {

	dn.level = level
	return dn
}

func (dn *DefaultNotifier) Notify(level Level, sql string, errors []error, args ...interface{}) {
	if level > dn.level {
		return
	}

	for i := 0; i < len(errors); i++ {
		log.MSKLog().Infof("[DefaultNotifier] error=%v, level=%v sql=%v args=%v", errors[i], level, sql, args)
	}
}
