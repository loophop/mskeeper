package notifier

import (
// "database/sql/driver"
)

type NotifierMux struct {
	notifiers []Notifier
}

func NewNotifierMux(args ...Notifier) *NotifierMux {

	noti := &NotifierMux{}
	noti.notifiers = make([]Notifier, 0)

	for _, notifier := range args {
		noti.AddNotifier(notifier)
	}

	return noti
}

func (nl *NotifierMux) Notify(level Level, sql string, errors []error, args ...interface{}) {

	for i := 0; i < len(nl.notifiers); i++ {
		nl.notifiers[i].Notify(level, sql, errors, args)
	}
}

func (nl *NotifierMux) AddNotifier(notifier Notifier) *NotifierMux {
	nl.notifiers = append(nl.notifiers, notifier)

	return nl
}

func (nl *NotifierMux) SetLogLevel(level Level) Notifier {
	for i := 0; i < len(nl.notifiers); i++ {
		nl.notifiers[i].SetLogLevel(level)
	}
	return nl
}
