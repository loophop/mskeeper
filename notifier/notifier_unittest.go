package notifier

import (
	// "database/sql/driver"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	// syslog "log"
	"sync"
	"time"
)

type NotifierUnitTest struct {
	errors []error
	sqls   []string
	delay  time.Duration
	lock   sync.RWMutex
}

func NewNotifierUnitTest() *NotifierUnitTest {

	notifier := &NotifierUnitTest{}
	notifier.errors = make([]error, 0)
	notifier.delay = 0

	return notifier
}

func (nl *NotifierUnitTest) Notify(level Level, sql string, errors []error, args ...interface{}) {
	nl.lock.Lock()
	defer nl.lock.Unlock()
	time.Sleep(nl.delay)
	nl.errors = append(nl.errors, errors...)
	nl.sqls = append(nl.sqls, sql)
}

func (nl *NotifierUnitTest) SetNotifyDelay(sec time.Duration) {
	nl.delay = sec
}

func (nl *NotifierUnitTest) HasErr(errCode policy.PolicyCode) bool {
	nl.lock.RLock()
	defer nl.lock.RUnlock()

	for i := 0; i < len(nl.errors); i++ {
		err, ok := nl.errors[i].(*policy.PolicyError)
		if ok && err.Code == errCode {
			return true
		}
	}

	return false
}

func (nl *NotifierUnitTest) GetNoSafeErrsCnt() int {
	nl.lock.RLock()
	defer nl.lock.RUnlock()

	cnt := 0
	for i := 0; i < len(nl.errors); i++ {
		err, ok := nl.errors[i].(*policy.PolicyError)
		if !ok || err.Code != policy.ErrPolicyCodeSafe {
			cnt++
		}
	}
	return cnt
}

func (nl *NotifierUnitTest) WithNoErrs() bool {
	nl.lock.RLock()
	defer nl.lock.RUnlock()

	for i := 0; i < len(nl.errors); i++ {
		err, ok := nl.errors[i].(*policy.PolicyError)
		if !ok || err.Code != policy.ErrPolicyCodeSafe {
			return false
		}
	}
	return true
}

func (nl *NotifierUnitTest) ErrsCount() int {
	nl.lock.RLock()
	defer nl.lock.RUnlock()

	return len(nl.errors)
}

func (nl *NotifierUnitTest) GetErrs() []error {
	nl.lock.RLock()
	defer nl.lock.RUnlock()

	clone := make([]error, len(nl.errors))
	copy(clone, nl.errors)
	return clone
}

func (nl *NotifierUnitTest) GetSQLs() []string {
	nl.lock.RLock()
	defer nl.lock.RUnlock()

	return nl.sqls
}

func (nl *NotifierUnitTest) ClearErr() {
	nl.lock.Lock()
	defer nl.lock.Unlock()

	nl.errors = make([]error, 0)
	nl.sqls = make([]string, 0)
}

func (nl *NotifierUnitTest) SetLogLevel(level Level) Notifier {
	// do nothing
	return nl
}
