package options

import (
	"io"
	"io/ioutil"
	"sync"
	"time"

	"gitlab.papegames.com/fringe/mskeeper/log"
	"gitlab.papegames.com/fringe/mskeeper/notifier"
	"gitlab.papegames.com/fringe/mskeeper/policy"
)

type Options struct {
	mutex           sync.RWMutex
	Switch          bool                // mskeeper的开关，可重入
	Notifier        notifier.Notifier   // 检查结果的通知对象
	MaxExecTime     time.Duration       // SQL的最大执行时间，超出则通知告警
	Capacity        int                 // mskeeper队列的最大长度，超出则丢失后来的SQL的检查
	MaxSilentPeriod time.Duration       // 最大的静默周期，即周期MaxSilentPeriod，相同签名的SQL告警至多只有一次。
	LogOutput       io.Writer           // mskeeper自身的日志输出 eg. os.Stdout, logfile
	SQLWhiteLists   map[string]struct{} // 不需要检测的SQL白名单
	SQLCacheSize    int                 // SQL哈希缓存大小设置, 0为不设置缓存, 默认以及上限是2千
	KeepAlivePeriod time.Duration       // KeepAlive包发送的周期, 默认 1h
}

const MaxSQLCacheSize = 2000
const DefaultKeepAlivePeriod = 1 * time.Hour

type Option func(*Options)

func NewOptions(opts ...Option) *Options {
	opt := DefaultOptions()

	for _, o := range opts {
		o(opt)
	}

	return opt
}

func (o *Options) Clone() *Options {
	nop := new(Options)
	nop.Switch = o.Switch
	nop.Notifier = o.Notifier
	nop.MaxExecTime = o.MaxExecTime
	nop.Capacity = o.Capacity
	nop.MaxSilentPeriod = o.MaxSilentPeriod
	nop.LogOutput = o.LogOutput
	nop.SQLCacheSize = o.SQLCacheSize
	nop.KeepAlivePeriod = o.KeepAlivePeriod

	nop.SQLWhiteLists = make(map[string]struct{})
	for k, v := range o.SQLWhiteLists {
		nop.SQLWhiteLists[k] = v
	}
	return nop
}

func DefaultOptions() *Options {
	opt := &Options{
		Switch:          false,
		MaxExecTime:     policy.DefaultMaxExecTime,
		MaxSilentPeriod: 1 * time.Hour,
		Notifier:        notifier.NewDefaultNotifier(),
		Capacity:        10240,
		LogOutput:       ioutil.Discard,
		SQLWhiteLists:   map[string]struct{}{},
		SQLCacheSize:    MaxSQLCacheSize,
		KeepAlivePeriod: DefaultKeepAlivePeriod,
	}
	return opt
}

func CheckIfInSQLWhiteLists(o *Options, sqlstring string) bool {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	_, ok := o.SQLWhiteLists[sqlstring]

	return ok
}

func WithSQLWhiteLists(sqlstring string) Option {
	return func(o *Options) {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		o.SQLWhiteLists[sqlstring] = struct{}{}
	}
}

func FetchLogLevel(o *Options) notifier.Level {

	return log.MSKLog().GetLevel()
}

func WithLogLevel(level notifier.Level) Option {
	return func(o *Options) {

		log.MSKLog().SetLevel(level)
	}
}

func FetchLogOutput(o *Options) io.Writer {

	return log.MSKLog().Out
}

func WithLogOutput(lo io.Writer) Option {
	return func(o *Options) {
		log.MSKLog().SetOutput(lo)
	}
}

func FetchMaxSilentPeriod(o *Options) time.Duration {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.MaxSilentPeriod
}

func FetchSQLCacheSize(o *Options) int {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.SQLCacheSize
}

func WithSQLCacheSize(s int) Option {
	return func(o *Options) {
		o.mutex.Lock()
		defer o.mutex.Unlock()
		if s > MaxSQLCacheSize {
			s = MaxSQLCacheSize
		}
		o.SQLCacheSize = s
	}
}

func WithMaxSilentPeriod(sp time.Duration) Option {
	return func(o *Options) {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		o.MaxSilentPeriod = sp
	}
}

func FetchSwitch(o *Options) bool {

	return o.Switch
}

func WithSwitch(s bool) Option {
	return func(o *Options) {

		o.Switch = s
	}
}

func FetchMaxExecTime(o *Options) time.Duration {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.MaxExecTime
}

func WithMaxExecTime(t time.Duration) Option {
	return func(o *Options) {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		o.MaxExecTime = t
	}
}

func FetchNotifier(o *Options) notifier.Notifier {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.Notifier
}

func WithNotifier(n notifier.Notifier) Option {
	return func(o *Options) {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		o.Notifier = n
	}
}

func FetchCapacity(o *Options) int {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.Capacity
}

func WithCapacity(n int) Option {
	return func(o *Options) {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		o.Capacity = n
	}
}

func FetchKeepAlivePeriod(o *Options) time.Duration {
	o.mutex.RLock()
	defer o.mutex.RUnlock()

	return o.KeepAlivePeriod
}

func WithKeepAlivePeriod(ka time.Duration) Option {
	return func(o *Options) {
		o.mutex.Lock()
		defer o.mutex.Unlock()

		o.KeepAlivePeriod = ka
	}
}
