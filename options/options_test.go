package options

import (
	// "log"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"gitlab.papegames.com/fringe/mskeeper/notifier"
)

func TestOptionsDefault(t *testing.T) {

	opts := NewOptions()

	defaultOpt := DefaultOptions()

	if FetchCapacity(opts) != FetchCapacity(defaultOpt) {
		t.Fatalf("defaultOpt.Capacity not initialized properly ")
	}

	if !reflect.DeepEqual(FetchNotifier(opts), FetchNotifier(defaultOpt)) {
		t.Fatalf("defaultOpt.Notifier not initialized properly ")
	}

	if FetchSwitch(opts) != FetchSwitch(defaultOpt) {
		t.Fatalf("defaultOpt.Switch not initialized properly ")
	}

	if FetchLogLevel(opts) != FetchLogLevel(defaultOpt) {
		t.Fatalf("defaultOpt.LogLevel not initialized properly ")
	}

	if !reflect.DeepEqual(FetchLogOutput(opts), FetchLogOutput(defaultOpt)) {
		t.Fatalf("defaultOpt.Output not initialized properly ")
	}

	if FetchMaxExecTime(opts) != FetchMaxExecTime(defaultOpt) {
		t.Fatalf("defaultOpt.MaxExecTime not initialized properly ")
	}
	if FetchMaxSilentPeriod(opts) != FetchMaxSilentPeriod(defaultOpt) {
		t.Fatalf("defaultOpt.MaxSilentPeriod not initialized properly ")
	}

	if FetchSQLCacheSize(opts) != FetchSQLCacheSize(defaultOpt) {
		t.Fatalf("defaultOpt.SQLCacheSize not initialized properly ")
	}

	if FetchKeepAlivePeriod(opts) != FetchKeepAlivePeriod(defaultOpt) {
		t.Fatalf("defaultOpt.KeepAlivePeriod not initialized properly ")
	}
}

func TestOptionsSetting1(t *testing.T) {

	noti := notifier.NewNotifierMux(
		notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
		notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))
	opts := NewOptions(
		WithCapacity(1234),
		WithSwitch(true),
		WithMaxExecTime(5*time.Second),
		WithMaxSilentPeriod(10*time.Second),
		WithNotifier(noti),
		WithLogOutput(os.Stdout),
		WithLogLevel(notifier.ErrorLevel), // 设置mskeeper自身的日志级别
		WithSQLWhiteLists("select * from client_config"),
		WithSQLCacheSize(1412),
		WithKeepAlivePeriod(1412*time.Second),
	)

	if FetchCapacity(opts) != 1234 {
		t.Fatalf("NewOptions.Capacity not initialized properly ")
	}

	if !reflect.DeepEqual(FetchNotifier(opts), noti) {
		t.Fatalf("NewOptions.Notifier not initialized properly ")
	}

	if FetchSwitch(opts) != true {
		t.Fatalf("NewOptions.Switch not initialized properly ")
	}

	if FetchLogLevel(opts) != notifier.ErrorLevel {
		t.Fatalf("NewOptions.LogLevel not initialized properly ")
	}

	if !reflect.DeepEqual(FetchLogOutput(opts), os.Stdout) {
		t.Fatalf("NewOptions.Output not initialized properly ")
	}

	if FetchMaxExecTime(opts) != 5*time.Second {
		t.Fatalf("NewOptions.MaxExecTime not initialized properly ")
	}

	if FetchMaxSilentPeriod(opts) != 10*time.Second {
		t.Fatalf("NewOptions.MaxSilentPeriod not initialized properly ")
	}

	if !CheckIfInSQLWhiteLists(opts, "select * from client_config") {
		t.Fatalf("NewOptions.SQLWhiteLists not initialized properly ")
	}

	if FetchSQLCacheSize(opts) != 1412 {
		t.Fatalf("NewOptions.SQLCacheSize not initialized properly")
	}

	if FetchKeepAlivePeriod(opts) != 1412*time.Second {
		t.Fatalf("NewOptions.KeepAlivePeriod not initialized properly")
	}
}

func TestOptionsSetting2(t *testing.T) {

	opts := NewOptions()
	noti := notifier.NewNotifierMux(
		notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
		notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))

	WithCapacity(1234)(opts)
	if FetchCapacity(opts) != 1234 {
		t.Fatalf("SetOptions.Capacity not initialized properly ")
	}

	WithCapacity(0)(opts)
	if FetchCapacity(opts) != 0 {
		t.Fatalf("SetOptions.Capacity not initialized properly ")
	}

	WithCapacity(2048000)(opts)
	if FetchCapacity(opts) != 2048000 {
		t.Fatalf("SetOptions.Capacity not initialized properly ")
	}

	WithNotifier(noti)(opts)
	if !reflect.DeepEqual(FetchNotifier(opts), noti) {
		t.Fatalf("SetOptions.Notifier not initialized properly ")
	}

	nt := notifier.NewNotifierUnitTest()
	WithNotifier(nt)(opts)
	if !reflect.DeepEqual(FetchNotifier(opts), nt) {
		t.Fatalf("SetOptions.Notifier not initialized properly ")
	}

	dd := notifier.NewNotifierDingDing("ac")
	WithNotifier(dd)(opts)
	if !reflect.DeepEqual(FetchNotifier(opts), dd) {
		t.Fatalf("SetOptions.Notifier not initialized properly ")
	}

	ml := notifier.NewNotifierLog("my.log")
	WithNotifier(ml)(opts)
	if !reflect.DeepEqual(FetchNotifier(opts), ml) {
		t.Fatalf("SetOptions.Notifier not initialized properly ")
	}

	WithSwitch(true)(opts)
	if FetchSwitch(opts) != true {
		t.Fatalf("SetOptions.Switch not initialized properly ")
	}

	WithSwitch(false)(opts)
	if FetchSwitch(opts) != false {
		t.Fatalf("SetOptions.Switch not initialized properly ")
	}

	WithLogLevel(notifier.ErrorLevel)(opts)
	if FetchLogLevel(opts) != notifier.ErrorLevel {
		t.Fatalf("SetOptions.LogLevel not initialized properly ")
	}

	WithLogLevel(notifier.InfoLevel)(opts)
	if FetchLogLevel(opts) != notifier.InfoLevel {
		t.Fatalf("SetOptions.LogLevel not initialized properly ")
	}

	WithLogLevel(notifier.FatalLevel)(opts)
	if FetchLogLevel(opts) != notifier.FatalLevel {
		t.Fatalf("SetOptions.LogLevel not initialized properly ")
	}

	WithLogLevel(notifier.DebugLevel)(opts)
	if FetchLogLevel(opts) != notifier.DebugLevel {
		t.Fatalf("SetOptions.LogLevel not initialized properly ")
	}

	WithLogLevel(notifier.TraceLevel)(opts)
	if FetchLogLevel(opts) != notifier.TraceLevel {
		t.Fatalf("SetOptions.LogLevel not initialized properly ")
	}

	WithLogLevel(notifier.PanicLevel)(opts)
	if FetchLogLevel(opts) != notifier.PanicLevel {
		t.Fatalf("SetOptions.LogLevel not initialized properly ")
	}

	WithLogLevel(notifier.WarnLevel)(opts)
	if FetchLogLevel(opts) != notifier.WarnLevel {
		t.Fatalf("SetOptions.LogLevel not initialized properly ")
	}

	WithLogOutput(os.Stdout)(opts)
	if !reflect.DeepEqual(FetchLogOutput(opts), os.Stdout) {
		t.Fatalf("SetOptions.Output not initialized properly ")
	}

	WithLogOutput(ioutil.Discard)(opts)
	if !reflect.DeepEqual(FetchLogOutput(opts), ioutil.Discard) {
		t.Fatalf("SetOptions.Output not initialized properly ")
	}

	WithMaxExecTime(5 * time.Second)(opts)
	if FetchMaxExecTime(opts) != 5*time.Second {
		t.Fatalf("SetOptions.MaxExecTime not initialized properly ")
	}

	WithMaxExecTime(0 * time.Second)(opts)
	if FetchMaxExecTime(opts) != 0*time.Second {
		t.Fatalf("SetOptions.MaxExecTime not initialized properly ")
	}

	WithMaxExecTime(11122 * time.Millisecond)(opts)
	if FetchMaxExecTime(opts) != 11122*time.Millisecond {
		t.Fatalf("SetOptions.MaxExecTime not initialized properly ")
	}

	WithMaxExecTime(112233445566 * time.Microsecond)(opts)
	if FetchMaxExecTime(opts) != 112233445566*time.Microsecond {
		t.Fatalf("SetOptions.MaxExecTime not initialized properly ")
	}

	WithMaxSilentPeriod(10 * time.Second)(opts)
	if FetchMaxSilentPeriod(opts) != 10*time.Second {
		t.Fatalf("SetOptions.MaxSilentPeriod not initialized properly ")
	}

	WithMaxSilentPeriod(0 * time.Second)(opts)
	if FetchMaxSilentPeriod(opts) != 0*time.Second {
		t.Fatalf("SetOptions.MaxSilentPeriod not initialized properly ")
	}

	WithMaxSilentPeriod(1 * time.Millisecond)(opts)
	if FetchMaxSilentPeriod(opts) != 1*time.Millisecond {
		t.Fatalf("SetOptions.MaxSilentPeriod not initialized properly ")
	}

	WithMaxSilentPeriod(4 * time.Hour)(opts)
	if FetchMaxSilentPeriod(opts) != 4*time.Hour {
		t.Fatalf("SetOptions.MaxSilentPeriod not initialized properly ")
	}

	WithSQLWhiteLists("select * from client_config")(opts)
	if !CheckIfInSQLWhiteLists(opts, "select * from client_config") {
		t.Fatalf("SetOptions.SQLWhiteLists not initialized properly ")
	}

	if CheckIfInSQLWhiteLists(opts, "select * from black_config") {
		t.Fatalf("SetOptions.SQLWhiteLists not initialized properly ")
	}

	WithSQLWhiteLists("select * from black_config")(opts)
	if !CheckIfInSQLWhiteLists(opts, "select * from black_config") {
		t.Fatalf("SetOptions.SQLWhiteLists not initialized properly ")
	}

	WithSQLWhiteLists("")(opts)
	if !CheckIfInSQLWhiteLists(opts, "") {
		t.Fatalf("SetOptions.SQLWhiteLists not initialized properly ")
	}

	WithSQLWhiteLists("  1111111111111  12222  223332323  ")(opts)
	if !CheckIfInSQLWhiteLists(opts, "  1111111111111  12222  223332323  ") {
		t.Fatalf("SetOptions.SQLWhiteLists not initialized properly ")
	}

	WithSQLCacheSize(1345)(opts)
	if FetchSQLCacheSize(opts) != 1345 {
		t.Fatalf("SetOptions.SQLCacheSize not initialized properly ")
	}
	WithSQLCacheSize(MaxSQLCacheSize + 1)(opts)
	if FetchSQLCacheSize(opts) != MaxSQLCacheSize {
		t.Fatalf("SetOptions.SQLCacheSize not initialized properly ")
	}

	WithKeepAlivePeriod(1 * time.Second)(opts)
	if FetchKeepAlivePeriod(opts) != 1*time.Second {
		t.Fatalf("SetOptions.KeepAlivePeriod not initialized properly ")
	}

	WithKeepAlivePeriod(1234 * time.Minute)(opts)
	if FetchKeepAlivePeriod(opts) != 1234*time.Minute {
		t.Fatalf("SetOptions.KeepAlivePeriod not initialized properly ")
	}
}

func TestOptionsClone(t *testing.T) {

	noti := notifier.NewNotifierMux(
		notifier.NewNotifierLog("./mskeeper.log").SetLogLevel(notifier.WarnLevel),
		notifier.NewDefaultNotifier().SetLogLevel(notifier.WarnLevel))
	opts := NewOptions()
	clone := opts.Clone()
	WithCapacity(1234)(opts)
	WithSwitch(true)(opts)
	WithMaxExecTime(5 * time.Second)(opts)
	WithMaxSilentPeriod(10 * time.Second)(opts)
	WithNotifier(noti)(opts)
	WithLogOutput(os.Stdout)(opts)
	WithLogLevel(notifier.WarnLevel)(opts) // 设置mskeeper自身的日志级别
	WithSQLCacheSize(1321)(opts)
	WithSQLWhiteLists("select * from client_config")(opts)
	WithSQLWhiteLists("select * from charge_config")(opts)

	if FetchCapacity(opts) == FetchCapacity(clone) {
		t.Fatalf("Options.Capacity not cloned properly")
	}

	if reflect.DeepEqual(FetchNotifier(opts), FetchNotifier(clone)) {
		t.Fatalf("Options.Notifier not cloned properly ")
	}

	if FetchSwitch(opts) == FetchSwitch(clone) {
		t.Fatalf("Options.Switch not cloned properly ")
	}

	// Use logrus's global value
	// if FetchLogLevel(opts) == FetchLogLevel(clone) {
	// 	t.Fatalf("Options.LogLevel not cloned properly ")
	// }

	if !reflect.DeepEqual(FetchLogOutput(opts), FetchLogOutput(clone)) {
		t.Fatalf("Options.Output not cloned properly ")
	}

	if FetchMaxExecTime(opts) == FetchMaxExecTime(clone) {
		t.Fatalf("Options.MaxExecTime not initialized properly ")
	}

	if FetchMaxSilentPeriod(opts) == FetchMaxSilentPeriod(clone) {
		t.Fatalf("Options.MaxSilentPeriod not cloned properly ")
	}

	if FetchSQLCacheSize(opts) == FetchSQLCacheSize(clone) {
		t.Fatalf("Options.SQLCacheSize not cloned properly ")
	}

	if CheckIfInSQLWhiteLists(clone, "select * from client_config") {
		t.Fatalf("Options.SQLWhiteLists not cloned properly ")
	}

	clone2 := opts.Clone()
	if CheckIfInSQLWhiteLists(clone, "select * from charge_config") {
		t.Fatalf("Options.SQLWhiteLists not cloned properly ")
	}

	if !CheckIfInSQLWhiteLists(clone2, "select * from charge_config") {
		t.Fatalf("Options.SQLWhiteLists not cloned properly ")
	}

}
