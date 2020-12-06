package addon

import (
	"database/sql"
	sqldriver "database/sql/driver"
	"gitlab.papegames.com/fringe/mskeeper/driver"
	"gitlab.papegames.com/fringe/mskeeper/options"
	"gitlab.papegames.com/fringe/mskeeper/policy"
	"time"
)

type Addon struct {
	db  *sql.DB
	msk driver.MSKeeperInter
}

func NewMSKeeperAddon(db *sql.DB, opts ...options.Option) *Addon {

	mska := &Addon{db: db, msk: driver.NewMSKeeperInstance(db, opts...)}
	return mska
}

func (a *Addon) AttachPolicy(policy policy.PolicyChecker) error {
	return a.msk.AttachPolicy(policy)
}

func (a *Addon) RawDB() *sql.DB {
	return a.msk.RawDB()
}

func (a *Addon) ClearPolicies() {
	a.msk.ClearPolicies()
}

func (a *Addon) ResetOptions(opts *options.Options) {
	a.msk.ResetOptions(opts)
}

func (a *Addon) ResyncInfoQueue() {
	a.msk.ResyncInfoQueue()
}

func (a *Addon) SetOption(o options.Option) {
	a.msk.SetOption(o)
}

func (a *Addon) GetOptions() *options.Options {
	return a.msk.GetOptions()
}

func (a *Addon) SetOptions(opts ...options.Option) {
	a.msk.SetOptions(opts...)
}

func (a *Addon) GetErr() []driver.NotifyInfo {
	return a.msk.GetErr()
}

func (a *Addon) Flush() error {
	return a.msk.Flush()
}

func (a *Addon) SyncProcess(t time.Time, query string, args []sqldriver.Value, reterrors *[]error) error {
	return a.msk.SyncProcess(t, query, args, reterrors)
}

func (a *Addon) ClearErr() {
	a.msk.ClearErr()
}

func (a *Addon) HasErr(errCode policy.PolicyCode) bool {
	return a.msk.HasErr(errCode)
}

func (a *Addon) ResyncPingTimer() {
	a.msk.ResyncPingTimer()
}
