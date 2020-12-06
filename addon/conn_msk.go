package addon

import (
	"context"
	"database/sql"
	"gitlab.papegames.com/fringe/mskeeper/driver"
	"time"
)

/*

// Tx is a transaction.
type Tx interface {
	Commit() error
	Rollback() error
}

*/
type MSKConn struct {
	*sql.Conn
	msk driver.MSKeeperInter
}

func (mskc *MSKConn) BeginTx(ctx context.Context, opts *sql.TxOptions) (*MSKTx, error) {
	tx, err := mskc.Conn.BeginTx(ctx, opts)
	msTx := &MSKTx{msk: mskc.msk}

	msTx.Tx = tx
	return msTx, err
}

func (mskc *MSKConn) PrepareContext(ctx context.Context, query string) (*MSKStmt, error) {
	stmt, err := mskc.Conn.PrepareContext(ctx, query)
	msStmt := &MSKStmt{msk: mskc.msk, querysql: query}

	msStmt.Stmt = stmt
	return msStmt, err
}

func (mskc *MSKConn) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	nargs, _ := converter{}.ConvertValues(args)
	defer mskc.msk.AfterProcess(time.Now(), query, nargs)

	return mskc.Conn.QueryRowContext(ctx, query, args...)
}

func (mskc *MSKConn) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer mskc.msk.AfterProcess(time.Now(), query, nargs)

	return mskc.Conn.QueryContext(ctx, query, args...)
}

func (mskc *MSKConn) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer mskc.msk.AfterProcess(time.Now(), query, nargs)

	return mskc.Conn.ExecContext(ctx, query, args...)
}

func (mskc *MSKConn) PingContext(ctx context.Context) error {

	return mskc.Conn.PingContext(ctx)
}

func (mskc *MSKConn) Close() error {
	return mskc.Conn.Close()
}
