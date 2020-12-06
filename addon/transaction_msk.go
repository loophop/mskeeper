// Go MSKeeper Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2020 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

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
type MSKTx struct {
	*sql.Tx
	msk driver.MSKeeperInter
}

func (tx *MSKTx) Commit() (err error) {

	return tx.Tx.Commit()
}

func (tx *MSKTx) Rollback() (err error) {

	return tx.Tx.Rollback()
}

func (tx *MSKTx) Exec(query string, args ...interface{}) (sql.Result, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer tx.msk.AfterProcess(time.Now(), query, nargs)

	return tx.Tx.Exec(query, args...)
}

func (tx *MSKTx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer tx.msk.AfterProcess(time.Now(), query, nargs)

	return tx.Tx.ExecContext(ctx, query, args...)
}

func (tx *MSKTx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	nargs, _ := converter{}.ConvertValues(args)
	defer tx.msk.AfterProcess(time.Now(), query, nargs)

	return tx.Tx.QueryRowContext(ctx, query, args...)
}

func (tx *MSKTx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer tx.msk.AfterProcess(time.Now(), query, nargs)

	return tx.Tx.QueryContext(ctx, query, args...)
}

func (tx *MSKTx) PrepareContext(ctx context.Context, query string) (*MSKStmt, error) {
	stmt, err := tx.Tx.PrepareContext(ctx, query)
	msStmt := &MSKStmt{msk: tx.msk, querysql: query}

	msStmt.Stmt = stmt
	return msStmt, err
}

func (tx *MSKTx) Prepare(query string) (*MSKStmt, error) {
	stmt, err := tx.Tx.Prepare(query)
	msStmt := &MSKStmt{msk: tx.msk, querysql: query}

	msStmt.Stmt = stmt
	return msStmt, err
}

func (tx *MSKTx) QueryRow(query string, args ...interface{}) *sql.Row {
	nargs, _ := converter{}.ConvertValues(args)
	defer tx.msk.AfterProcess(time.Now(), query, nargs)

	return tx.Tx.QueryRow(query, args...)
}

func (tx *MSKTx) Query(query string, args ...interface{}) (*sql.Rows, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer tx.msk.AfterProcess(time.Now(), query, nargs)

	return tx.Tx.Query(query, args...)
}
