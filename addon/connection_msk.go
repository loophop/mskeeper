// Go MSKeeper Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2013 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package addon

import (
	"context"
	"database/sql"
	"time"
)

func (mska *Addon) Begin() (*MSKTx, error) {
	tx, err := mska.db.Begin()
	msTx := &MSKTx{msk: mska.msk}

	msTx.Tx = tx
	return msTx, err
}

func (mska *Addon) BeginTx(ctx context.Context, opts *sql.TxOptions) (*MSKTx, error) {
	tx, err := mska.db.BeginTx(ctx, opts)
	msTx := &MSKTx{msk: mska.msk}

	msTx.Tx = tx
	return msTx, err
}

func (mska *Addon) Close() error {

	return mska.db.Close()
}

func (mska *Addon) Conn(ctx context.Context) (*MSKConn, error) {
	con, err := mska.db.Conn(ctx)
	mskconn := &MSKConn{msk: mska.msk}
	mskconn.Conn = con
	if err != nil {
		return nil, err
	}
	return mskconn, nil
}

func (mska *Addon) Ping() error {

	return mska.db.Ping()
}

func (mska *Addon) PingContext(ctx context.Context) error {

	return mska.db.PingContext(ctx)
}

func (mska *Addon) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {

	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.ExecContext(ctx, query, args...)
}

func (mska *Addon) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {

	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.QueryContext(ctx, query, args...)
}

func (mska *Addon) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {

	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.QueryRowContext(ctx, query, args...)
}

func (mska *Addon) PrepareContext(ctx context.Context, query string) (*MSKStmt, error) {
	stmt, err := mska.db.PrepareContext(ctx, query)
	msStmt := &MSKStmt{msk: mska.msk, querysql: query}

	msStmt.Stmt = stmt
	return msStmt, err
}

func (mska *Addon) Prepare(query string) (*MSKStmt, error) {
	stmt, err := mska.db.Prepare(query)
	msStmt := &MSKStmt{msk: mska.msk, querysql: query}

	msStmt.Stmt = stmt
	return msStmt, err
}

func (mska *Addon) QueryRow(query string, args ...interface{}) *sql.Row {
	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.QueryRow(query, args...)
}

func (mska *Addon) Query(query string, args ...interface{}) (*sql.Rows, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.Query(query, args...)
}

func (mska *Addon) Exec(query string, args ...interface{}) (sql.Result, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer mska.msk.AfterProcess(time.Now(), query, nargs)

	return mska.db.Exec(query, args...)
}
