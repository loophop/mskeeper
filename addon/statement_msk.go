// Go MSKeeper Driver - A MySQL-Driver for Go's database/sql package
//
// Copyright 2020 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package addon

import (
	"database/sql"
	"gitlab.papegames.com/fringe/mskeeper/driver"
	"time"
)

type MSKStmt struct {
	*sql.Stmt
	querysql string
	msk      driver.MSKeeperInter
}

func (msks *MSKStmt) Close() error {

	return msks.Stmt.Close()
}

func (msks *MSKStmt) Exec(args ...interface{}) (sql.Result, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer msks.msk.AfterProcess(time.Now(), msks.querysql, nargs)

	return msks.Stmt.Exec(args...)
}

func (msks *MSKStmt) QueryRow(args ...interface{}) *sql.Row {
	nargs, _ := converter{}.ConvertValues(args)
	defer msks.msk.AfterProcess(time.Now(), msks.querysql, nargs)

	return msks.Stmt.QueryRow(args...)
}

func (msks *MSKStmt) Query(args ...interface{}) (*sql.Rows, error) {
	nargs, _ := converter{}.ConvertValues(args)
	defer msks.msk.AfterProcess(time.Now(), msks.querysql, nargs)

	return msks.Stmt.Query(args...)
}
