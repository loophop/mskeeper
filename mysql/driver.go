// Copyright 2012 The MSKeeper Authors. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

// Package mysql provides a MySQL driver for Go's database/sql package.
//
// The driver should be used via the database/sql package:
//
//  import "database/sql"
//  import _ "gitlab.papegames.com/fringe/mskeeper"
//
//  db, err := sql.Open("mskeeper", "user:password@/dbname")
//
// See https:gitlab.papegames.com/fringe/mskeeper for details
package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	mskdriver "gitlab.papegames.com/fringe/mskeeper/driver"
	"log"
	"net"
	"reflect"
	"sync"
)

var (
	ErrMSKeeperInstanceNil = errors.New("mskeeper instance no exists")
)

// Global connector array keeps track of all connections to mysql's driver
var mskDriver *MSKeeperDriver = &MSKeeperDriver{}
var mskInstanceMap sync.Map

// MSKeeperDriver is exported to make the driver directly accessible.
// In general the driver is used via the database/sql package.
type MSKeeperDriver struct {
}

// DialFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDial
//
// Deprecated: users should register a DialContextFunc instead
type DialFunc func(addr string) (net.Conn, error)

// DialContextFunc is a function which can be used to establish the network connection.
// Custom dial functions must be registered with RegisterDialContext
type DialContextFunc func(ctx context.Context, addr string) (net.Conn, error)

var (
	dialsLock sync.RWMutex
	dials     map[string]DialContextFunc
)

// RegisterDialContext registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// The current context for the connection and its address is passed to the dial function.
func RegisterDialContext(net string, dial DialContextFunc) {
	dialsLock.Lock()
	defer dialsLock.Unlock()
	if dials == nil {
		dials = make(map[string]DialContextFunc)
	}
	dials[net] = dial
}

// RegisterDial registers a custom dial function. It can then be used by the
// network address mynet(addr), where mynet is the registered new network.
// addr is passed as a parameter to the dial function.
//
// Deprecated: users should call RegisterDialContext instead
func RegisterDial(network string, dial DialFunc) {
	RegisterDialContext(network, func(_ context.Context, addr string) (net.Conn, error) {
		return dial(addr)
	})
}

// Open new Connection.
// See https://github.com/go-sql-driver/mysql#dsn-data-source-name for how
// the DSN string is formatted
func (d MSKeeperDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}

	formatedDSN := cfg.FormatDSN()
	msk := d.loadMSKeeperInstanceBy(formatedDSN)
	if msk == nil {
		msk = mskdriver.NewMSK(c)
		if err := mskDriver.storeMSKeeperInstanceWith(formatedDSN, msk); err != nil {
			return nil, err
		}
	}
	c.msk = msk

	return c.Connect(context.Background())
}

func init() {
	sql.Register("mskeeper", mskDriver)
}

func MSKeeperInstance(dsn string) *mskdriver.MSKeeper {
	cfg, err := ParseDSN(dsn)
	if err != nil {
		log.Printf("MSKeeperInstance failed to parse dsn %v", dsn)
		return nil
	}

	if msk := mskDriver.loadMSKeeperInstanceBy(cfg.FormatDSN()); msk != nil {
		return msk
	}
	return nil
}

// NewConnector returns new driver.Connector.
func NewConnector(cfg *Config) (driver.Connector, error) {
	cfg = cfg.Clone()
	// normalize the contents of cfg so calls to NewConnector have the same
	// behavior as MSKeeperDriver.OpenConnector
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	c := &connector{cfg: cfg}
	formatedDSN := cfg.FormatDSN()
	msk := mskDriver.loadMSKeeperInstanceBy(formatedDSN)
	if msk == nil {
		msk = mskdriver.NewMSK(c)
		if err := mskDriver.storeMSKeeperInstanceWith(formatedDSN, msk); err != nil {
			return nil, err
		}
	}
	c.msk = msk
	return c, nil
}

// OpenConnector implements driver.DriverContext.
func (d MSKeeperDriver) OpenConnector(dsn string) (driver.Connector, error) {

	cfg, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c := &connector{
		cfg: cfg,
	}
	formatedDSN := cfg.FormatDSN()
	msk := d.loadMSKeeperInstanceBy(formatedDSN)
	if msk == nil {
		msk = mskdriver.NewMSK(c)
		if err := mskDriver.storeMSKeeperInstanceWith(formatedDSN, msk); err != nil {
			return nil, err
		}
	}
	c.msk = msk

	return c, nil
}

// Load the mskeeper instance by formated dsn
func (d MSKeeperDriver) loadMSKeeperInstanceBy(formatedDSN string) *mskdriver.MSKeeper {

	msk, ok := mskInstanceMap.Load(formatedDSN)
	if ok {
		formatedMSK, okk := msk.(*mskdriver.MSKeeper)
		if okk {
			return formatedMSK
		}
	}
	return nil
}

// Load the mskeeper instance by formated dsn
func (d MSKeeperDriver) storeMSKeeperInstanceWith(formatedDSN string, msk *mskdriver.MSKeeper) error {
	actual, ok := mskInstanceMap.LoadOrStore(formatedDSN, msk)
	if !ok {
		if !reflect.DeepEqual(actual, msk) {
			return errors.New("stored sth different with the expected MSKeeper")
		}
	}
	return nil
}
