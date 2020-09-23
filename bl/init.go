/*
Copyright © 2020 stepsman authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bl

import (
	"fmt"
	_ "github.com/jackc/pgx/stdlib"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strings"
)

var DB DBI
var IsRemote = false

type Error struct {
	msg  string
	code int64
	err  error
}

func WrapBLError(msg string, code int64, err error, args []interface{}) error {
	var errMsg string
	if err != nil {
		errMsg = fmt.Errorf(msg, args...).Error()
	} else {
		errMsg = fmt.Sprintf(msg, args)
	}
	return &Error{
		msg:  errMsg,
		code: code,
		err:  err,
	}
}
func (e *Error) Error() string {
	return e.msg
}
func (e *Error) Code() int64 {
	return e.code
}
func (e *Error) Unwrap() error {
	return e.err
}

type DBI interface {
	SQL() *sqlx.DB
	VerifyDBCreation() error
	Migrate0(tx *sqlx.Tx) error
	CreateRun(tx *sqlx.Tx, run interface{}) (int64, error)
}

func InitBL(databaseVendor string, dataSourceName string) error {
	if databaseVendor != "remote" {
		IsRemote = false
		err := migrateDB(databaseVendor, dataSourceName)
		if err != nil {
			return err
		}
	} else {
		IsRemote = true
	}
	return nil
}

func migrateDB(databaseVendor string, dataSourceName string) error {
	var version = -1
	var err error
	var internalDriverName string
	switch strings.TrimSpace(databaseVendor) {
	case "sqlite":
		internalDriverName = "sqlite3"
		_, err = os.Stat(dataSourceName)
		if err != nil {
			return fmt.Errorf("failed to verify sqlite file existance: %s", dataSourceName)
		}
	case "postgresql":
		internalDriverName = "pgx"
	default:
		return fmt.Errorf("unsupported database vendor name: %s", databaseVendor)
	}
	{
		dbOpen, err := sqlx.Open(internalDriverName, dataSourceName)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		switch internalDriverName {
		case "sqlite3":
			DB = (*Sqlite3SqlxDB)(dbOpen)
		case "pgx":
			DB = (*PostgreSQLSqlxDB)(dbOpen)
		default:
			return fmt.Errorf("unsupported internal database driver name: %s", internalDriverName)
		}
	}

	err = DB.VerifyDBCreation()
	if err != nil {
		return fmt.Errorf("failed to verify database table creation: %w", err)
	}
	tx, err := DB.SQL().Beginx()
	if err != nil {
		return fmt.Errorf("failed to start a database transaction: %w", err)
	}
	{
		var count = -1
		err = tx.Get(&count, "select count(*) from migration")
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to get database table migration count: %w", err)
		}
		if count == 0 {
			_, err = tx.Exec("insert into migration (id, version) values(1,0)")
			if err != nil {
				err = Rollback(tx, err)
				return fmt.Errorf("failed to add database migration row: %w", err)
			}
		}
	}
	err = tx.Get(&version, "select version from migration where id=1")
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to get database version: %w", err)
	}
	switch version {
	case 0:
		DB.Migrate0(tx)
		if err != nil {
			err = Rollback(tx, err)
			return err
		}
		_, err = tx.Exec("update migration set version=1 where id=1")
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to update database migration row to version 1: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit migration transaction: %w", err)
	}
	return nil
}

func Rollback(tx *sqlx.Tx, err error) error {
	err2 := tx.Rollback()
	if err2 != nil {
		err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	}
	return err
}
