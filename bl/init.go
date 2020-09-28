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
)

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

func InitBL(databaseVendor string, dataSourceName string) error {
	if databaseVendor != "remote" {
		IsRemote = false
		err := OpenDatabase(databaseVendor, dataSourceName)
		if err != nil {
			return err
		}
		err = MigrateDB()
		if err != nil {
			return err
		}
	} else {
		IsRemote = true
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
