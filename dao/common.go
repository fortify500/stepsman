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
package dao

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"os"
	"strings"
)

var DB DBI

type DBI interface {
	SQL() *sqlx.DB
	VerifyDBCreation() error
	Migrate0(tx *sqlx.Tx) error
	CreateRun(tx *sqlx.Tx, run interface{}) (int64, error)
}

func OpenDatabase(databaseVendor string, dataSourceName string) error {
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
	return err
}

func Rollback(tx *sqlx.Tx, err error) error {
	err2 := tx.Rollback()
	if err2 != nil {
		err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	}
	return err
}
