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
	"github.com/fortify500/stepsman/dao"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"strings"
)

var DB DBI

type DBI interface {
	SQL() *sqlx.DB
	VerifyDBCreation() error
	Migrate0(tx *sqlx.Tx) error
}

func InitBL(driverName string, dataSourceName string) error {
	err := migrateDB(driverName, dataSourceName)
	if err != nil {
		return err
	}
	return nil
}

func migrateDB(driverName string, dataSourceName string) error {
	var version = -1
	var err error
	driverName = strings.TrimSpace(driverName)
	switch driverName {
	case "sqlite3":
	//case "postgres":
	default:
		return fmt.Errorf("unsupported driver name: %s", driverName)
	}
	{
		dbOpen, err := sqlx.Open(driverName, dataSourceName)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		switch driverName {
		case "sqlite3":
		//case "postgres":
		default:
			return fmt.Errorf("unsupported driver name: %s", driverName)
		}
		DB = (*dao.Sqlite3SqlxDB)(dbOpen)
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
