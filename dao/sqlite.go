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
	_ "github.com/mattn/go-sqlite3"
)

type Sqlite3SqlxDB sqlx.DB

func (db *Sqlite3SqlxDB) CreateRun(tx *sqlx.Tx, runRecord interface{}) (int64, error) {
	exec, err := tx.NamedExec("INSERT INTO runs(uuid, title, cursor, status, script) values(:uuid,:title,:cursor,:status,:script)", runRecord)
	if err != nil {
		return -1, fmt.Errorf("failed to insert database runs row: %w", err)
	}
	id, err := exec.LastInsertId()
	if err != nil {
		return -1, fmt.Errorf("failed to retrieve database runs row autoincremented id: %w", err)
	}
	return id, nil
}

func (db *Sqlite3SqlxDB) VerifyDBCreation() error {
	_, err := db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		return fmt.Errorf("failed to set journal mode: %w", err)
	}
	_, err = db.Exec("PRAGMA synchronous = NORMAL")
	if err != nil {
		return fmt.Errorf("failed to set synchronous mode: %w", err)
	}
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("failed to open a database connection: %w", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS migration (
    id INTEGER PRIMARY KEY NOT NULL,
	version INTEGER NOT NULL
    );`)
	return err
}

func (db *Sqlite3SqlxDB) SQL() *sqlx.DB {
	return (*sqlx.DB)(db)
}
func (db *Sqlite3SqlxDB) Migrate0(tx *sqlx.Tx) error {
	_, err := tx.Exec(`CREATE TABLE runs (
                                     id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                     uuid TEXT NOT NULL,
	                                 title TEXT,
	                                 cursor INTEGER,
	                                 status INTEGER NOT NULL,
	                                 script TEXT
                                     )`)
	if err != nil {
		return fmt.Errorf("failed to create database runs table: %w", err)
	}
	_, err = tx.Exec(`CREATE TABLE steps (
                                     run_id INTEGER NOT NULL,
                                     step_id INTEGER NOT NULL,
                                     uuid TEXT NOT NULL,
	                                 name TEXT,
	                                 status INTEGER NOT NULL,
	                                 heartbeat INTEGER NOT NULL,
	                                 script TEXT,
	                                 PRIMARY KEY (run_id, step_id)
                                     )`)
	if err != nil {
		return fmt.Errorf("failed to create database steps table: %w", err)
	}
	_, err = tx.Exec(`CREATE UNIQUE INDEX idx_runs_uuid ON runs (uuid)`)
	if err != nil {
		return fmt.Errorf("failed to create index idx_runs_title_status: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX idx_runs_title_status ON runs (title, status)`)
	if err != nil {
		return fmt.Errorf("failed to create index idx_runs_title_status: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX idx_runs_status ON runs (status)`)
	if err != nil {
		return fmt.Errorf("failed to create index idx_runs_status: %w", err)
	}
	return nil
}
