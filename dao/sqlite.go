/*
 * Copyright © 2020 stepsman authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dao

import (
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Sqlite3SqlxDB sqlx.DB

func (db *Sqlite3SqlxDB) Notify(_ *sqlx.Tx, _ string, _ string) {
	panic("unsupported operation")
}

func (db *Sqlite3SqlxDB) RecoverSteps(DAO *DAO, tx *sqlx.Tx, _ int) []string {
	result := make([]string, 0)
	rows, err := tx.Queryx(`select uuid from steps where  complete_by<DATETIME(CURRENT_TIMESTAMP, '-10 seconds')`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var step api.StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row - get: %w", err))
		}
		result = append(result, step.UUID)
	}
	_, err = tx.Exec(fmt.Sprintf(`update steps
		set status=$1, heartbeat=CURRENT_TIMESTAMP, complete_by=DATETIME(CURRENT_TIMESTAMP, '+%d seconds')
		where (run_id,"index") in (select run_id, "index" from steps where  complete_by<DATETIME(CURRENT_TIMESTAMP, '-10 seconds'))`, DAO.CompleteByPendingInterval), api.StepPending)
	if err != nil {
		panic(err)
	}
	return result
}

func (db *Sqlite3SqlxDB) VerifyDBCreation(tx *sqlx.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS migration (
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
                                     id varchar(128) PRIMARY KEY NOT NULL,
                                     created_at TIMESTAMP NOT NULL,
                                     template_version INTEGER NOT NULL,
	                                 status INTEGER NOT NULL,
                                     key TEXT NOT NULL,
	                                 template_title TEXT,
	                                 template TEXT
                                     )`)
	_, err = tx.Exec(`CREATE UNIQUE INDEX idx_runs_key ON runs (key)`)
	if err != nil {
		return fmt.Errorf("failed to create index idx_runs_title_status: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX idx_runs_status ON runs (status)`)
	if err != nil {
		return fmt.Errorf("failed to create index idx_runs_status: %w", err)
	}
	_, err = tx.Exec(`CREATE TABLE steps (
                                     run_id varchar(128) NOT NULL,
                                     "index" INTEGER NOT NULL,
                                     uuid varchar(128) NOT NULL,
	                                 name TEXT,
	                                 label TEXT NOT NULL,
	                                 status INTEGER NOT NULL,
	                                 status_owner TEXT NOT NULL,
	                                 heartbeat TIMESTAMP NOT NULL,
									 complete_by TIMESTAMP NULL,
									 retries_left INTEGER NOT NULL,
									 context TEXT NOT NULL, 
	                                 state text,
	                                 PRIMARY KEY (run_id, "index")
                                     )`)
	if err != nil {
		return fmt.Errorf("failed to create database steps table: %w", err)
	}
	_, err = tx.Exec(`CREATE UNIQUE INDEX idx_steps_uuid ON steps (uuid)`)
	if err != nil {
		return fmt.Errorf("failed to create index idx_steps_uuid: %w", err)
	}
	_, err = tx.Exec(`CREATE UNIQUE INDEX idx_steps_run_id_label ON steps (run_id, label)`)
	if err != nil {
		return fmt.Errorf("failed to create index idx_steps_run_id_label: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX index_steps_run_id_status_heartbeat ON steps (run_id, status, heartbeat)`)
	if err != nil {
		return fmt.Errorf("failed to create index index_steps_run_id_status_heartbeat: %w", err)
	}
	return nil
}
func (db *Sqlite3SqlxDB) CreateStepTx(tx *sqlx.Tx, stepRecord *api.StepRecord) {
	query := "INSERT INTO steps(run_id, \"index\", label, uuid, name, status, status_owner, heartbeat, complete_by, retries_left, context, state) values(:run_id,:index,:label,:uuid,:name,:status,:status_owner,0,null,:retries_left,:context,:state)"
	if _, err := tx.NamedExec(query, stepRecord); err != nil {
		panic(err)
	}
}

func (db *Sqlite3SqlxDB) completeByUpdateStatement(completeBy *int64) string {
	return fmt.Sprintf(",complete_by=DATETIME(CURRENT_TIMESTAMP, '+%d seconds')", *completeBy)
}
