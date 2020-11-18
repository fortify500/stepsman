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
	"database/sql"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type Sqlite3SqlxDB sqlx.DB

func (db *Sqlite3SqlxDB) Notify(_ *sqlx.Tx, _ string, _ string) {
	panic("unsupported operation")
}

func (db *Sqlite3SqlxDB) RecoverSteps(_ *sqlx.Tx) []string {
	panic("unsupported operation")
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
	                                 status_uuid TEXT NOT NULL,
	                                 heartbeat TIMESTAMP NOT NULL,
									 complete_by TIMESTAMP NULL,
									 retries_left INTEGER NOT NULL,
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
	return nil
}
func (db *Sqlite3SqlxDB) CreateStepTx(tx *sqlx.Tx, stepRecord *api.StepRecord) {
	query := "INSERT INTO steps(run_id, \"index\", label, uuid, name, status, status_uuid, heartbeat, complete_by, retries_left, state) values(:run_id,:index,:label,:uuid,:name,:status,:status_uuid,0,null,:retries_left,:state)"
	if _, err := tx.NamedExec(query, stepRecord); err != nil {
		panic(err)
	}
}

func (db *Sqlite3SqlxDB) UpdateManyStatusAndHeartBeatByUUIDTx(tx *sqlx.Tx, uuids []string, newStatus api.StepStatusType, prevStatus []api.StepStatusType, _ *int64) []UUIDAndStatusUUID {
	var result []UUIDAndStatusUUID
	for _, stepUUID := range uuids {
		uuid4, err := uuid.NewRandom()
		if err != nil {
			panic(fmt.Errorf("failed to generate uuid: %w", err))
		}
		var res sql.Result
		if len(prevStatus) == 1 {
			res, err = tx.Exec("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, status_uuid=$2 where uuid=$3 and status=$4", newStatus, uuid4.String(), stepUUID, prevStatus[0])
		} else {
			res, err = tx.Exec("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, status_uuid=$2 where uuid=$3", newStatus, uuid4.String(), stepUUID)
		}
		if err != nil {
			panic(err)
		}
		var affected int64
		affected, err = res.RowsAffected()
		if err != nil {
			panic(err)
		}
		if affected == 1 {
			result = append(result, UUIDAndStatusUUID{
				UUID:       stepUUID,
				StatusUUID: uuid4.String(),
			})
		}
	}
	return result
}

func (db *Sqlite3SqlxDB) UpdateManyStatusAndHeartBeatTx(tx *sqlx.Tx, runId string, indices []int64, newStatus api.StepStatusType, prevStatus []api.StepStatusType, _ *int64, retriesLeft *int) []UUIDAndStatusUUID {
	var result []UUIDAndStatusUUID
	for _, index := range indices {
		uuid4, err := uuid.NewRandom()
		if err != nil {
			panic(fmt.Errorf("failed to generate uuid: %w", err))
		}
		var res sql.Result
		var setRetriesLeftStr string
		if retriesLeft != nil {
			setRetriesLeftStr = fmt.Sprintf(", retries_left=%d", *retriesLeft)
		}
		if len(prevStatus) == 1 {
			res, err = tx.Exec(fmt.Sprintf("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, status_uuid=$2%s where run_id=$3 and \"index\"=$4 and status=$5", setRetriesLeftStr), newStatus, uuid4.String(), runId, index, prevStatus[0])
		} else {
			res, err = tx.Exec(fmt.Sprintf("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, status_uuid=$2%s where run_id=$3 and \"index\"=$4", setRetriesLeftStr), newStatus, uuid4.String(), runId, index)
		}
		if err != nil {
			panic(err)
		}
		var affected int64
		affected, err = res.RowsAffected()
		if err != nil {
			panic(err)
		}
		if affected == 1 {
			var partialStep *api.StepRecord
			partialStep, err = GetStepTx(tx, runId, index, []string{UUID})
			if err != nil {
				panic(err)
			}
			result = append(result, UUIDAndStatusUUID{
				UUID:       partialStep.UUID,
				StatusUUID: uuid4.String(),
			})
		}
	}
	return result
}

func (db *Sqlite3SqlxDB) UpdateStepStateAndStatusAndHeartBeatTx(tx *sqlx.Tx, runId string, index int64, newStatus api.StepStatusType, newState string, _ *int64, retriesLeft *int) string {
	uuid4, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Errorf("failed to generate uuid: %w", err))
	}
	var setRetriesLeftStr string
	if retriesLeft != nil {
		setRetriesLeftStr = fmt.Sprintf(", retries_left=%d", *retriesLeft)
	}
	res, err := tx.Exec(fmt.Sprintf("update steps set status=$1, state=$2, heartbeat=CURRENT_TIMESTAMP, status_uuid=$3%s where run_id=$4 and \"index\"=$5", setRetriesLeftStr), newStatus, newState, uuid4.String(), runId, index)
	if err != nil {
		panic(err)
	}
	var affected int64
	affected, err = res.RowsAffected()
	if err != nil {
		panic(err)
	}
	if affected != 1 {
		panic(fmt.Errorf("illegal state, cannot retrieve more than 1 step row for update"))
	}
	return uuid4.String()
}
