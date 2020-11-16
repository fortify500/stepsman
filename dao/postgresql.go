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
)

type PostgreSQLSqlxDB sqlx.DB

func (db *PostgreSQLSqlxDB) VerifyDBCreation(tx *sqlx.Tx) error {
	_, err := tx.Exec(`CREATE TABLE IF NOT EXISTS "public"."migration" ( 
	"id" Bigint NOT NULL,
	"version" Bigint NOT NULL,
	PRIMARY KEY ( "id" ) );`)
	return err
}

func (db *PostgreSQLSqlxDB) SQL() *sqlx.DB {
	return (*sqlx.DB)(db)
}
func (db *PostgreSQLSqlxDB) Migrate0(tx *sqlx.Tx) error {
	_, err := tx.Exec(`CREATE TABLE "public"."runs" ( 
	"id" uuid NOT NULL,
	"status" Bigint NOT NULL,
	"template_version" BIGINT NOT NULL,
	"key" Text NOT NULL,
	"template_title" Text NOT NULL ,
	"template" jsonb,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_runs_key" UNIQUE( "key" ) )`)
	if err != nil {
		return fmt.Errorf("failed to create database runs table: %w", err)
	}
	_, err = tx.Exec(`CREATE TABLE "public"."steps" ( 
	"run_id" uuid NOT NULL,
	"index" Bigint NOT NULL,
	"uuid" uuid NOT NULL,
	"status" Bigint NOT NULL,
	"label" Text NOT NULL,
	"name" Text,
	"status_uuid" uuid NOT NULL,
	"heartbeat" TIMESTAMP NOT NULL,
	"complete_by" TIMESTAMP NULL,
	"retries_left" INTEGER NOT NULL,
	"state" jsonb,
	PRIMARY KEY ( "run_id", "index" ),
	CONSTRAINT "foreign_key_runs" FOREIGN KEY(run_id) REFERENCES runs(id),
    CONSTRAINT "unique_steps_uuid" UNIQUE( "uuid" ),
    CONSTRAINT "unique_steps_label" UNIQUE( "run_id", "label" ) )`)
	if err != nil {
		return fmt.Errorf("failed to create database steps table: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_runs_status" ON "public"."runs" USING btree( "status" Asc NULLS Last )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_runs_status: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_steps_complete_by" ON "public"."steps" USING btree( "complete_by" Asc NULLS Last )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_steps_complete_by: %w", err)
	}
	return nil
}

func (db *PostgreSQLSqlxDB) CreateStepTx(tx *sqlx.Tx, stepRecord *api.StepRecord) {
	if _, err := tx.NamedExec("INSERT INTO steps(run_id, \"index\", label, uuid, name, status, status_uuid, heartbeat, complete_by, retries_left, state) values(:run_id,:index,:label,:uuid,:name,:status,:status_uuid,to_timestamp(0),null,:retries_left,:state)", stepRecord); err != nil {
		panic(err)
	}
}

func (db *PostgreSQLSqlxDB) UpdateManyStatusAndHeartBeatByUUIDTx(tx *sqlx.Tx, uuids []string, newStatus api.StepStatusType, prevStatus []api.StepStatusType, completeBy *int64) []UUIDAndStatusUUID {
	var result []UUIDAndStatusUUID
	for _, stepUUID := range uuids {
		uuid4, err := uuid.NewRandom()
		if err != nil {
			panic(fmt.Errorf("failed to generate uuid: %w", err))
		}
		var res sql.Result
		var completeByStr string
		if completeBy != nil {
			completeByStr = fmt.Sprintf("CURRENT_TIMESTAMP + INTERVAL '%d seconds'", *completeBy)
		} else {
			completeByStr = "null"
		}
		if len(prevStatus) == 1 {
			res, err = tx.Exec(fmt.Sprintf("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, complete_by=%s, status_uuid=$2 where uuid=$3 and status=$4", completeByStr), newStatus, uuid4.String(), stepUUID, prevStatus[0])
		} else {
			res, err = tx.Exec(fmt.Sprintf("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, complete_by=%s, status_uuid=$2 where uuid=$3", completeByStr), newStatus, uuid4.String(), stepUUID)
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

func (db *PostgreSQLSqlxDB) UpdateManyStatusAndHeartBeatTx(tx *sqlx.Tx, runId string, indices []int64, newStatus api.StepStatusType, prevStatus []api.StepStatusType, completeBy *int64, retriesLeft *int) []UUIDAndStatusUUID {
	var result []UUIDAndStatusUUID
	for _, index := range indices {
		uuid4, err := uuid.NewRandom()
		if err != nil {
			panic(fmt.Errorf("failed to generate uuid: %w", err))
		}
		var res sql.Result
		var completeByStr string
		var setRetriesLeftStr string
		if completeBy != nil {
			completeByStr = fmt.Sprintf("CURRENT_TIMESTAMP + INTERVAL '%d seconds'", *completeBy)
		} else {
			completeByStr = "null"
		}
		if retriesLeft != nil {
			setRetriesLeftStr = fmt.Sprintf(", retries_left=%d", *retriesLeft)
		}
		if len(prevStatus) == 1 {
			res, err = tx.Exec(fmt.Sprintf("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, complete_by=%s, status_uuid=$2%s where run_id=$3 and \"index\"=$4 and status=$5", completeByStr, setRetriesLeftStr), newStatus, uuid4.String(), runId, index, prevStatus[0])
		} else {
			res, err = tx.Exec(fmt.Sprintf("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP, complete_by=%s, status_uuid=$2%s where run_id=$3 and \"index\"=$4", completeByStr, setRetriesLeftStr), newStatus, uuid4.String(), runId, index)
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

func (db *PostgreSQLSqlxDB) UpdateStepStateAndStatusAndHeartBeatTx(tx *sqlx.Tx, runId string, index int64, newStatus api.StepStatusType, newState string, completeBy *int64, retriesLeft *int) string {
	uuid4, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Errorf("failed to generate uuid: %w", err))
	}
	var completeByStr string
	var setRetriesLeftStr string
	if completeBy != nil {
		completeByStr = fmt.Sprintf("CURRENT_TIMESTAMP + INTERVAL '%d seconds'", *completeBy)
	} else {
		completeByStr = "null"
	}
	if retriesLeft != nil {
		setRetriesLeftStr = fmt.Sprintf(", retries_left=%d", *retriesLeft)
	}
	res, err := tx.Exec(fmt.Sprintf("update steps set status=$1, state=$2, heartbeat=CURRENT_TIMESTAMP, complete_by=%s, status_uuid=$3%s where run_id=$4 and \"index\"=$5", completeByStr, setRetriesLeftStr), newStatus, newState, uuid4.String(), runId, index)
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
func (db *PostgreSQLSqlxDB) Notify(tx *sqlx.Tx, channel string, message string) {
	_, err := tx.Exec(`SELECT pg_notify($1, $2)`, channel, message)
	if err != nil {
		panic(err)
	}
}
func (db *PostgreSQLSqlxDB) RecoverSteps(tx *sqlx.Tx) []string {
	var result []string
	rows, err := tx.Queryx(fmt.Sprintf(`with R as (select run_id, "index" from steps
		where  complete_by<(NOW() - interval '10 second')
		FOR UPDATE SKIP LOCKED)

		update steps
		set status=$1, heartbeat=CURRENT_TIMESTAMP, complete_by=CURRENT_TIMESTAMP + INTERVAL '%d second'
		FROM R
		where steps.run_id = R.run_id and steps.index = R.index
		RETURNING steps.UUID`, CompleteByPendingInterval), api.StepPending)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var stepUUID string
		err = rows.Scan(&stepUUID)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row - get: %w", err))
		}
		result = append(result, stepUUID)
	}
	return result
}
