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
	"created_at" TIMESTAMP NOT NULL,
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
	"complete_by" TIMESTAMP NULL,
	"heartbeat" TIMESTAMP NOT NULL,
	"retries_left" INTEGER NOT NULL,
	"status_owner" Text NOT NULL,
	"context" jsonb NOT NULL, 
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
	if _, err := tx.NamedExec("INSERT INTO steps(run_id, \"index\", label, uuid, name, status, status_owner, heartbeat, complete_by, retries_left, context, state) values(:run_id,:index,:label,:uuid,:name,:status,:status_owner,to_timestamp(0),null,:retries_left,:context, :state)", stepRecord); err != nil {
		panic(err)
	}
}

func (db *PostgreSQLSqlxDB) completeByUpdateStatement(completeBy *int64) string {
	return fmt.Sprintf(",complete_by=CURRENT_TIMESTAMP + INTERVAL '%d seconds'", *completeBy)
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
