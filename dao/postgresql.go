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
    "group_id" uuid NOT NULL,
	"created_at" timestamp with time zone NOT NULL,
    "id" uuid NOT NULL,
	"status" Bigint NOT NULL,
	"template_version" BIGINT NOT NULL,
	"complete_by" timestamp with time zone NULL,
	"key" Text NOT NULL,
	"template_title" Text NOT NULL ,
	"tags" jsonb NOT NULL,
	"template" jsonb,
	PRIMARY KEY ( "group_id", "created_at", "id" ),
	CONSTRAINT "unique_runs_key" UNIQUE( "key" ) )`)
	if err != nil {
		return fmt.Errorf("failed to create database runs table: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_runs_complete_by" ON "public"."runs" USING btree( "complete_by" Asc NULLS Last )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_runs_complete_by: %w", err)
	}
	_, err = tx.Exec(`CREATE TABLE "public"."steps" (
    "group_id" uuid NOT NULL,
    "created_at" timestamp with time zone NOT NULL,
	"run_id" uuid NOT NULL,
	"index" Bigint NOT NULL,
	"uuid" uuid NOT NULL,
	"status" Bigint NOT NULL,
	"label" Text NOT NULL,
	"tags" jsonb NOT NULL,
	"name" Text,
	"complete_by" timestamp with time zone NULL,
	"heartbeat" timestamp with time zone NOT NULL,
	"retries_left" INTEGER NOT NULL,
	"status_owner" Text NOT NULL,
	"context" jsonb NOT NULL, 
	"state" jsonb,
	PRIMARY KEY ("group_id","created_at", "run_id", "index" ),
	CONSTRAINT "foreign_key_runs" FOREIGN KEY("group_id","created_at","run_id") REFERENCES runs("group_id","created_at","id"),
    CONSTRAINT "unique_steps_uuid" UNIQUE( "uuid" ),
    CONSTRAINT "unique_steps_label" UNIQUE( "run_id", "label" ) )`)
	if err != nil {
		return fmt.Errorf("failed to create database steps table: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_runs_status" ON "public"."runs" USING btree( "group_id" Asc, "status" Asc NULLS Last )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_runs_status: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_steps_complete_by" ON "public"."steps" USING btree( "complete_by" Asc NULLS Last )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_steps_complete_by: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_steps_run_id_status_heartbeat" ON "public"."steps" USING btree( "group_id" Asc, "run_id" Asc, "status" Asc, "heartbeat" Asc )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_steps_run_id_status_heartbeat: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_runs_tags" ON "public"."runs" USING gin( "tags" )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_runs_tags: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_steps_tags" ON "public"."steps" USING gin( "tags" )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_steps_tags: %w", err)
	}
	return nil
}

func (db *PostgreSQLSqlxDB) CreateStepTx(tx *sqlx.Tx, stepRecord *api.StepRecord) {
	if _, err := tx.NamedExec("INSERT INTO steps(group_id, created_at, run_id, \"index\", label, uuid, name, status, status_owner, heartbeat, complete_by, retries_left, context, state, tags) values(:group_id,CURRENT_TIMESTAMP,:run_id,:index,:label,:uuid,:name,:status,:status_owner,to_timestamp(0),null,:retries_left,:context, :state, :tags)", stepRecord); err != nil {
		panic(err)
	}
}

func (db *PostgreSQLSqlxDB) CreateRunTx(tx *sqlx.Tx, runRecord interface{}, completeBy int64) {
	completeByStr := "NULL"
	if completeBy > 0 {
		completeByStr = fmt.Sprintf("CURRENT_TIMESTAMP + INTERVAL '%d seconds'", completeBy)
	}
	query := fmt.Sprintf("INSERT INTO runs(group_id, id, key, template_version, template_title, status, created_at, complete_by, tags, template) values(:group_id,:id,:key,:template_version,:template_title,:status,CURRENT_TIMESTAMP,%s,:tags,:template)", completeByStr)
	if _, err := tx.NamedExec(query, runRecord); err != nil {
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
func (db *PostgreSQLSqlxDB) RecoverSteps(DAO *DAO, tx *sqlx.Tx, limit int, disableSkipLocks bool) []UUIDAndGroupId {
	var result []UUIDAndGroupId
	skipLock := "SKIP LOCKED"
	if disableSkipLocks {
		skipLock = ""
	}
	query := fmt.Sprintf(`with R as (select group_id, created_at, run_id, "index" from steps
		where  complete_by<(NOW() - interval '10 second')
		FOR UPDATE %s LIMIT $1)

		update steps
		set status=$2, heartbeat=CURRENT_TIMESTAMP, complete_by=CURRENT_TIMESTAMP + INTERVAL '%d second'
		FROM R
		where steps.group_id = R.group_id and steps.created_at = R.created_at and steps.run_id = R.run_id and steps.index = R.index
		RETURNING steps.group_id,steps.UUID`, skipLock, DAO.CompleteByPendingInterval)
	rows, err := tx.Queryx(query, limit, api.StepPending)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var uuidAndGroupId UUIDAndGroupId
		err = rows.StructScan(&uuidAndGroupId)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row - RecoverSteps: %w", err))
		}
		result = append(result, uuidAndGroupId)
	}
	return result
}

func (db *PostgreSQLSqlxDB) GetAndUpdateExpiredRuns(DAO *DAO, tx *sqlx.Tx, limit int, disableSkipLocks bool) []IdAndGroupId {
	var result []IdAndGroupId
	skipLock := "SKIP LOCKED"
	if disableSkipLocks {
		skipLock = ""
	}
	query := fmt.Sprintf(`with R as (select group_id, created_at, id from runs
		where  complete_by<(NOW() - interval '10 second')
		FOR UPDATE %s LIMIT $1)

		update runs
		set complete_by=CURRENT_TIMESTAMP + INTERVAL '%d second'
		FROM R
		where runs.group_id = R.group_id and runs.created_at = R.created_at and runs.id = R.id
		RETURNING runs.group_id,runs.id`, skipLock, DAO.CompleteByPendingInterval)
	rows, err := tx.Queryx(query, limit)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var idAndGroupId IdAndGroupId
		err = rows.StructScan(&idAndGroupId)
		if err != nil {
			panic(fmt.Errorf("failed to parse database runs row - GetAndUpdateExpiredRuns: %w", err))
		}
		result = append(result, idAndGroupId)
	}
	return result
}
