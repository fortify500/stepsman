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
	"status_uuid" uuid NOT NULL,
	"heartbeat" TIMESTAMP NOT NULL,
	"label" Text NOT NULL,
	"name" Text,
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
	return nil
}

func (db *PostgreSQLSqlxDB) CreateStepTx(tx *sqlx.Tx, stepRecord *StepRecord) (sql.Result, error) {
	return tx.NamedExec("INSERT INTO steps(run_id, \"index\", label, uuid, name, status, status_uuid, heartbeat, state) values(:run_id,:index,:label,:uuid,:name,:status,:status_uuid,to_timestamp(0),:state)", stepRecord)
}

func (db *PostgreSQLSqlxDB) ListStepsTx(tx *sqlx.Tx, runId string, rows *sqlx.Rows, err error) (*sqlx.Rows, error) {
	rows, err = tx.Queryx("SELECT *,CURRENT_TIMESTAMP as now FROM steps WHERE run_id=$1", runId)
	return rows, err
}
