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
	"github.com/jmoiron/sqlx"
)

type PostgreSQLSqlxDB sqlx.DB

func (db *PostgreSQLSqlxDB) VerifyDBCreation() error {
	err := db.Ping()
	if err != nil {
		return fmt.Errorf("failed to open a database connection: %w", err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS "public"."migration" ( 
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
	"id" UUid NOT NULL,
	"status" Bigint NOT NULL,
	"template_version" BIGINT NOT NULL,
	"key" Text NOT NULL,
	"template_title" Text NOT NULL ,
	"template" jsonb,
	"state" jsonb,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_runs_key" UNIQUE( "key" ) )`)
	if err != nil {
		return fmt.Errorf("failed to create database runs table: %w", err)
	}
	_, err = tx.Exec(`CREATE TABLE "public"."steps" ( 
	"run_id" UUid NOT NULL,
	"index" Bigint NOT NULL,
	"uuid" UUid NOT NULL,
	"status" Bigint NOT NULL,
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
