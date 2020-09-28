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
)

type PostgreSQLSqlxDB sqlx.DB

func (db *PostgreSQLSqlxDB) CreateRun(tx *sqlx.Tx, runRecord interface{}) (int64, error) {
	stmt, err := tx.PrepareNamed("INSERT INTO runs(uuid, title, cursor, status, script) values(:uuid,:title,:cursor,:status,:script) RETURNING id")
	if err != nil {
		return -1, err
	}
	var id int64
	err = stmt.Get(&id, runRecord)
	return id, err
}

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
	_, err := tx.Exec(`CREATE SEQUENCE "public"."runs_id_seq"
	INCREMENT 1
	MINVALUE 1
	MAXVALUE 9223372036854775807
	START 1;`)
	if err != nil {
		return fmt.Errorf("failed to create sequence runs_id_seq: %w", err)
	}
	_, err = tx.Exec(`CREATE TABLE "public"."runs" ( 
	"id" Bigint NOT NULL DEFAULT nextval('runs_id_seq'),
	"uuid" UUid NOT NULL,
	"title" Text,
	"cursor" Bigint,
	"status" Bigint NOT NULL,
	"script" Text,
	PRIMARY KEY ( "id" ),
	CONSTRAINT "unique_runs_uuid" UNIQUE( "uuid" ) )`)
	if err != nil {
		return fmt.Errorf("failed to create database runs table: %w", err)
	}
	_, err = tx.Exec(`CREATE TABLE "public"."steps" ( 
	"run_id" Bigint NOT NULL,
	"step_id" Bigint NOT NULL,
	"uuid" UUid NOT NULL,
	"name" Text,
	"status" Bigint NOT NULL,
	"heartbeat" Bigint NOT NULL,
	"script" Text,
	PRIMARY KEY ( "run_id", "step_id" ) )`)
	if err != nil {
		return fmt.Errorf("failed to create database steps table: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_runs_status" ON "public"."runs" USING btree( "status" Asc NULLS Last )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_runs_status: %w", err)
	}
	_, err = tx.Exec(`CREATE INDEX "index_runs_title_status" ON "public"."runs" USING btree( "title" Asc NULLS Last, "status" Asc NULLS Last )`)
	if err != nil {
		return fmt.Errorf("failed to create index index_runs_title_status: %w", err)
	}
	return nil
}
