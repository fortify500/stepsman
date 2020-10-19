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
	"github.com/jmoiron/sqlx"
)

func CountMigrationTx(tx *sqlx.Tx, count *int) error {
	return tx.Get(count, "select count(*) from migration")
}

func UpdateMigration(tx *sqlx.Tx, version int) (sql.Result, error) {
	return tx.Exec("update migration set version=$1 where id=1", version)
}

func GetMigration(tx *sqlx.Tx, version *int) error {
	return tx.Get(version, "select version from migration where id=1")
}

func CreateMigration(tx *sqlx.Tx) (sql.Result, error) {
	return tx.Exec("insert into migration (id, version) values(1,0)")
}
