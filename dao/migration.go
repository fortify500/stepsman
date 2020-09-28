package dao

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
)

func CountMigrationTx(tx *sqlx.Tx, count int) error {
	return tx.Get(&count, "select count(*) from migration")
}

func UpdateMigration(tx *sqlx.Tx) (sql.Result, error) {
	return tx.Exec("update migration set version=1 where id=1")
}

func GetMigration(tx *sqlx.Tx, version int) error {
	return tx.Get(&version, "select version from migration where id=1")
}

func CreateMigration(tx *sqlx.Tx) (sql.Result, error) {
	return tx.Exec("insert into migration (id, version) values(1,0)")
}
