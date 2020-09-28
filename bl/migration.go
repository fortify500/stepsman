package bl

import (
	"fmt"
	"github.com/fortify500/stepsman/dao"
	"github.com/jmoiron/sqlx"
	"os"
	"strings"
)

func MigrateDB() error {
	var version = -1
	err := dao.DB.VerifyDBCreation()
	if err != nil {
		return fmt.Errorf("failed to verify database table creation: %w", err)
	}
	tx, err := dao.DB.SQL().Beginx()
	if err != nil {
		return fmt.Errorf("failed to start a database transaction: %w", err)
	}
	{
		var count = -1
		err = tx.Get(&count, "select count(*) from migration")
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to get database table migration count: %w", err)
		}
		if count == 0 {
			_, err = tx.Exec("insert into migration (id, version) values(1,0)")
			if err != nil {
				err = Rollback(tx, err)
				return fmt.Errorf("failed to add database migration row: %w", err)
			}
		}
	}
	err = tx.Get(&version, "select version from migration where id=1")
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to get database version: %w", err)
	}
	switch version {
	case 0:
		dao.DB.Migrate0(tx)
		if err != nil {
			err = Rollback(tx, err)
			return err
		}
		_, err = tx.Exec("update migration set version=1 where id=1")
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to update database migration row to version 1: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit migration transaction: %w", err)
	}
	return nil
}

func OpenDatabase(databaseVendor string, dataSourceName string) error {
	var err error
	var internalDriverName string
	switch strings.TrimSpace(databaseVendor) {
	case "sqlite":
		internalDriverName = "sqlite3"
		_, err = os.Stat(dataSourceName)
		if err != nil {
			return fmt.Errorf("failed to verify sqlite file existance: %s", dataSourceName)
		}
	case "postgresql":
		internalDriverName = "pgx"
	default:
		return fmt.Errorf("unsupported database vendor name: %s", databaseVendor)
	}
	{
		dbOpen, err := sqlx.Open(internalDriverName, dataSourceName)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		switch internalDriverName {
		case "sqlite3":
			dao.DB = (*dao.Sqlite3SqlxDB)(dbOpen)
		case "pgx":
			dao.DB = (*dao.PostgreSQLSqlxDB)(dbOpen)
		default:
			return fmt.Errorf("unsupported internal database driver name: %s", internalDriverName)
		}
	}
	return err
}
