package bl

import (
	"fmt"
	"github.com/fortify500/stepsman/dao"
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
		err = dao.CountMigrationTx(tx, count)
		if err != nil {
			err = dao.Rollback(tx, err)
			return fmt.Errorf("failed to get database table migration count: %w", err)
		}
		if count == 0 {
			_, err = dao.CreateMigration(tx)
			if err != nil {
				err = dao.Rollback(tx, err)
				return fmt.Errorf("failed to add database migration row: %w", err)
			}
		}
	}
	err = dao.GetMigration(tx, version)
	if err != nil {
		err = dao.Rollback(tx, err)
		return fmt.Errorf("failed to get database version: %w", err)
	}
	switch version {
	case 0:
		dao.DB.Migrate0(tx)
		if err != nil {
			err = dao.Rollback(tx, err)
			return err
		}
		_, err = dao.UpdateMigration(tx)
		if err != nil {
			err = dao.Rollback(tx, err)
			return fmt.Errorf("failed to update database migration row to version 1: %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit migration transaction: %w", err)
	}
	return nil
}
