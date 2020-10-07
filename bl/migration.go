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
		err = dao.CountMigrationTx(tx, &count)
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
	err = dao.GetMigration(tx, &version)
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
		_, err = dao.UpdateMigration(tx, version+1)
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
