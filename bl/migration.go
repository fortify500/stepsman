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
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

const CodeDatabaseVersion = 1

func (b *BL) MigrateDB(autoMigrate bool) error {
	if !autoMigrate {
		defer func() {
			if p := recover(); p != nil {
				log.Warn("skipping migration due to failure")
			}
		}()
	}
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		var version = -1
		var err error
		if autoMigrate {
			err = b.DAO.DB.VerifyDBCreation(tx)
			if err != nil {
				return fmt.Errorf("failed to verify database table creation: %w", err)
			}
			var count = -1
			err = dao.CountMigrationTx(tx, &count)
			if err != nil {
				return fmt.Errorf("failed to get database table migration count: %w", err)
			}
			if count == 0 {
				_, err = dao.CreateMigration(tx)
				if err != nil {
					return fmt.Errorf("failed to add database migration row: %w", err)
				}
			}
		}
		err = dao.GetMigration(tx, &version)
		if err != nil {
			return fmt.Errorf("failed to get database version: %w", err)
		}
		if version > CodeDatabaseVersion {
			return fmt.Errorf("database version is greater then this distribution code version - aborting")
		}
		if autoMigrate {
			switch version {
			case 0:
				err = b.DAO.DB.Migrate0(tx)
				if err != nil {
					return fmt.Errorf("failed to migrate db: %w", err)
				}
				_, err = dao.UpdateMigration(tx, version+1)
				if err != nil {
					return fmt.Errorf("failed to update database migration row to version 1: %w", err)
				}
			}
		} else if version < CodeDatabaseVersion {
			return fmt.Errorf("database version is lower then this distribution code version and db-auto-migrate=false - aborting")
		}

		return nil
	})
	return tErr
}
