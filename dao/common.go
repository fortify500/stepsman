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
	"bytes"
	"database/sql"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"os"
	"runtime/debug"
	"strings"
)

var GitCommit string

const (
	Id              = "id"
	RunId           = "run-id"
	Key             = "key"
	Template        = "template"
	TemplateVersion = "template-version"
	TemplateTitle   = "template-title"
	Status          = "status"
	Index           = "index"
	RetriesLeft     = "retries-left"
	CreatedAt       = "created-at"
	UUID            = "uuid"
	StatusOwner     = "status-owner"
	HeartBeat       = "heartbeat"
	CompleteBy      = "complete-by"
	Now             = "now"
	State           = "state"
	Label           = "label"
	Name            = "name"
)

type DBI interface {
	SQL() *sqlx.DB
	VerifyDBCreation(tx *sqlx.Tx) error
	CreateStepTx(tx *sqlx.Tx, stepRecord *api.StepRecord)
	Migrate0(tx *sqlx.Tx) error
	completeByUpdateStatement(completeBy *int64) string
	RecoverSteps(DAO *DAO, tx *sqlx.Tx, limit int) []string
	Notify(tx *sqlx.Tx, channel string, message string)
}

func (d *DAO) openDatabase(databaseVendor string, dataSourceName string) error {
	var err error
	var internalDriverName string
	switch strings.TrimSpace(databaseVendor) {
	case "sqlite":
		internalDriverName = "sqlite3"
		if dataSourceName != ":memory:" {
			_, err = os.Stat(dataSourceName)
			if err != nil {
				return fmt.Errorf("failed to verify sqlite file existance: %s", dataSourceName)
			}
		} else {
			dataSourceName = fmt.Sprintf("%s?cache=shared", dataSourceName)
		}
	case "postgresql":
		internalDriverName = "pgx"
	default:
		return fmt.Errorf("unsupported database vendor name: %s", databaseVendor)
	}
	{
		var dbOpen *sqlx.DB
		dbOpen, err = sqlx.Open(internalDriverName, dataSourceName)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}
		if d.DB != nil && d.DB.SQL() != nil {
			defer d.DB.SQL().Close()
		}
		switch internalDriverName {
		case "sqlite3":
			d.DB = (*Sqlite3SqlxDB)(dbOpen)
			_, err = d.DB.SQL().Exec("PRAGMA journal_mode = WAL")
			if err != nil {
				panic(fmt.Errorf("failed to set journal mode: %w", err))
			}
			if strings.Contains(strings.ToLower(dataSourceName), ":memory:") {
				d.DB.SQL().SetMaxOpenConns(1)
			}
			_, err = d.DB.SQL().Exec("PRAGMA synchronous = NORMAL")
			if err != nil {
				panic(fmt.Errorf("failed to set synchronous mode: %w", err))
			}
		case "pgx":
			d.DB = (*PostgreSQLSqlxDB)(dbOpen)
		default:
			panic(fmt.Errorf("unsupported internal database driver name: %s", internalDriverName))
		}
	}
	return err
}

var IsRemote = false

type ParametersType struct {
	DataSourceName      string
	DatabaseVendor      string
	DatabaseHost        string
	DatabasePort        int64
	DatabaseName        string
	DatabaseSSLMode     bool
	DatabaseUserName    string
	DatabasePassword    string
	DatabaseSchema      string
	DatabaseAutoMigrate bool
}

type DAO struct {
	Parameters                ParametersType
	CompleteByPendingInterval int64
	DB                        DBI
}

func New(parameters *ParametersType) (*DAO, error) {
	var newDAO DAO
	newDAO.CompleteByPendingInterval = 600 // 10 minutes for it to be started, otherwise it will be enqueued again when recovered.
	if viper.IsSet("COMPLETE_BY_PENDING_INTERVAL_SECS") {
		newDAO.CompleteByPendingInterval = viper.GetInt64("COMPLETE_BY_PENDING_INTERVAL_SECS")
	}
	newDAO.Parameters = *parameters
	switch strings.TrimSpace(newDAO.Parameters.DatabaseVendor) {
	//goland:noinspection SpellCheckingInspection
	case "postgresql":
		sslMode := "disable"
		if newDAO.Parameters.DatabaseSSLMode {
			sslMode = "enable"
		}

		var connectionString string
		if newDAO.Parameters.DatabaseSchema != "" {
			connectionString = "host=%[1]s port=%[2]d user=%[3]s password=%[4]s dbname=%[5]s sslmode=%[6]s search_path=%[7]s"
		} else {
			connectionString = "host=%[1]s port=%[2]d user=%[3]s password=%[4]s dbname=%[5]s sslmode=%[6]s"
		}
		newDAO.Parameters.DataSourceName = fmt.Sprintf(connectionString,
			newDAO.Parameters.DatabaseHost,
			newDAO.Parameters.DatabasePort,
			newDAO.Parameters.DatabaseUserName,
			newDAO.Parameters.DatabasePassword,
			newDAO.Parameters.DatabaseName,
			sslMode,
			newDAO.Parameters.DatabaseSchema)
		fallthrough
	case "sqlite":
		IsRemote = false
		err := newDAO.openDatabase(newDAO.Parameters.DatabaseVendor, newDAO.Parameters.DataSourceName)
		if err != nil {
			return nil, err
		}
	case "remote":
		IsRemote = true
	default:
		return nil, fmt.Errorf("database vendor: %s is not supported", newDAO.Parameters.DatabaseVendor)
	}
	return &newDAO, nil
}

func (d *DAO) Transactional(transactionalFunction func(tx *sqlx.Tx) error) (err error) {
	var tx *sqlx.Tx
	tx, err = d.DB.SQL().Beginx()
	if err != nil {
		panic(err)
	}
	defer func() {
		if p := recover(); p != nil {
			err2 := tx.Rollback()
			if err2 != nil {
				defer log.WithField("stack", string(debug.Stack())).Error(fmt.Errorf("failed to rollback: %w", err2))
			}
			panic(p)
		} else if err != nil {
			err2 := tx.Rollback()
			if err2 != nil {
				panic(err2)
			}
		} else {
			err2 := tx.Commit()
			if err2 != nil {
				panic(err2)
			}
		}
	}()
	err = transactionalFunction(tx)
	return err
}
func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
}

func VetIds(ids []string) error {
	if ids != nil {
		for _, id := range ids {
			_, err := uuid.Parse(id)
			if err != nil {
				return api.NewWrapError(api.ErrInvalidParams, err, "failed to parse UUID: %s, %w", id, err)
			}
		}
	}
	return nil
}

type indicesUUIDs struct {
	runId string
	index int64
	uuid  string
}

func (d *DAO) updateManyStepsPartsTxInternal(tx *sqlx.Tx, indicesUuids []indicesUUIDs, newStatus api.StepStatusType, newStatusOwner string, prevStatus []api.StepStatusType, completeBy *int64, retriesLeft *int, context api.Context, state *string) []UUIDAndStatusOwner {
	var result []UUIDAndStatusOwner
	for _, indexOrUUID := range indicesUuids {
		var err error
		var toSet []string
		var where []string
		var res sql.Result
		var params []interface{}
		statusOwner := newStatusOwner
		if statusOwner == "" {
			var uuid4 uuid.UUID
			uuid4, err = uuid.NewRandom()
			if err != nil {
				panic(fmt.Errorf("failed to generate uuid: %w", err))
			}
			statusOwner = uuid4.String()
		}
		toSet = append(toSet, "status")
		params = append(params, newStatus)
		toSet = append(toSet, "status_owner")
		params = append(params, statusOwner)

		if state != nil {
			toSet = append(toSet, "state")
			params = append(params, *state)
		}
		if context != nil {
			toSet = append(toSet, "context")
			params = append(params, context)
		}
		if retriesLeft != nil {
			toSet = append(toSet, "retries_left")
			params = append(params, *retriesLeft)
		}

		if indexOrUUID.runId != "" {
			where = append(where, "run_id")
			params = append(params, indexOrUUID.runId)
		}
		if indexOrUUID.index > 0 {
			where = append(where, "\"index\"")
			params = append(params, indexOrUUID.index)
		}
		if indexOrUUID.uuid != "" {
			where = append(where, "uuid")
			params = append(params, indexOrUUID.uuid)
		}

		if len(prevStatus) == 1 {
			where = append(where, "status")
			params = append(params, prevStatus[0])
		}
		var buf bytes.Buffer
		buf.WriteString("update steps set heartbeat=CURRENT_TIMESTAMP")
		i := 0
		for _, s := range toSet {
			i++
			buf.WriteString(",")
			buf.WriteString(s)
			buf.WriteString(fmt.Sprintf("=$%d", i))
		}
		if completeBy != nil {
			buf.WriteString(d.DB.completeByUpdateStatement(completeBy))
		} else {
			buf.WriteString(",complete_by=null")
		}
		buf.WriteString(" where")
		for j, s := range where {
			if j > 0 {
				buf.WriteString(" AND")
			}
			i++
			buf.WriteString(" ")
			buf.WriteString(s)
			buf.WriteString(fmt.Sprintf("=$%d", i))
		}
		res, err = tx.Exec(buf.String(), params...)
		if err != nil {
			panic(err)
		}
		var affected int64
		affected, err = res.RowsAffected()
		if err != nil {
			panic(err)
		}
		if affected == 1 {
			var partialStep *api.StepRecord
			if indexOrUUID.uuid == "" {
				partialStep, err = GetStepTx(tx, indexOrUUID.runId, indexOrUUID.index, []string{UUID})
				if err != nil {
					panic(err)
				}
				result = append(result, UUIDAndStatusOwner{
					UUID:        partialStep.UUID,
					StatusOwner: statusOwner,
				})
			} else {
				result = append(result, UUIDAndStatusOwner{
					UUID:        indexOrUUID.uuid,
					StatusOwner: statusOwner,
				})
			}
		}
	}
	return result
}
