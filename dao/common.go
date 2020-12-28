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
	"github.com/fortify500/stepsman/api"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"time"
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
	Tags            = "tags"
)

type UUIDAndGroupId struct {
	UUID    uuid.UUID `db:"uuid" json:"uuid,omitempty"`
	GroupId uuid.UUID `db:"group_id" json:"group-id,omitempty"`
}
type IdAndGroupId struct {
	Id      uuid.UUID `db:"id" json:"id,omitempty"`
	GroupId uuid.UUID `db:"group_id" json:"group-id,omitempty"`
}
type DBI interface {
	SQL() *sqlx.DB
	VerifyDBCreation(tx *sqlx.Tx) error
	CreateStepTx(tx *sqlx.Tx, stepRecord *api.StepRecord)
	CreateRunTx(tx *sqlx.Tx, runRecord interface{}, completeBy int64)
	Migrate0(tx *sqlx.Tx) error
	completeByUpdateStatement(completeBy *int64) string
	RecoverSteps(DAO *DAO, tx *sqlx.Tx, limit int, disableSkipLocks bool) []UUIDAndGroupId
	GetAndUpdateExpiredRuns(DAO *DAO, tx *sqlx.Tx, limit int, disableSkipLocks bool) []IdAndGroupId
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
			if strings.Contains(strings.ToLower(dataSourceName), ":memory:") {
				d.DB.SQL().SetMaxOpenConns(1)
			} else {
				d.setConnectionConfiguration(dbOpen)
			}
			_, err = d.DB.SQL().Exec("PRAGMA journal_mode = WAL")
			if err != nil {
				panic(fmt.Errorf("failed to set journal mode: %w", err))
			}
			_, err = d.DB.SQL().Exec("PRAGMA synchronous = NORMAL")
			if err != nil {
				panic(fmt.Errorf("failed to set synchronous mode: %w", err))
			}
		case "pgx":
			d.setConnectionConfiguration(dbOpen)
			d.DB = (*PostgreSQLSqlxDB)(dbOpen)
		default:
			panic(fmt.Errorf("unsupported internal database driver name: %s", internalDriverName))
		}
	}
	return err
}

func (d *DAO) setConnectionConfiguration(dbOpen *sqlx.DB) {
	if d.MaxOpenConnections > 0 {
		dbOpen.SetMaxOpenConns(d.MaxOpenConnections)
	}
	if d.MaxIdleConnections > 0 {
		dbOpen.SetMaxIdleConns(d.MaxIdleConnections)
	}
	if d.ConnectionMaxIdleTimeSeconds > 0 {
		dbOpen.SetConnMaxIdleTime(time.Duration(d.ConnectionMaxIdleTimeSeconds) * time.Second)
	}
	if d.ConnectionMaxLifeTimeSeconds > 0 {
		dbOpen.SetConnMaxLifetime(time.Duration(d.ConnectionMaxLifeTimeSeconds) * time.Second)
	}
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
	Parameters                   ParametersType
	CompleteByPendingInterval    int64
	MaxOpenConnections           int
	MaxIdleConnections           int
	ConnectionMaxIdleTimeSeconds int64
	ConnectionMaxLifeTimeSeconds int64
	DB                           DBI
}

func New(parameters *ParametersType) (*DAO, error) {
	var newDAO DAO
	newDAO.CompleteByPendingInterval = 600 // 10 minutes for it to be started, otherwise it will be enqueued again when recovered.
	newDAO.MaxOpenConnections = 700
	if viper.IsSet("COMPLETE_BY_PENDING_INTERVAL_SECS") {
		newDAO.CompleteByPendingInterval = viper.GetInt64("COMPLETE_BY_PENDING_INTERVAL_SECS")
	}
	if viper.IsSet("MAX_OPEN_CONNECTIONS") {
		newDAO.MaxOpenConnections = viper.GetInt("MAX_OPEN_CONNECTIONS")
	}
	if viper.IsSet("MAX_IDLE_CONNECTIONS") {
		newDAO.MaxIdleConnections = viper.GetInt("MAX_IDLE_CONNECTIONS")
	}
	if viper.IsSet("CONNECTION_MAX_IDLE_TIME_SECS") {
		newDAO.ConnectionMaxIdleTimeSeconds = viper.GetInt64("CONNECTION_MAX_IDLE_TIME_SECS")
	}
	if viper.IsSet("CONNECTION_MAX_LIFE_TIME_SECS") {
		newDAO.ConnectionMaxLifeTimeSeconds = viper.GetInt64("CONNECTION_MAX_LIFE_TIME_SECS")
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

type indicesUUIDs struct {
	runId uuid.UUID
	label string
	index int64
	uuid  uuid.UUID
}
