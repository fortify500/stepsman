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
package bl

import (
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path"
)

var DB *sqlx.DB
var StoreDir string
var cfgFile string
var Luberjack *lumberjack.Logger

func InitBL(cfgFile string) error {
	flag.Parse()
	dir, err := homedir.Dir()
	if err != nil {
		return fmt.Errorf("failed to detect home directory: %w", err)
	}
	StoreDir = path.Join(dir, ".stepsman")
	_, err = os.Stat(StoreDir)

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(StoreDir)
		viper.SetConfigName(".stepsman")
	}

	viper.AutomaticEnv() // read in environment variables that match
	Luberjack = &lumberjack.Logger{
		Filename:   path.Join(StoreDir, "stepsman.log"),
		MaxSize:    100, // megabytes
		MaxBackups: 2,
		MaxAge:     1, // days
		Compress:   true,
	}
	// use this later on
	log.SetOutput(Luberjack)

	//mw := io.MultiWriter(os.Stdout, &lumberjack.Logger{
	//	Filename:   path.Join(StoreDir, "stepsman.log"),
	//	MaxSize:    10, // megabytes
	//	MaxBackups: 2,
	//	MaxAge:     1, // days
	//	Compress:   true,
	//})
	//log.SetOutput(mw)
	log.SetLevel(log.TraceLevel)

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	}

	if os.IsNotExist(err) {
		err = os.MkdirAll(StoreDir, 0700)
		if err != nil {
			return fmt.Errorf("failed to create the .stepsman diretory: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to determine existance of .stepsman directory: %w", err)
	}

	DB, err = sqlx.Open("sqlite3", path.Join(StoreDir, "stepsman.DB"))
	_, err = DB.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		return fmt.Errorf("failed to set journal mode: %w", err)
	}
	_, err = DB.Exec("PRAGMA synchronous = NORMAL")
	if err != nil {
		return fmt.Errorf("failed to set synchronous mode: %w", err)
	}
	err = DB.Ping()
	if err != nil {
		return fmt.Errorf("failed to open a database connection: %w", err)
	}
	err = migrateDB()
	if err != nil {
		return err
	}
	return nil
}

func migrateDB() error {
	var version = -1
	_, err := DB.Exec(`CREATE TABLE IF NOT EXISTS migration (
    id INTEGER PRIMARY KEY NOT NULL,
	version INTEGER NOT NULL
    );`)
	if err != nil {
		return fmt.Errorf("failed to verify database migration table creation: %w", err)
	}
	tx, err := DB.Beginx()
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
		_, err := tx.Exec(`CREATE TABLE runs (
                                     id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                                     uuid TEXT NOT NULL,
	                                 title TEXT,
	                                 cursor INTEGER,
	                                 status INTEGER NOT NULL,
	                                 script TEXT
                                     )`)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to create database runs table: %w", err)
		}
		_, err = tx.Exec(`CREATE TABLE steps (
                                     run_id INTEGER NOT NULL,
                                     step_id INTEGER NOT NULL,
                                     uuid TEXT NOT NULL,
	                                 name TEXT,
	                                 status INTEGER NOT NULL,
	                                 heartbeat INTEGER NOT NULL,
	                                 script TEXT,
	                                 PRIMARY KEY (run_id, step_id)
                                     )`)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to create database steps table: %w", err)
		}
		_, err = tx.Exec(`CREATE UNIQUE INDEX idx_runs_uuid ON runs (uuid)`)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to create index idx_runs_title_status: %w", err)
		}
		_, err = tx.Exec(`CREATE INDEX idx_runs_title_status ON runs (title, status)`)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to create index idx_runs_title_status: %w", err)
		}

		_, err = tx.Exec(`CREATE INDEX idx_runs_status ON runs (status)`)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to create index idx_runs_status: %w", err)
		}
		//CREATE INDEX idx_contacts_title
		//ON contacts (first_name, last_name);
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
