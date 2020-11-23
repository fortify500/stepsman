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
	"context"
	"fmt"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/dao"
	"github.com/go-chi/valve"
	_ "github.com/jackc/pgx/stdlib"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"strings"
	"sync"
)

var (
	ShutdownValve                *valve.Valve
	ValveCtx                     context.Context
	CompleteByInProgressInterval int64 = 3600

	JobQueueNumberOfWorkers  = 5000
	JobQueueMemoryQueueLimit = 1 * 1000 * 1000
)
var Parameters dao.ParametersType
var parametersMu sync.RWMutex

func IsPostgreSQL() bool {
	parametersMu.RLock()
	defer parametersMu.RUnlock()
	switch strings.TrimSpace(Parameters.DatabaseVendor) {
	case "postgresql":
		return true
	}
	return false
}
func IsSqlite() bool {
	parametersMu.RLock()
	defer parametersMu.RUnlock()
	switch strings.TrimSpace(Parameters.DatabaseVendor) {
	case "sqlite":
		return true
	}
	return false
}
func InitBL(daoParameters *dao.ParametersType) error {
	err := dao.InitDAO(daoParameters)
	if err != nil {
		return err
	}
	parametersMu.Lock()
	Parameters = *daoParameters
	parametersMu.Unlock()

	if !dao.IsRemote {
		err = MigrateDB(daoParameters.DatabaseAutoMigrate)
		if err != nil {
			return fmt.Errorf("failed to init: %w", err)
		}
	} else {
		if viper.IsSet("MAX_RESPONSE_HEADER_BYTES") {
			initDO(viper.GetInt64("MAX_RESPONSE_HEADER_BYTES"))
		}
		client.InitClient(dao.Parameters.DatabaseSSLMode, dao.Parameters.DatabaseHost, dao.Parameters.DatabasePort)
	}
	InitQueue()
	return nil
}

func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
}
