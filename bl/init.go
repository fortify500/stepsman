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
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/dao"
	"github.com/go-chi/valve"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	_ "github.com/jackc/pgx/stdlib"
	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"runtime/debug"
)

type BL struct {
	netTransport                            *http.Transport
	ValveCtx                                context.Context
	ShutdownValve                           *valve.Valve
	CancelValveCtx                          context.CancelFunc
	memoryQueue                             chan *doWork
	queue                                   chan *doWork
	stop                                    <-chan struct{}
	workCounter                             workCounter
	completeByInProgressInterval            int64
	jobQueueNumberOfWorkers                 int
	jobQueueMemoryQueueLimit                int
	PendingRecursionDepthLimit              int
	recoveryDisableSkipLocks                bool
	recoveryMaxRecoverItemsPassLimit        int
	recoveryAllowUnderJobQueueNumberOfItems int
	recoveryShortIntervalMinimumSeconds     int
	recoveryShortIntervalRandomizedSeconds  int
	recoveryLongIntervalMinimumSeconds      int
	recoveryLongIntervalRandomizedSeconds   int
	recoveryReschedule                      chan RecoveryMessage
	templateCacheSize                       int
	maxRegoEvaluationTimeoutSeconds         int
	InstanceId                              string
	templateCache                           *lru.Cache
	DAO                                     *dao.DAO
	Client                                  *client.CLI
}

func (b *BL) IsPostgreSQL() bool {
	switch b.DAO.DB.(type) {
	case *dao.PostgreSQLSqlxDB:
		return true
	}
	return false
}
func (b *BL) IsSqlite() bool {
	switch b.DAO.DB.(type) {
	case *dao.Sqlite3SqlxDB:
		return true
	}
	return false
}
func New(daoParameters *dao.ParametersType) (*BL, error) {
	var newBL BL
	uuid4, err := uuid.NewRandom()
	if err != nil {
		panic(err)
	}
	newBL.InstanceId = uuid4.String()
	log.Info(fmt.Sprintf("stepsman instance id: %s", newBL.InstanceId))
	log.Info(fmt.Sprintf("stepsman starting [build commit id: %s]", dao.GitCommit))
	bi, ok := debug.ReadBuildInfo()
	if ok {
		log.WithField("build-info", bi).Info()
	}
	newBL.maxRegoEvaluationTimeoutSeconds = 3
	newBL.completeByInProgressInterval = 3600
	newBL.jobQueueNumberOfWorkers = 5000
	newBL.jobQueueMemoryQueueLimit = 1 * 1000 * 1000
	newBL.PendingRecursionDepthLimit = 100
	newBL.templateCacheSize = 1000
	newBL.recoveryLongIntervalMinimumSeconds = 10 * 60
	newBL.recoveryLongIntervalRandomizedSeconds = 10 * 60
	newBL.recoveryMaxRecoverItemsPassLimit = newBL.jobQueueMemoryQueueLimit / 2
	newBL.recoveryAllowUnderJobQueueNumberOfItems = newBL.jobQueueMemoryQueueLimit / 2
	newBL.recoveryReschedule = make(chan RecoveryMessage)
	if viper.IsSet("JOB_QUEUE_NUMBER_OF_WORKERS") {
		newBL.jobQueueNumberOfWorkers = viper.GetInt("JOB_QUEUE_NUMBER_OF_WORKERS")
	}
	if viper.IsSet("JOB_QUEUE_MEMORY_QUEUE_LIMIT") {
		newBL.jobQueueMemoryQueueLimit = viper.GetInt("JOB_QUEUE_MEMORY_QUEUE_LIMIT")
	}
	if viper.IsSet("PENDING_RECURSION_DEPTH_LIMIT") {
		newBL.PendingRecursionDepthLimit = viper.GetInt("PENDING_RECURSION_DEPTH_LIMIT")
	}
	if viper.IsSet("COMPLETE_BY_IN_PROGRESS_INTERVAL_SECS") {
		newBL.completeByInProgressInterval = viper.GetInt64("COMPLETE_BY_IN_PROGRESS_INTERVAL_SECS")
	}
	if viper.IsSet("RECOVERY_MAX_RECOVER_ITEMS_PASS_LIMIT") {
		newBL.recoveryMaxRecoverItemsPassLimit = viper.GetInt("RECOVERY_MAX_RECOVER_ITEMS_PASS_LIMIT")
	}
	if viper.IsSet("RECOVERY_ALLOW_UNDER_JOB_QUEUE_NUMBER_OF_ITEMS") {
		newBL.recoveryAllowUnderJobQueueNumberOfItems = viper.GetInt("RECOVERY_ALLOW_UNDER_JOB_QUEUE_NUMBER_OF_ITEMS")
	}

	if viper.IsSet("RECOVERY_DISABLE_SKIP_LOCKS") {
		newBL.recoveryDisableSkipLocks = viper.GetBool("RECOVERY_DISABLE_SKIP_LOCKS")
	}

	if viper.IsSet("TEMPLATE_CACHE_SIZE") {
		newBL.templateCacheSize = viper.GetInt("TEMPLATE_CACHE_SIZE")
	}

	cache, err := lru.New(newBL.templateCacheSize)
	if err != nil {
		log.Fatal(api.NewLocalizedError("failed to initialize template cache: %w", err))
	}
	newBL.templateCache = cache
	newBL.DAO, err = dao.New(daoParameters)
	if err != nil {
		return nil, err
	}

	if newBL.IsSqlite() {
		newBL.recoveryShortIntervalMinimumSeconds = 5
		newBL.recoveryShortIntervalRandomizedSeconds = 2
	} else {
		newBL.recoveryShortIntervalMinimumSeconds = 60
		newBL.recoveryShortIntervalRandomizedSeconds = 2 * 60
	}

	if viper.IsSet("RECOVERY_SHORT_INTERVAL_MINIMUM_SECONDS") {
		newBL.recoveryShortIntervalMinimumSeconds = viper.GetInt("RECOVERY_SHORT_INTERVAL_MINIMUM_SECONDS")
	}

	if viper.IsSet("RECOVERY_SHORT_INTERVAL_RANDOMIZED_SECONDS") {
		newBL.recoveryShortIntervalRandomizedSeconds = viper.GetInt("RECOVERY_SHORT_INTERVAL_RANDOMIZED_SECONDS")
	}

	if viper.IsSet("RECOVERY_LONG_INTERVAL_MINIMUM_SECONDS") {
		newBL.recoveryLongIntervalMinimumSeconds = viper.GetInt("RECOVERY_LONG_INTERVAL_MINIMUM_SECONDS")
	}

	if viper.IsSet("RECOVERY_LONG_INTERVAL_RANDOMIZED_SECONDS") {
		newBL.recoveryLongIntervalRandomizedSeconds = viper.GetInt("RECOVERY_LONG_INTERVAL_RANDOMIZED_SECONDS")
	}
	if viper.IsSet("MAX_REGO_EVALUATION_TIMEOUT_SECONDS") {
		newBL.maxRegoEvaluationTimeoutSeconds = viper.GetInt("MAX_REGO_EVALUATION_TIMEOUT_SECONDS")
	}

	maxResponseHeaderByte := int64(0)
	if viper.IsSet("MAX_RESPONSE_HEADER_BYTES") {
		maxResponseHeaderByte = viper.GetInt64("MAX_RESPONSE_HEADER_BYTES")
	}
	newBL.initDO(maxResponseHeaderByte)

	if !dao.IsRemote {
		err = newBL.MigrateDB(daoParameters.DatabaseAutoMigrate)
		if err != nil {
			return nil, fmt.Errorf("failed to init: %w", err)
		}
		newBL.initQueue()
	} else {
		newBL.Client = client.New(newBL.DAO.Parameters.DatabaseSSLMode, newBL.DAO.Parameters.DatabaseHost, newBL.DAO.Parameters.DatabasePort)
	}

	return &newBL, nil
}

func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
}
