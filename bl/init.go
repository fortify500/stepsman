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
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

type JOBQueueType int

const (
	JobQueueTypeMemoryQueue JOBQueueType = 0
	JobQueueTypeRabbitMQ    JOBQueueType = 10
)

func TranslateToJOBQueueType(status string) (JOBQueueType, error) {
	switch status {
	case "rabbitmq":
		return JobQueueTypeRabbitMQ, nil
	case "memory":
		return JobQueueTypeMemoryQueue, nil
	default:
		return JobQueueTypeMemoryQueue, api.NewError(api.ErrInvalidParams, "failed to translate to job queue type: %s", status)
	}
}

var TESTSLock sync.Mutex

type BL struct {
	TLSEnable                               bool
	TLSEnableClientCertificateCheck         bool
	TLSCertFile                             string
	TLSKeyFile                              string
	TLSCAFile                               string
	InterruptServe                          chan os.Signal
	ServerReady                             bool
	stopping                                workCounter
	netTransport                            *http.Transport
	ValveCtx                                context.Context
	ShutdownValve                           *valve.Valve
	CancelValveCtx                          context.CancelFunc
	memoryQueue                             chan *doWork
	queue                                   chan *workerDoItem
	stopMemoryJobQueue                      chan struct{}
	stopRabbitMQJobQueue                    chan struct{}
	stopRecovery                            chan struct{}
	stopRecoveryNotifications1              chan struct{}
	stopRecoveryNotifications2              chan struct{}
	workCounter                             workCounter
	completeByInProgressInterval            int64
	jobQueueNumberOfWorkers                 int
	jobQueueMemoryQueueLimit                int
	JobQueueType                            JOBQueueType
	rabbitMQJobQueueName                    string
	rabbitMQURIConnectionString             string
	rabbitMQReconnectDelay                  time.Duration
	rabbitMQReInitDelay                     time.Duration
	rabbitMQResendDelay                     time.Duration
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
	TESTSLock.Lock()
	defer TESTSLock.Unlock()
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
	newBL.TLSEnable = false
	newBL.TLSEnableClientCertificateCheck = false
	newBL.rabbitMQJobQueueName = "stepsman_tasks"
	newBL.rabbitMQURIConnectionString = "amqp://guest:guest@localhost:5672/"
	newBL.rabbitMQReconnectDelay = 5 * time.Second
	newBL.rabbitMQReInitDelay = 2 * time.Second
	newBL.rabbitMQResendDelay = 5 * time.Second
	newBL.JobQueueType = JobQueueTypeMemoryQueue
	newBL.InterruptServe = make(chan os.Signal, 1)
	signal.Notify(newBL.InterruptServe, os.Interrupt, syscall.SIGTERM)
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

	if viper.IsSet("TLS_ENABLE") {
		newBL.TLSEnable = viper.GetBool("TLS_ENABLE")
	}
	if viper.IsSet("TLS_ENABLE_CLIENT_CERTIFICATE_CHECK") {
		newBL.TLSEnableClientCertificateCheck = viper.GetBool("TLS_ENABLE_CLIENT_CERTIFICATE_CHECK")
	}
	if viper.IsSet("TLS_CA_FILE") {
		newBL.TLSCAFile = viper.GetString("TLS_CA_FILE")
	}
	if viper.IsSet("TLS_CERT_FILE") {
		newBL.TLSCertFile = viper.GetString("TLS_CERT_FILE")
	}
	if viper.IsSet("TLS_KEY_FILE") {
		newBL.TLSKeyFile = viper.GetString("TLS_KEY_FILE")
	}
	if viper.IsSet("JOB_QUEUE_NUMBER_OF_WORKERS") {
		newBL.jobQueueNumberOfWorkers = viper.GetInt("JOB_QUEUE_NUMBER_OF_WORKERS")
	}
	if viper.IsSet("JOB_QUEUE_MEMORY_QUEUE_LIMIT") {
		newBL.jobQueueMemoryQueueLimit = viper.GetInt("JOB_QUEUE_MEMORY_QUEUE_LIMIT")
	}
	if viper.IsSet("JOB_QUEUE_TYPE") {
		newBL.JobQueueType, err = TranslateToJOBQueueType(viper.GetString("JOB_QUEUE_TYPE"))
		if err != nil {
			log.Fatal(api.NewLocalizedError("failed to get job queue type: %w", err))
		}
	}
	if viper.IsSet("RABBITMQ_JOB_QUEUE_NAME") {
		newBL.rabbitMQJobQueueName = viper.GetString("RABBITMQ_JOB_QUEUE_NAME")
	}
	if viper.IsSet("RABBITMQ_URI_CONNECTION_STRING") {
		newBL.rabbitMQURIConnectionString = viper.GetString("RABBITMQ_URI_CONNECTION_STRING")
	}
	if viper.IsSet("RABBITMQ_RECONNECT_DELAY") {
		newBL.rabbitMQReconnectDelay = time.Duration(viper.GetInt("RABBITMQ_RECONNECT_DELAY")) * time.Second
	}
	if viper.IsSet("RABBITMQ_RE_INIT_DELAY") {
		newBL.rabbitMQReInitDelay = time.Duration(viper.GetInt("RABBITMQ_RE_INIT_DELAY")) * time.Second
	}
	if viper.IsSet("RABBITMQ_RE_SEND_DELAY") {
		newBL.rabbitMQResendDelay = time.Duration(viper.GetInt("RABBITMQ_RE_SEND_DELAY")) * time.Second
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
		newBL.Client = client.New(newBL.DAO.Parameters.DatabaseSSLMode, newBL.DAO.Parameters.DatabaseHost, newBL.DAO.Parameters.DatabasePort, nil)
	}

	return &newBL, nil
}

func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
}
