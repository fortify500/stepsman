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

package serve

import (
	"context"
	"fmt"
	"github.com/InVisionApp/go-health/v2"
	"github.com/chi-middleware/logrus-logger"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/valve"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var InterruptServe chan os.Signal

type dbHealthCheck struct{}

func (c *dbHealthCheck) Status() (interface{}, error) {
	err := dao.DB.SQL().Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return nil, nil
}

var output io.Writer

func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
	output = out
}
func Serve(port int64) {
	shutdownValve := valve.New()
	baseCtx := shutdownValve.Context()
	newLog := log.New()
	newLog.SetFormatter(&log.JSONFormatter{})
	newLog.SetLevel(log.GetLevel())
	newLog.SetOutput(output)
	r := chi.NewRouter()
	r.Use(middleware.AllowContentType("application/json"))
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(logger.Logger("router", newLog))
	r.Use(middleware.NoCache)
	r.Use(middleware.Compress(5))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	h := health.New()
	if err := h.AddChecks([]*health.Config{
		{
			Name:     "db-health-check",
			Checker:  &dbHealthCheck{},
			Interval: time.Duration(15) * time.Second,
			Fatal:    true,
		},
	}); err != nil {
		panic(err)
	}
	if err := h.Start(); err != nil {
		panic(err)
	}
	r.Get("/health", CustomJSONHandlerFunc(h, nil))
	r.Post("/v0/json-rpc", GetJsonRpcHandler().ServeHTTP)

	serverAddress := fmt.Sprintf(":%v", port)
	log.Info(fmt.Sprintf("using server address: %s", serverAddress))
	srv := http.Server{Addr: serverAddress, Handler: chi.ServerBaseContext(baseCtx, r)}

	InterruptServe = make(chan os.Signal, 1)
	signal.Notify(InterruptServe, os.Interrupt, syscall.SIGTERM)
	exit := make(chan int, 1)
	bl.InitQueue(baseCtx)
	go func() {
		viper.SetDefault("SHUTDOWN_INTERVAL", time.Duration(120))
		<-InterruptServe
		shutdownInterval := viper.GetDuration("SHUTDOWN_INTERVAL")
		log.Info("shutting down..")
		// create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), shutdownInterval*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			// start http shutdown
			if err := srv.Shutdown(ctx); err != nil {
				log.Error(fmt.Errorf("failed to start shutting down service: %w", err))
			}
		}()
		go func() {
			defer wg.Done()
			//  shutdownValve is for long running jobs if any.
			if err := shutdownValve.Shutdown(shutdownInterval * time.Second); err != nil {
				log.Error(fmt.Errorf("failed to start shutting down valve: %w", err))
			}
		}()
		wg.Wait()
		exit <- 0
	}()

	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		err = nil
	}
	if err != nil {
		log.Error(fmt.Errorf("failed to start listening and serving: %w", err))
		go func() {
			InterruptServe <- syscall.SIGINT
		}()
	}
	<-exit
}
