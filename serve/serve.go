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
	"github.com/chi-middleware/logrus-logger"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/valve"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var InterruptServe chan os.Signal

func Serve(port int64) {
	shutdownValve := valve.New()
	baseCtx := shutdownValve.Context()
	newLog := logrus.New()
	newLog.SetFormatter(&logrus.JSONFormatter{})
	newLog.SetLevel(logrus.TraceLevel)
	newLog.SetOutput(os.Stdout)
	r := chi.NewRouter()
	r.Use(middleware.AllowContentType("application/json"))
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(logger.Logger("router", newLog))
	r.Use(middleware.NoCache)
	r.Use(middleware.Compress(5))
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Post("/v0/json-rpc", GetJsonRpcHandler().ServeHTTP)

	serverAddress := fmt.Sprintf(":%v", port)
	logrus.Info(fmt.Sprintf("using server address: %s", serverAddress))
	srv := http.Server{Addr: serverAddress, Handler: chi.ServerBaseContext(baseCtx, r)}

	InterruptServe = make(chan os.Signal, 1)
	signal.Notify(InterruptServe, os.Interrupt, syscall.SIGTERM)
	exit := make(chan int, 1)
	go func() {
		viper.SetDefault("SHUTDOWN_INTERVAL", time.Duration(120))
		<-InterruptServe
		shutdownInterval := viper.GetDuration("SHUTDOWN_INTERVAL")
		logrus.Info("shutting down..")
		// create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), shutdownInterval*time.Second)
		defer cancel()

		// start http shutdown
		err := srv.Shutdown(ctx)
		if err != nil {
			logrus.Error(fmt.Errorf("failed to start shutting down service: %w", err))
		}
		//  shutdownValve is for long running jobs if any.
		err = shutdownValve.Shutdown(shutdownInterval * time.Second)
		if err != nil {
			logrus.Error(fmt.Errorf("failed to start shutting down valve: %w", err))
		}
		exit <- 0
	}()
	err := srv.ListenAndServe()
	if err == http.ErrServerClosed {
		err = nil
	}
	if err != nil {
		logrus.Error(fmt.Errorf("failed to start listening and serving: %w", err))
		go func() {
			InterruptServe <- syscall.SIGINT
		}()
	}
	<-exit
}
