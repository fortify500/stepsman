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
package cmd

import (
	"context"
	"fmt"
	"github.com/chi-middleware/logrus-logger"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/valve"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var ServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "serve will enter server mode and serve http requests",
	Long:  `Use serve to remote control stepsman via http. You can query, monitor and do remotely.`,
	Run: func(cmd *cobra.Command, args []string) {
		serve()
	},
}

func init() {
	ServeCmd.Flags().Int64VarP(&Parameters.ServerPort, "serve-port", "p", 3333, "server port number")
	RootCmd.AddCommand(ServeCmd)
}

func serve() {

	shutdownValve := valve.New()
	baseCtx := shutdownValve.Context()
	newLog := log.New()
	newLog.SetFormatter(&log.JSONFormatter{})
	newLog.SetLevel(log.TraceLevel)
	mw := io.MultiWriter(os.Stdout, Luberjack)
	newLog.SetOutput(mw)
	log.SetOutput(mw)
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

	serverAddress := fmt.Sprintf(":%v", Parameters.ServerPort)
	log.Info(fmt.Sprintf("using server address: %s", serverAddress))
	srv := http.Server{Addr: serverAddress, Handler: chi.ServerBaseContext(baseCtx, r)}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
	go func() {
		for range interrupt {
			log.Info("shutting down..")

			// first shutdownValve
			shutdownValve.Shutdown(20 * time.Second)

			// create context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			// start http shutdown
			srv.Shutdown(ctx)

			// verify, in worst case call cancel via defer
			select {
			case <-time.After(21 * time.Second):
				log.Error("not all connections done")
			case <-ctx.Done():

			}
		}
	}()
	srv.ListenAndServe()
}
