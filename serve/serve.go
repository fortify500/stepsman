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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/InVisionApp/go-health/v2"
	"github.com/chi-middleware/logrus-logger"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/bl"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"
)

type dbHealthCheck struct {
	BL *bl.BL
}

func (c *dbHealthCheck) Status() (interface{}, error) {
	err := c.BL.DAO.DB.SQL().Ping()
	if err != nil {
		err = fmt.Errorf("health - failed to ping database: %w", err)
		log.Error(err)
		return nil, err
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

func createTLSServerConfig(BL *bl.BL) (*tls.Config, error) {
	if BL.TLSEnableClientCertificateCheck {
		caCertPEM, err := ioutil.ReadFile(BL.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA file:%w", err)
		}

		roots := x509.NewCertPool()
		ok := roots.AppendCertsFromPEM(caCertPEM)
		if !ok {
			return nil, fmt.Errorf("failed to load CA:%w", err)
		}

		cert, err := tls.LoadX509KeyPair(BL.TLSCertFile, BL.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load X509 key pair:%w", err)
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    roots,
		}, nil
	} else {
		cert, err := tls.LoadX509KeyPair(BL.TLSCertFile, BL.TLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load X509 key pair: %w", err)
		}
		return &tls.Config{
			Certificates: []tls.Certificate{cert},
		}, nil
	}
}

func Serve(BL *bl.BL, address string, port int64, healthPort int64) {
	newLog := log.New()
	newLog.SetFormatter(&log.JSONFormatter{})
	newLog.SetLevel(log.GetLevel())
	newLog.SetOutput(output)
	rHealth := chi.NewRouter()
	rHealth.Use(middleware.AllowContentType("application/json"))
	rHealth.Use(middleware.RequestID)
	rHealth.Use(middleware.RealIP)
	rHealth.Use(logger.Logger("router", newLog))
	rHealth.Use(middleware.NoCache)
	rHealth.Use(middleware.Recoverer)
	rHealth.Use(middleware.Timeout(60 * time.Second))
	h := health.New()
	h.DisableLogging()
	if err := h.AddChecks([]*health.Config{
		{
			Name: "db-health-check",
			Checker: &dbHealthCheck{
				BL: BL,
			},
			Interval: time.Duration(15) * time.Second,
			Fatal:    true,
		},
	}); err != nil {
		panic(err)
	}
	if err := h.Start(); err != nil {
		panic(err)
	}
	rHealth.Get("/health", CustomJSONHandlerFunc(h, nil))
	healthServerAddress := fmt.Sprintf(":%v", healthPort)
	log.Info(fmt.Sprintf("using server address: %s", healthServerAddress))
	srvHealth := http.Server{Addr: healthServerAddress, Handler: chi.ServerBaseContext(context.Background(), rHealth)}
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

	serverAddress := fmt.Sprintf("%s:%v", address, port)
	log.Info(fmt.Sprintf("using server address: %s", serverAddress))
	baseContext := context.WithValue(BL.ValveCtx, "BL", BL)
	srv := http.Server{Addr: serverAddress, Handler: chi.ServerBaseContext(baseContext, r)}
	exit := make(chan int, 1)
	go func() {
		viper.SetDefault("SHUTDOWN_INTERVAL", time.Duration(20)*time.Minute)
		<-BL.InterruptServe
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
			defer BL.CancelValveCtx()
			//  shutdownValve is for long running jobs if any.
			if err := BL.ShutdownValve.Shutdown(shutdownInterval * time.Second); err != nil {
				log.Error(fmt.Errorf("failed to start shutting down valve: %w", err))
			}
		}()
		wg.Wait()
		exit <- 0
	}()

	go func() {
		errHealth := srvHealth.ListenAndServe()
		if errHealth == http.ErrServerClosed {
			errHealth = nil
		}
		if errHealth != nil {
			log.Fatal(api.NewLocalizedError("failed to start serving health checks: %w", errHealth))
		}
	}()
	var err error
	var l net.Listener
	bl.TESTSLock.Lock()
	tlsEnable := BL.TLSEnable
	bl.TESTSLock.Unlock()
	if tlsEnable {
		var config *tls.Config
		config, err = createTLSServerConfig(BL)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to configure for listening - tls: %w", err))
		}
		l, err = tls.Listen("tcp", srv.Addr, config)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to start listening - tls: %w", err))
		}
	} else {
		l, err = net.Listen("tcp", srv.Addr)
		if err != nil {
			log.Fatal(fmt.Errorf("failed to start listening: %w", err))
		}
	}
	BL.ServerReady = true
	err = srv.Serve(l)
	if err != nil {
		log.Error(fmt.Errorf("failed to start serving: %w", err))
		go func() {
			BL.InterruptServe <- syscall.SIGINT
		}()
	}
	{
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
		go cancel()
		err = srvHealth.Shutdown(ctx)
		if err != nil {
			log.Error(fmt.Errorf("failed to shutdown health check service: %w", err))
		}
	}
	<-exit
}
