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
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

const RecoveryChannelName = "recovery"

type RecoveryMessage struct {
	InstanceUUID string `json:"instance-uuid"`
	ReachedLimit bool
}

func (b *BL) recoveryScheduler() {
	var shortInterval = true
	for {
		var interval time.Duration
		if shortInterval {
			shortInterval = false
			interval = time.Duration(b.recoveryShortIntervalMinimumSeconds)*time.Second +
				time.Duration(rand.Intn(b.recoveryShortIntervalRandomizedSeconds))*time.Second
		} else {
			interval = time.Duration(b.recoveryLongIntervalMinimumSeconds)*time.Second +
				time.Duration(rand.Intn(b.recoveryLongIntervalRandomizedSeconds))*time.Second
		}
		select {
		case <-b.stop:
			log.Info("leaving postgresql listener, server is shutting down")
			return
		case <-b.ValveCtx.Done():
			log.Info("leaving postgresql listener, server is shutting down")
			return
		case msg := <-b.recoveryReschedule:
			log.Debug("rescheduling postgresql listener")
			if msg.ReachedLimit {
				shortInterval = true
			} else {
				shortInterval = false
			}
		case <-time.After(interval):
			var stepsUUIDs []string
			if b.IsPostgreSQL() && len(b.memoryQueue) >= b.recoveryAllowUnderJobQueueNumberOfItems {
				shortInterval = true
				continue
			}
			tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
				if b.IsPostgreSQL() {
					msg, err := json.Marshal(RecoveryMessage{
						InstanceUUID: api.InstanceId,
					})
					if err != nil {
						panic(err)
					}
					b.DAO.DB.Notify(tx, RecoveryChannelName, string(msg))
				}
				return nil
			})
			if tErr != nil {
				log.Errorf("failed to recover steps: %w", tErr)
			}
			tErr = b.DAO.Transactional(func(tx *sqlx.Tx) error {
				stepsUUIDs = b.DAO.DB.RecoverSteps(b.DAO, tx, b.recoveryMaxRecoverItemsPassLimit)
				return nil
			})
			if tErr != nil {
				log.Errorf("failed to recover steps: %w", tErr)
			}
			if tErr != nil || (b.IsPostgreSQL() && len(stepsUUIDs) >= b.recoveryMaxRecoverItemsPassLimit) {
				tErr = b.DAO.Transactional(func(tx *sqlx.Tx) error {
					if b.IsPostgreSQL() {
						shortInterval = true
						msg, err := json.Marshal(RecoveryMessage{
							InstanceUUID: api.InstanceId,
							ReachedLimit: true,
						})
						if err != nil {
							panic(err)
						}
						b.DAO.DB.Notify(tx, RecoveryChannelName, string(msg))
					}
					return nil
				})
				if tErr != nil {
					log.Errorf("failed to recover steps: %w", tErr)
				}
			}
			for _, item := range stepsUUIDs {
				work := doWork(item)
				if err := b.Enqueue(&work); err != nil {
					log.Errorf("in recover steps, failed to enqueue item:%s, with err %w", item, tErr)
				}
			}
		}
		log.Debug("rescheduling postgresql listener, restarting loop")
	}
}
func (b *BL) startRecoveryListening() {
	reportErrFunc := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			log.Errorf("failed to start listener: %w", err)
			select {
			case <-b.ValveCtx.Done():
				panic(api.NewError(api.ErrShuttingDown, "leaving postgresql listener, server is shutting down"))
			default:
			}
		}
	}
	processAllNotifications := func(l *pq.Listener) error {
		var msg *pq.Notification
		for {
			msg = nil
			select {
			case <-b.stop:
				return api.NewError(api.ErrShuttingDown, "leaving postgresql listener, server is shutting down")
			case <-b.ValveCtx.Done():
				return api.NewError(api.ErrShuttingDown, "leaving postgresql listener, server is shutting down")
			case msg = <-l.Notify:
			}
			if msg == nil {
				panic(fmt.Errorf("postgresql listener msg cannot be nil"))
			}
			var recoveryMessage RecoveryMessage
			if log.IsLevelEnabled(log.TraceLevel) {
				log.Trace(fmt.Sprintf("postgresql listener got: %#v", msg))
			}
			err := json.Unmarshal([]byte(msg.Extra), &recoveryMessage)
			if err != nil {
				panic(fmt.Errorf("postgresql listener failed to parse '%s': %w", msg.Extra, err))
			}
			if log.IsLevelEnabled(log.TraceLevel) {
				log.Trace("received event: %s", msg.Extra)
			}
			log.Debugf("received skip recovery message: %v", recoveryMessage)
			if recoveryMessage.InstanceUUID != api.InstanceId {
				b.recoveryReschedule <- recoveryMessage
			}
		}
	}
	waitIt := func() {
		recoverable(func() error {
			log.Info("starting to listen for postgresql notifications...")
			listener := pq.NewListener(b.DAO.Parameters.DataSourceName, time.Duration(2)*time.Second, time.Duration(64)*time.Second, reportErrFunc)
			defer func() {
				err := listener.Close()
				log.Errorf("failed to close listener: %w", err)
			}()
			err := listener.Listen(RecoveryChannelName)
			if err != nil {
				panic(fmt.Errorf("postgresql listener failed to LISTEN to channel: %w", err))
			}
			err = processAllNotifications(listener)
			return err
		})
	}
	for {
		select {
		case <-b.stop:
			log.Info("leaving postgresql listener, shutting down")
			return
		case <-b.ValveCtx.Done():
			log.Info("leaving postgresql listener, shutting down")
			return
		default:
		}
		waitIt()
	}
}
