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
	"github.com/go-chi/valve"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"runtime/debug"
	"sync/atomic"
	"time"
)

type WorkType bool

const (
	RunWorkType  WorkType = true
	StepWorkType WorkType = false
)

type workerDoItem struct {
	doWork *doWork
	finish func()
}
type doWork struct {
	Item     uuid.UUID
	Options  api.Options
	ItemType WorkType
}
type workCounter int32

func (c *workCounter) inc() int32 {
	return atomic.AddInt32((*int32)(c), 1)
}
func (c *workCounter) dec() int32 {
	return atomic.AddInt32((*int32)(c), -1)
}

func (c *workCounter) get() int32 {
	return atomic.LoadInt32((*int32)(c))
}

func (b *BL) Enqueue(do *doWork) error {
	if b.stopping.get() > 0 {
		return api.NewError(api.ErrShuttingDown, "leaving enqueue, server is shutting down")
	}
	timer := time.NewTimer(10 * time.Second) // timer that will be GC even if not reached
	defer timer.Stop()
	select {
	case <-b.ValveCtx.Done():
		return api.NewError(api.ErrShuttingDown, "leaving enqueue, server is shutting down")
	case b.memoryQueue <- do:
		return nil
	case <-timer.C:
		return api.NewError(api.ErrJobQueueUnavailable, "leaving enqueue, timeout writing to the job queue")
	}
}

// try not to use this, this is primarily for testing
func (b *BL) QueuesIdle() bool {
	if b != nil && len(b.memoryQueue) == 0 && len(b.queue) == 0 && b.workCounter.get() == 0 {
		return true
	} else if b == nil {
		return true
	}
	return false
}

func recoverable(recoverableFunction func() error) {
	var err error
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(error); ok {
				defer log.WithField("stack", string(debug.Stack())).Error(fmt.Errorf("failed to process: %w", p.(error)))
			} else {
				defer log.WithField("stack", string(debug.Stack())).Error(fmt.Errorf("failed to process: %v", p))
			}
		} else if err != nil {
			_ = api.ResolveErrorAndLog(err, true)
		}
	}()
	err = recoverableFunction()
}
func (b *BL) processMsg(msg *doWork) {
	b.workCounter.inc()
	defer b.workCounter.dec()
	if log.IsLevelEnabled(log.TraceLevel) {
		log.Tracef("processing msg: %#v", msg)
	}
	recoverable(func() error {
		var err error
		switch msg.ItemType {
		case StepWorkType:
			_, _, err = b.doStep(nil, &api.DoStepByUUIDParams{
				UUID:    msg.Item,
				Options: msg.Options,
			}, true)
		case RunWorkType:
			err = b.doRun(msg.Options, msg.Item)
		}
		return err
	})
}

func (b *BL) startWorkers() {
	for w := 0; w < b.jobQueueNumberOfWorkers; w++ {
		if err := valve.Lever(b.ValveCtx).Open(); err != nil {
			panic(err)
		}
		go func() {
			defer valve.Lever(b.ValveCtx).Close()
			for item := range b.queue {
				func() {
					if item.finish != nil {
						defer item.finish()
					}
					b.processMsg(item.doWork)
				}()
			}
		}()
	}
}

func (b *BL) startWorkLoop() {
	if b.IsPostgreSQL() {
		go b.startRecoveryListening()
	}
	go b.recoveryAndExpirationScheduler()
	b.startWorkers()
	switch b.JobQueueType {
	case JobQueueTypeMemoryQueue:
		go func() {
			interval := 3600 * time.Second
			timer := time.NewTimer(interval)
			defer timer.Stop()
			defer close(b.queue)
			startExit := false

			for {
				if startExit && b.QueuesIdle() {
					return
				}
				resetTimer(timer, interval)
				select {
				case <-b.stopMemoryJobQueue:
					log.Info(fmt.Errorf("starting leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
					startExit = true
					interval = 1 * time.Second
				case <-b.ValveCtx.Done():
					log.Info(fmt.Errorf("leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
					return
				case msg := <-b.memoryQueue:
					doItem := workerDoItem{
						doWork: msg,
					}
					b.queue <- &doItem
				case <-timer.C:
				}
			}
		}()
	case JobQueueTypeRabbitMQ:
		go b.rabbitMessageQueueManager()
	}
}
func (b *BL) initQueue() {
	if b.ShutdownValve == nil {
		b.ShutdownValve = valve.New()
		b.ValveCtx, b.CancelValveCtx = context.WithCancel(b.ShutdownValve.Context())
		b.queue = make(chan *workerDoItem)
		b.stopMemoryJobQueue = make(chan struct{}, 1)
		b.stopRabbitMQJobQueue = make(chan struct{}, 1)
		b.stopRecovery = make(chan struct{}, 1)
		b.stopRecoveryNotifications1 = make(chan struct{}, 1)
		b.stopRecoveryNotifications2 = make(chan struct{}, 1)
		go func() {
			stop := valve.Lever(b.ValveCtx).Stop()
			select {
			case <-stop:
			case <-b.ValveCtx.Done():
			}
			b.stopping.inc()
			b.stopMemoryJobQueue <- struct{}{}
			b.stopRabbitMQJobQueue <- struct{}{}
			b.stopRecovery <- struct{}{}
			b.stopRecoveryNotifications1 <- struct{}{}
			b.stopRecoveryNotifications2 <- struct{}{}
		}()
		b.memoryQueue = make(chan *doWork, b.jobQueueMemoryQueueLimit)
		b.startWorkLoop()
	}
}
