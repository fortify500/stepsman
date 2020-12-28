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

type doWork struct {
	item     uuid.UUID
	options  api.Options
	itemType WorkType
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
	timer := time.NewTimer(10 * time.Second) // timer that will be GC even if not reached
	defer timer.Stop()
	select {
	case <-b.stop:
		return api.NewError(api.ErrShuttingDown, "leaving enqueue, server is shutting down")
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
		switch msg.itemType {
		case StepWorkType:
			_, _, err = b.doStep(nil, &api.DoStepByUUIDParams{
				UUID:    msg.item,
				Options: msg.options,
			}, true)
		case RunWorkType:
			err = b.doRun(msg.options, msg.item)
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
				b.processMsg(item)
			}
		}()
	}
}
func (b *BL) startWorkLoop() {
	b.startWorkers()
	if b.IsPostgreSQL() {
		go b.startRecoveryListening()
	}
	go b.recoveryAndExpirationScheduler()
	go func() {
		defer close(b.queue)
		startExit := false
		for {
			if startExit && b.QueuesIdle() {
				return
			}
			select {
			case <-b.stop:
				log.Info(fmt.Errorf("starting leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
				startExit = true
			case <-b.ValveCtx.Done():
				log.Info(fmt.Errorf("leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
				return
			case msg := <-b.memoryQueue:
				b.queue <- msg
			}
		}
	}()
}

func (b *BL) initQueue() {
	if b.ShutdownValve == nil {
		b.ShutdownValve = valve.New()
		b.ValveCtx, b.CancelValveCtx = context.WithCancel(b.ShutdownValve.Context())
		b.queue = make(chan *doWork)
		b.stop = valve.Lever(b.ValveCtx).Stop()
		b.memoryQueue = make(chan *doWork, b.jobQueueMemoryQueueLimit)
		b.startWorkLoop()
	}
}
