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
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/go-chi/valve"
	log "github.com/sirupsen/logrus"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

type DoWork api.DoStepParams

var memoryQueue chan *DoWork
var queue = make(chan *DoWork)
var stop <-chan struct{}

type WorkCounter int32

var workCounter WorkCounter

func (c *WorkCounter) inc() int32 {
	return atomic.AddInt32((*int32)(c), 1)
}
func (c *WorkCounter) dec() int32 {
	return atomic.AddInt32((*int32)(c), -1)
}

func (c *WorkCounter) get() int32 {
	return atomic.LoadInt32((*int32)(c))
}

func Enqueue(do *DoWork) error {
	select {
	case <-stop:
		return api.NewError(api.ErrShuttingDown, "leaving enqueue, server is shutting down")
	case <-ValveCtx.Done():
		return api.NewError(api.ErrShuttingDown, "leaving enqueue, server is shutting down")
	case memoryQueue <- do:
		return nil
	case <-time.After(10 * time.Second):
		return api.NewError(api.ErrJobQueueUnavailable, "leaving enqueue, timeout writing to the job queue")
	}
}

// try not to use this, this is primarily for testing
func QueuesIdle() bool {
	if len(memoryQueue) == 0 && len(queue) == 0 && workCounter.get() == 0 {
		return true
	}
	return false
}

//func Dequeue() (*DoWork, *api.Error) {
//	select {
//	case <-stop:
//		return nil, api.NewError(api.ErrShuttingDown, "cannot serve from queue, server is shutting down")
//
//	case <-valveContext.Done():
//		return nil, api.NewError(api.ErrShuttingDown, "cannot serve from queue, server is shutting down")
//	case msg := <-queue:
//		return msg, nil
//	}
//	return nil, api.NewError(api.ErrShuttingDown, "cannot serve from queue, server is shutting down")
//}

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
			defer log.Debug(fmt.Errorf("failed to serve: %w", err))
		}
	}()
	err = recoverableFunction()
}
func processMsg(msg *DoWork) {
	workCounter.inc()
	defer workCounter.dec()
	if log.IsLevelEnabled(log.TraceLevel) {
		log.Tracef("processing msg: %#v", msg)
	}
	recoverable(func() error {
		_, err := doStepSynchronous((*api.DoStepParams)(msg))
		return err
	})
}

func startWorkers() {
	for w := 0; w < JobQueueNumberOfWorkers; w++ {
		if err := valve.Lever(ValveCtx).Open(); err != nil {
			panic(err)
		}
		go func() {
			defer valve.Lever(ValveCtx).Close()
			for item := range queue {
				processMsg(item)
			}
		}()
	}
}
func startWorkLoop() {
	startWorkers()
	go StartRecoveryListening()
	go func() {
		defer close(queue)
		for {
			select {
			case <-stop:
				log.Info(fmt.Errorf("leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
				return
			case <-ValveCtx.Done():
				log.Info(fmt.Errorf("leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
				return
			case msg := <-memoryQueue:
				queue <- msg
			}
		}
	}()
}

var mu sync.Mutex

func InitQueue() {
	mu.Lock()
	defer mu.Unlock()
	if ShutdownValve == nil {
		ShutdownValve = valve.New()
		ValveCtx = ShutdownValve.Context()
		stop = valve.Lever(ValveCtx).Stop()
		memoryQueue = make(chan *DoWork, JobQueueMemoryQueueLimit)
		startWorkLoop()
	}
}