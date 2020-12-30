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
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type RabbitMQ struct {
	name            string
	connection      *amqp.Connection
	channel         *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation
	notifyReturn    chan amqp.Return
}

func (b *BL) rabbitMessageQueueManager() {
	defer close(b.queue)
	startExit := false
	for {
		recoverable(func() error {
			b.rabbitMQConnectAndServe(&startExit)
			return nil
		})
		if startExit && b.QueuesIdle() {
			log.Info("exiting rabbitmq work loop, queue empty")
			return
		}
		select {
		case <-b.ValveCtx.Done():
			log.Info(fmt.Errorf("leaving rabbitmq work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
			return
		default:
		}
	}
}

func (b *BL) rabbitMQConnectAndServe(startExit *bool) {
	var err error
	var bufferLock sync.Mutex // handle race conditions whereby rabbitmq recycles buffers (apparently)
	var consume <-chan amqp.Delivery
	var conn *amqp.Connection
	rabbitMQ := RabbitMQ{
		name: b.rabbitMQJobQueueName,
	}
	log.Info("attempting to connect to rabbitmq")
	doneReceive := make(chan struct{}, 10)
	receiverFailed := make(chan struct{}, 10)
	defer func() {
		doneReceive <- struct{}{}
	}()

	exitTimerInterval := 3600 * time.Second
	exitTimer := time.NewTimer(exitTimerInterval)
	defer exitTimer.Stop()

	conn, err = rabbitMQ.connect(b.rabbitMQURIConnectionString)
	defer rabbitMQ.Close()
	if err != nil {
		log.Error(fmt.Errorf("failed to connect to rabbitmq: %w", err))
		select {
		case <-b.ValveCtx.Done():
			log.Info("leaving rabbitmq listener, shutting down")
			return
		case <-time.After(b.rabbitMQReconnectDelay):
			break
		}
		return
	}

	for {
		err = rabbitMQ.init(b, conn)
		if err != nil {
			log.Error(fmt.Errorf("failed to initialize channel: %w", err))
			select {
			case <-b.ValveCtx.Done():
				log.Info("leaving rabbitmq listener, shutting down")
				return
			case <-time.After(b.rabbitMQReInitDelay):
				return
			}
		}
		go rabbitMQ.rabbitMQHandleReturns(b, &bufferLock)
		rabbitMQ.clearChannel(&doneReceive)
		rabbitMQ.clearChannel(&receiverFailed)
		var wg sync.WaitGroup
		if !*startExit {
			consume, err = rabbitMQ.channel.Consume(
				rabbitMQ.name,
				"",
				false,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Error(fmt.Errorf("failed to initialize consumer channel: %w", err))
				select {
				case <-b.ValveCtx.Done():
					log.Error("leaving rabbitmq listener, shutting down")
					return
				case <-time.After(b.rabbitMQReInitDelay):
					return
				}
			}
			wg.Add(1)
			go rabbitMQ.rabbitConsumer(b, &wg, &doneReceive, &receiverFailed, &consume, &bufferLock)
		}
	Sending:
		for {
			if *startExit && b.QueuesIdle() {
				log.Info("returning for exiting from rabbitmq work loop, queue empty")
				return
			}
			resetTimer(exitTimer, exitTimerInterval)
			select {
			case <-receiverFailed:
				time.Sleep(b.rabbitMQReInitDelay)
				rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
				break Sending
			case <-exitTimer.C:
			case <-b.stopRabbitMQJobQueue:
				if !*startExit {
					log.Info("starting to stop rabbitmq work loop")
					*startExit = true
					exitTimerInterval = 1 * time.Second
					doneReceive <- struct{}{}
					wg.Wait()
				}
			case <-b.ValveCtx.Done():
				log.Error(fmt.Errorf("leaving rabbitmq work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
				rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
				return
			case <-rabbitMQ.notifyConnClose:
				log.Error("rabbitmq connection is closed")
				rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
				return
			case <-rabbitMQ.notifyChanClose:
				log.Error("rabbitmq channel is closed")
				rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
				break Sending
			case msg := <-b.memoryQueue:
				var msgData []byte
				msgData, err = json.Marshal(msg)
				if err != nil {
					panic(err)
				}
			MessageSending:
				for {
					msgCopy := make([]byte, len(msgData))
					bufferLock.Lock()
					copy(msgCopy, msgData)
					bufferLock.Unlock()
					err = rabbitMQ.channel.Publish(
						"",
						rabbitMQ.name,
						true,
						false,
						amqp.Publishing{
							ContentType: "application/json",
							Body:        msgCopy,
						},
					)
					if err != nil {
						log.Error(fmt.Errorf("rabbitmq publish failed: %w", err))
						select {
						case <-b.ValveCtx.Done():
							log.Error(fmt.Errorf("leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
							rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
							return
						default:
							b.memoryQueue <- msg
							rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
							break Sending
						}
					}
					select {
					case <-b.ValveCtx.Done():
						log.Error(fmt.Errorf("leaving work loop: %w", api.NewError(api.ErrShuttingDown, "server is shutting down")))
						rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
						return
					case confirmMsg := <-rabbitMQ.notifyConfirm:
						if confirmMsg.Ack {
							log.Tracef("publish confirmed for: %v", msg)
							break MessageSending
						} else {
							log.Error("publish didn't confirm, ack is false")
							b.memoryQueue <- msg
							rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
							break Sending
						}
					case <-time.After(b.rabbitMQResendDelay):
						log.Error("publish didn't confirm on time")
						b.memoryQueue <- msg
						rabbitMQ.LeaveConsumer(startExit, &doneReceive, &wg)
						break Sending
					}
				}
			}
		}
	}
}

func (r *RabbitMQ) rabbitMQHandleReturns(BL *BL, bufferLock *sync.Mutex) {
	for msg := range r.notifyReturn {
		do := doWork{}
		msgCopy := make([]byte, len(msg.Body))
		bufferLock.Lock()
		copy(msgCopy, msg.Body)
		bufferLock.Unlock()
		err := json.Unmarshal(msgCopy, &do)
		if err != nil {
			log.Error("failed to unmarshal from rabbitmq: %w", err)
			continue
		}
		BL.memoryQueue <- &do
		log.Error("rabbitmq msg returned: %v", msg)
	}
}

func (r *RabbitMQ) clearChannel(c *chan struct{}) {
	for {
		select {
		case <-*c:
			continue
		default:
		}
		break
	}
}

func (r *RabbitMQ) LeaveConsumer(startExit *bool, done *chan struct{}, wg *sync.WaitGroup) {
	if !*startExit {
		*done <- struct{}{}
		wg.Wait()
	}
	return
}

func (r *RabbitMQ) rabbitConsumer(BL *BL, wg *sync.WaitGroup, done *chan struct{}, receiverFailed *chan struct{}, consume *<-chan amqp.Delivery, bufferLock *sync.Mutex) {
	var funErr error
	var sentFailed bool
	defer wg.Done()
	for {
		select {
		case <-*done:
			log.Info("rabbitmq leaving receive consumer")
			return
		case msg := <-*consume:
			do := doWork{}
			// look like rabbitmq library bug that occurs when a queue is deleted.
			if msg.Body == nil && msg.RoutingKey == "" && msg.ConsumerTag == "" {
				log.Errorf("rabbitmq receive consumer has failed, exiting: %v", msg)
				if !sentFailed {
					*receiverFailed <- struct{}{}
				}
				sentFailed = true
				time.Sleep(10 * time.Millisecond) //slow it down a bit. usually this is an event storm.
				continue
			}
			msgCopy := make([]byte, len(msg.Body))
			bufferLock.Lock()
			copy(msgCopy, msg.Body)
			bufferLock.Unlock()
			funErr = json.Unmarshal(msgCopy, &do)
			if funErr != nil {
				log.Error(fmt.Errorf("rabbitmq failed to unmarshal in receive consumer: %w", funErr))
				funErr = msg.Reject(false)
				if funErr != nil {
					log.Error(fmt.Errorf("rabbitmq failed to reject at receive consumer: %w", funErr))
				}
			} else {
				doItem := workerDoItem{
					doWork: &do,
					finish: func() {
						tmpErr := msg.Ack(false)
						if tmpErr != nil {
							log.Error(fmt.Errorf("rabbitmq failed to ack a recieve consumer item: %w", tmpErr))
						}
					},
				}
				select {
				case BL.queue <- &doItem:
				case <-*done:
					log.Info("rabbitmq leaving receive consumer while processing message, will requeue")
					funErr = msg.Nack(false, true)
					if funErr != nil {
						log.Error(fmt.Errorf("rabbitmq failed to requeue msg in receive consumer while leaving: %w", funErr))
					}
					return
				}
			}
		}
	}
}

func (r *RabbitMQ) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)

	if err != nil {
		return nil, fmt.Errorf("rabbitmq failed to connect: %w", err)
	}

	r.setConnectionDetails(conn)
	log.Infof("rabbitmq connected to server: %s", addr)
	return conn, nil
}

func (r *RabbitMQ) Close() {
	recoverable(func() error {
		if r.connection != nil {
			err := r.connection.Close()
			if err != nil {
				log.Error(fmt.Errorf("rabbitmq failed to close connection: %w", err))
			}
		}
		return nil
	})
}

func (r *RabbitMQ) init(b *BL, conn *amqp.Connection) error {
	ch, err := conn.Channel()

	if err != nil {
		return fmt.Errorf("rabbitmq failed to initialize channel: %w", err)
	}

	err = ch.Confirm(false)

	if err != nil {
		return fmt.Errorf("rabbitmq failed to initialize confirm: %w", err)
	}
	_, err = ch.QueueDeclare(
		r.name,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)

	if err != nil {
		return fmt.Errorf("rabbitmq failed to initialize queue: %w", err)
	}
	err = ch.Qos(
		b.jobQueueNumberOfWorkers, // prefetch count
		0,                         // prefetch size
		false,                     // global
	)
	if err != nil {
		return fmt.Errorf("rabbitmq failed to initialize channel QOS: %w", err)
	}
	r.setChannelDetails(ch)
	log.Info("rabbitmq finished initialization")

	return nil
}

// setConnectionDetails takes a new connection to the queue,
// and updates the close listener to reflect this.
func (r *RabbitMQ) setConnectionDetails(connection *amqp.Connection) {
	r.connection = connection
	r.notifyConnClose = make(chan *amqp.Error, 10)
	r.connection.NotifyClose(r.notifyConnClose)
}

// setChannelDetails takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (r *RabbitMQ) setChannelDetails(channel *amqp.Channel) {
	r.channel = channel
	r.notifyChanClose = make(chan *amqp.Error, 10)
	r.notifyConfirm = make(chan amqp.Confirmation, 10)
	r.notifyReturn = make(chan amqp.Return, 10)
	r.channel.NotifyClose(r.notifyChanClose)
	r.channel.NotifyPublish(r.notifyConfirm)
	r.channel.NotifyReturn(r.notifyReturn)
}
