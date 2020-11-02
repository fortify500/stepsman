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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/dao"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const DefaultHeartBeatInterval = 10 * time.Second

func GetSteps(query *api.GetStepsQuery) ([]api.StepRecord, error) {
	if dao.IsRemote {
		return client.RemoteGetSteps(query)
	} else {
		return getStepsByQuery(query)
	}
}
func ListSteps(query *api.ListQuery) ([]api.StepRecord, *api.RangeResult, error) {
	if dao.IsRemote {
		return client.RemoteListSteps(query)
	}
	return listStepsByQuery(query)
}

func (s *Step) UpdateStateAndStatus(prevStepRecord *api.StepRecord, newStatus api.StepStatusType, newState *dao.StepState, doFinish bool) (*api.StepRecord, error) {
	var stepRecord *api.StepRecord
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runId := prevStepRecord.RunId
		stepRecord, err = dao.GetStepTx(tx, runId, prevStepRecord.Index)
		if err != nil {
			return fmt.Errorf("failed to update database stepRecord row: %w", err)
		}

		// if we are starting check if the stepRecord is already in-progress.
		if !doFinish && stepRecord.Status == api.StepInProgress {
			delta := time.Time(stepRecord.Now).Sub(time.Time(stepRecord.Heartbeat))
			if delta < 0 {
				delta = delta * -1
			}
			heartBeatInterval := s.GetHeartBeatInterval()
			if delta <= heartBeatInterval {
				err = fmt.Errorf("stepRecord is already in progress and has a heartbeat with an interval of %d: %w", heartBeatInterval, ErrStepAlreadyInProgress)
				return fmt.Errorf("failed to update database stepRecord row: %w", err)
			}
		}

		// don't change done if the status did not change.
		if newStatus == stepRecord.Status {
			return ErrStatusNotChanged
		}
		if newState == nil {
			dao.UpdateStatusAndHeartBeatTx(tx, stepRecord, newStatus)
		} else {
			dao.UpdateStateAndStatusAndHeartBeatTx(tx, stepRecord, newStatus, newState)
		}

		if newStatus != api.StepIdle {
			var run *api.RunRecord
			run, err = GetRunByIdTx(tx, runId)
			if err != nil {
				return fmt.Errorf("failed to update database stepRecord row: %w", err)
			}
			if run.Status == api.RunIdle {
				dao.UpdateRunStatusTx(tx, run.Id, api.RunInProgress)
			} else if run.Status == api.RunDone {
				err = ErrRunIsAlreadyDone
				return fmt.Errorf("failed to update database stepRecord status: %w", err)
			}
		}
		stepRecord, err = dao.GetStepTx(tx, runId, prevStepRecord.Index)
		if err != nil {
			return fmt.Errorf("failed to update database stepRecord row: %w", err)
		}

		return nil
	})
	return stepRecord, tErr
}

func (s *Step) GetHeartBeatInterval() time.Duration {
	if s.stepDo.HeartBeatTimeout > 0 {
		return time.Duration(s.stepDo.HeartBeatTimeout) * time.Second
	}
	return DefaultHeartBeatInterval
}
func (s *Step) StartDo(stepRecord *api.StepRecord) error {
	stepRecord, err := s.UpdateStateAndStatus(stepRecord, api.StepInProgress, nil, false)
	if err != nil {
		return fmt.Errorf("failed to start do: %w", err)
	}
	heartbeatInterval := s.GetHeartBeatInterval() / 2
	if heartbeatInterval < DefaultHeartBeatInterval {
		heartbeatInterval = DefaultHeartBeatInterval
	}
	var wg sync.WaitGroup
	heartBeatDone1 := make(chan int)
	heartBeatDone2 := make(chan int)
	heartbeat := func() {
	OUT:
		for {
			select {
			case <-heartBeatDone1:
				break OUT
			case <-heartBeatDone2:
				break OUT
			case <-time.After(heartbeatInterval):
				errBeat := dao.UpdateHeartBeat(stepRecord, stepRecord.StatusUUID)
				if errBeat != nil {
					log.Warn(fmt.Errorf("while trying to update heartbeat: %w", errBeat))
				}
			}
		}
		wg.Done()
	}
	wg.Add(1)
	go heartbeat()
	defer close(heartBeatDone1) //in case of a panic
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(stepRecord.State)))
	decoder.DisallowUnknownFields()
	var prevState dao.StepState
	err = decoder.Decode(&prevState)
	var newState *dao.StepState
	var doErr error
	if err == nil {
		newState, doErr = do(s.doType, s.Do, &prevState)
	}
	close(heartBeatDone2)
	wg.Wait()
	var newStepStatus api.StepStatusType
	if err != nil || doErr != nil {
		newStepStatus = api.StepFailed
	} else {
		newStepStatus = api.StepDone
	}
	stepRecord, err = s.UpdateStateAndStatus(stepRecord, newStepStatus, newState, true)
	return err
}

func getStepsByQuery(query *api.GetStepsQuery) ([]api.StepRecord, error) {
	var stepRecords []api.StepRecord
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		stepRecords, err = dao.GetStepsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to get steps by query: %w", err)
		}
		return nil
	})
	return stepRecords, tErr
}

func listStepsByQuery(query *api.ListQuery) ([]api.StepRecord, *api.RangeResult, error) {
	var stepRecords []api.StepRecord
	var rangeResult *api.RangeResult
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		stepRecords, rangeResult, err = dao.ListStepsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to list steps by query: %w", err)
		}
		return nil
	})
	return stepRecords, rangeResult, tErr
}
