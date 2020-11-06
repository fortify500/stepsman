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

func UpdateStep(query *api.UpdateQuery) error {
	if dao.IsRemote {
		return client.RemoteUpdateStep(query)
	} else {
		return updateStep(query)
	}
}

func updateStep(query *api.UpdateQuery) error {
	vetErr := dao.VetIds([]string{query.Id})
	if vetErr != nil {
		return fmt.Errorf("failed to update step: %w", vetErr)
	}
	if len(query.Changes) > 0 {
		if len(query.Changes) > 2 {
			return api.NewError(api.ErrInvalidParams, "more than 2 change types. currently only 2 change type are supported: status or heartbeat")
		}
		val, ok := query.Changes["status"]
		if ok {
			var statusStr string
			statusStr, ok = val.(string)
			if !ok {
				return api.NewError(api.ErrInvalidParams, "status must be of string type")
			}
			newStatus, err := api.TranslateToStepStatus(statusStr)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}

			stepRecords, err := GetSteps(&api.GetStepsQuery{
				UUIDs: []string{query.Id},
			})
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			if len(stepRecords) != 1 {
				return api.NewError(api.ErrRecordNotFound, "failed to locate step record for uuid [%s]", query.Id)
			}

			stepRecord := stepRecords[0]
			run, err := GetRun(stepRecord.RunId)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			template := Template{}
			err = template.LoadFromBytes(false, []byte(run.Template))
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			step := template.Steps[stepRecord.Index-1]

			_, err = step.UpdateStateAndStatus(&stepRecord, newStatus, nil, false)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
		} else {
			val, ok = query.Changes["heartbeat"]
			if !ok {
				return api.NewError(api.ErrInvalidParams, "unsupported update fields provided")
			}
			var statusUUIDStr string
			statusUUIDStr, ok = val.(string)
			if !ok {
				return api.NewError(api.ErrInvalidParams, "status uuid must be of string type")
			}
			err := dao.UpdateHeartBeat(query.Id, statusUUIDStr)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
		}
	}
	return nil
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
				return api.NewError(api.ErrStepAlreadyInProgress, "failed to update database stepRecord row, stepRecord is already in progress and has a heartbeat with an interval of %d", heartBeatInterval)
			}
		} else if stepRecord.Status == api.StepInProgress {
			if stepRecord.StatusUUID != prevStepRecord.StatusUUID {
				return api.NewError(api.ErrRecordNotAffected, "while updating step status, no rows where affected, suggesting status_uuid has changed (but possibly the record have been deleted) for step uuid: %s, and status uuid: %s", prevStepRecord.UUID, prevStepRecord.StatusUUID)
			}
		}

		// don't change done if the status did not change.
		if newStatus == stepRecord.Status {
			return api.NewError(api.ErrStatusNotChanged, "step status have not changed")
		}
		//remote state if we are back to idle.
		if newStatus == api.StepIdle {
			if newState == nil {
				newState = &dao.StepState{}
			}
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
				return api.NewError(api.ErrRunIsAlreadyDone, "failed to update database stepRecord status, run is already done and no change is possible")
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
func (s *Step) StartDo(stepRecord *api.StepRecord) (*api.StepRecord, error) {
	updatedStepRecord, err := s.UpdateStateAndStatus(stepRecord, api.StepInProgress, nil, false)
	if err != nil {
		return nil, fmt.Errorf("failed to start do: %w", err)
	}
	heartbeatInterval := s.GetHeartBeatInterval() / 2
	if heartbeatInterval < DefaultHeartBeatInterval {
		heartbeatInterval = DefaultHeartBeatInterval
	}
	var wg sync.WaitGroup
	var errBeat error
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
				errBeat = dao.UpdateHeartBeat(updatedStepRecord.UUID, updatedStepRecord.StatusUUID)
				if errBeat != nil {
					log.Warn(fmt.Errorf("while trying to update heartbeat: %w", errBeat))
					break OUT
				}
			}
		}
		wg.Done()
	}
	wg.Add(1)
	go heartbeat()
	defer close(heartBeatDone1) //in case of a panic
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(updatedStepRecord.State)))
	decoder.DisallowUnknownFields()
	var prevState dao.StepState
	err = decoder.Decode(&prevState)
	if err != nil {
		panic(err)
	}
	var newState *dao.StepState
	var doErr error
	if errBeat == nil {
		newState, doErr = do(s.doType, s.Do, &prevState)
	}
	close(heartBeatDone2)
	wg.Wait()
	var newStepStatus api.StepStatusType
	if doErr != nil || errBeat != nil {
		newStepStatus = api.StepFailed
	} else {
		newStepStatus = api.StepDone
	}
	updatedStepRecord, err = s.UpdateStateAndStatus(updatedStepRecord, newStepStatus, newState, true)
	return updatedStepRecord, err
}

func DoStep(uuid string) (*api.DoStepResult, error) {
	if dao.IsRemote {
		return client.RemoteDoStep(uuid)
	} else {
		return doStep(uuid)
	}
}

func doStep(uuid string) (*api.DoStepResult, error) {
	stepRecords, err := GetSteps(&api.GetStepsQuery{
		UUIDs: []string{uuid},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to do step: %w", err)
	}
	if len(stepRecords) != 1 {
		return nil, api.NewError(api.ErrRecordNotFound, "failed to locate step uuid [%s]", uuid)
	}
	stepRecord := stepRecords[0]
	run, err := GetRun(stepRecord.RunId)
	if err != nil {
		return nil, fmt.Errorf("failed to do step: %w", err)
	}
	if run.Status == api.RunDone {
		return nil, api.NewError(api.ErrRunIsAlreadyDone, "failed to do step, run is already done and no change is possible")
	}

	template := Template{}
	err = template.LoadFromBytes(false, []byte(run.Template))
	if err != nil {
		return nil, fmt.Errorf("failed to do step, failed to convert step record to step: %w", err)
	}
	step := template.Steps[stepRecord.Index-1]
	updatedStepRecord, err := step.StartDo(&stepRecord)
	if err != nil {
		return nil, fmt.Errorf("failed to start do: %w", err)
	}
	return &api.DoStepResult{
		StatusUUID: updatedStepRecord.StatusUUID,
	}, nil
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
