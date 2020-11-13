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

			_, err = step.UpdateStateAndStatus(&stepRecord, newStatus, nil, false, false)
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
			err := dao.UpdateStepHeartBeat(query.Id, statusUUIDStr)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
		}
	}
	return nil
}
func (s *Step) UpdateStateAndStatus(prevStepRecord *api.StepRecord, newStatus api.StepStatusType, newState *dao.StepState, doFinish bool, mustMatchPrevStatus bool) (*api.StepRecord, error) {
	var updatedStepRecord api.StepRecord
	var stopErr *api.Error
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runId := prevStepRecord.RunId
		partialSteps, err := dao.GetStepsTx(tx, &api.GetStepsQuery{
			UUIDs:            []string{prevStepRecord.UUID},
			ReturnAttributes: []string{dao.HeartBeat, dao.StatusUUID, dao.Status},
		})
		if err != nil {
			panic(err)
		}
		if len(partialSteps) != 1 {
			panic(fmt.Errorf("only 1 step record expected while updating"))
		}
		partialStepRecord := partialSteps[0]
		if mustMatchPrevStatus && partialStepRecord.Status != prevStepRecord.Status {
			return api.NewError(api.ErrPrevStepStatusDoesNotMatch, "failed an assumption checking, prev status: %s, transaction status: %s", prevStepRecord.Status.TranslateStepStatus(), partialStepRecord.Status.TranslateStepStatus())
		}

		if partialStepRecord.Status == api.StepInProgress {
			if doFinish {
				if partialStepRecord.StatusUUID != prevStepRecord.StatusUUID {
					return api.NewError(api.ErrRecordNotAffected, "while updating step status, no rows where affected, suggesting status_uuid has changed (but possibly the record have been deleted) for step uuid: %s, and status uuid: %s", prevStepRecord.UUID, prevStepRecord.StatusUUID)
				}
			} else {
				delta := time.Time(partialStepRecord.Now).Sub(time.Time(partialStepRecord.Heartbeat))
				if delta < 0 {
					delta = delta * -1
				}
				heartBeatInterval := s.GetHeartBeatTimeout()
				if delta <= heartBeatInterval {
					return api.NewError(api.ErrStepAlreadyInProgress, "failed to update database stepRecord row, stepRecord is already in progress and has a heartbeat with an interval of %d", heartBeatInterval)
				}
			}
		}

		// don't change done if the status did not change.
		if newStatus == partialStepRecord.Status {
			return api.NewError(api.ErrStatusNotChanged, "step status have not changed")
		}
		//remote state if we are back to idle.
		if newStatus == api.StepIdle {
			if newState == nil {
				newState = &dao.StepState{}
			}
		}

		var run *api.RunRecord
		run, err = GetRunByIdTx(tx, runId)
		if err != nil {
			return fmt.Errorf("failed to update database stepRecord row: %w", err)
		}
		if run.Status == api.RunIdle {
			dao.UpdateRunStatusTx(tx, run.Id, api.RunInProgress)
		} else if run.Status == api.RunDone {
			stopErr = api.NewError(api.ErrRunIsAlreadyDone, "failed to update database stepRecord status, run is already done and no change is possible")
			if newStatus == api.StepInProgress || newStatus == api.StepPending {
				if partialStepRecord.Status == api.StepIdle || partialStepRecord.Status == api.StepDone {
					return stopErr
				}
				newStatus = api.StepIdle // we want to finish this so it won't be revived.
				newState = nil
			} else {
				return stopErr
			}
		}

		updatedStepRecord = *prevStepRecord
		var completeBy *int64
		if newStatus == api.StepPending {
			completeBy = &dao.CompleteByPendingInterval
		} else if newStatus == api.StepInProgress {
			completeBy = s.GetCompleteBy()
		}
		if newState == nil {
			updatedStepRecord.StatusUUID = dao.UpdateStepStatusAndHeartBeatTx(tx, prevStepRecord.RunId, prevStepRecord.Index, newStatus, completeBy).StatusUUID
		} else {
			var newStateBytes []byte
			newStateBytes, err = json.Marshal(newState)
			if err != nil {
				panic(err)
			}
			updatedStepRecord.StatusUUID = dao.DB.UpdateStepStateAndStatusAndHeartBeatTx(tx, prevStepRecord.RunId, prevStepRecord.Index, newStatus, string(newStateBytes), completeBy)
			updatedStepRecord.State = string(newStateBytes)
		}
		updatedStepRecord.Status = newStatus

		return nil
	})
	if tErr == nil && stopErr != nil {
		tErr = stopErr
	}
	return &updatedStepRecord, tErr
}
func (s *Step) GetCompleteBy() *int64 {
	if s.stepDo.CompleteBy > 0 {
		return &s.stepDo.CompleteBy
	}
	return &CompleteByInProgressInterval
}

func (s *Step) GetHeartBeatTimeout() time.Duration {
	if s.stepDo.HeartBeatTimeout > 0 {
		return time.Duration(s.stepDo.HeartBeatTimeout) * time.Second
	}
	return DefaultHeartBeatInterval
}
func (s *Step) StartDo(stepRecord *api.StepRecord, checkPending bool) (*api.StepRecord, error) {
	updatedStepRecord, err := s.UpdateStateAndStatus(stepRecord, api.StepInProgress, nil, false, checkPending)
	if err != nil {
		return nil, fmt.Errorf("failed to start do: %w", err)
	}
	var errBeat error
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
	var newStepStatus api.StepStatusType
	if doErr != nil || errBeat != nil {
		newStepStatus = api.StepFailed
	} else {
		newStepStatus = api.StepDone
		if s.On.PreDone != nil {
			for _, rule := range s.On.PreDone.Rules {
				if rule.Then != nil {
					if len(rule.Then.Do) > 0 {
						var indices []int64
						for _, do := range rule.Then.Do {
							index, ok := s.template.labelsToIndices[do.Label]
							if !ok {
								panic(fmt.Errorf("label should have an index"))
							}
							indices = append(indices, index)
						}
						if len(indices) > 0 {
							var uuidsToEnqueue []dao.UUIDAndStatusUUID
							if tErr := dao.Transactional(func(tx *sqlx.Tx) error {
								uuidsToEnqueue = dao.DB.UpdateManyStatusAndHeartBeatTx(tx, stepRecord.RunId, indices, api.StepPending, []api.StepStatusType{api.StepIdle}, &dao.CompleteByPendingInterval)
								return nil
							}); tErr != nil {
								panic(tErr)
							}
							for _, item := range uuidsToEnqueue {
								if err = Enqueue(&DoWork{
									UUID: item.UUID,
								}); err != nil {
									return nil, err
								}
							}
						}
					}
				}
			}
		}

	}
	updatedStepRecord, err = s.UpdateStateAndStatus(updatedStepRecord, newStepStatus, newState, true, true)
	return updatedStepRecord, err
}

func DoStep(params *api.DoStepParams, synchronous bool, checkPending bool) (*api.DoStepResult, error) {
	if dao.IsRemote {
		return client.RemoteDoStep(params) //always async
	} else {
		return doStep(params, synchronous, checkPending)
	}
}

var emptyDoStepResult = api.DoStepResult{}

func doStep(params *api.DoStepParams, synchronous bool, checkPending bool) (*api.DoStepResult, error) {
	if !synchronous {
		tErr := dao.Transactional(func(tx *sqlx.Tx) error {
			updated := dao.DB.UpdateManyStatusAndHeartBeatByUUIDTx(tx, []string{params.UUID}, api.StepPending, []api.StepStatusType{api.StepIdle}, &dao.CompleteByPendingInterval)
			if len(updated) != 1 {
				return api.NewError(api.ErrPrevStepStatusDoesNotMatch, "enqueue failed because it is highly probably the step with uuid:%s, is not in a pending state or the record is missing")
			}
			return nil
		})
		if tErr != nil {
			panic(tErr)
		}
		err := Enqueue((*DoWork)(params))
		if err != nil {
			return nil, fmt.Errorf("failed to equeue step:%w", err)
		}
		return &emptyDoStepResult, nil
	}
	return doStepSynchronous(params, checkPending)
}

func doStepSynchronous(params *api.DoStepParams, checkPending bool) (*api.DoStepResult, error) {
	stepRecords, err := GetSteps(&api.GetStepsQuery{
		UUIDs: []string{params.UUID},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to do step: %w", err)
	}
	if len(stepRecords) != 1 {
		return nil, api.NewError(api.ErrRecordNotFound, "failed to locate step uuid [%s]", params.UUID)
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
	_, err = step.StartDo(&stepRecord, checkPending)
	if err != nil {
		return nil, fmt.Errorf("failed to start do: %w", err)
	}
	return &api.DoStepResult{}, nil
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
