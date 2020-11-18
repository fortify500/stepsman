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
			_, err = template.TransitionStateAndStatus(run.Id, stepRecord.UUID, "", newStatus, nil, query.Force)
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
func (t *Template) TransitionStateAndStatus(runId string, stepUUID string, prevStatusUUID string, newStatus api.StepStatusType, newState *dao.StepState, force bool) (*api.StepRecord, error) {
	var updatedStepRecord api.StepRecord
	var softError *api.Error
	toEnqueue := false
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		partialSteps, err := dao.GetStepsTx(tx, &api.GetStepsQuery{
			UUIDs:            []string{stepUUID},
			ReturnAttributes: []string{dao.HeartBeat, dao.StatusUUID, dao.Status, dao.State, dao.Index, dao.RetriesLeft},
		})
		if err != nil {
			panic(err)
		}
		if len(partialSteps) != 1 {
			panic(fmt.Errorf("only 1 step record expected while updating"))
		}
		partialStepRecord := partialSteps[0]
		step := t.Steps[partialStepRecord.Index-1]
		partialStepRecord.UUID = stepUUID
		partialStepRecord.RunId = runId

		// don't change done if the status did not change.
		if newStatus == partialStepRecord.Status {
			return api.NewError(api.ErrStatusNotChanged, "step status have not changed")
		}
		if newStatus != api.StepDone && newStatus != api.StepFailed {
			newState = &dao.StepState{}
		}

		switch partialStepRecord.Status {
		case api.StepIdle:
			switch newStatus {
			case api.StepPending:
				toEnqueue = true
			case api.StepInProgress:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
			case api.StepFailed:
			case api.StepDone:
			}
		case api.StepPending:
			switch newStatus {
			case api.StepIdle:
			case api.StepInProgress:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
			case api.StepFailed:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
				toEnqueue = true
			case api.StepDone:
			}
		case api.StepInProgress:
			if !force {
				if partialStepRecord.StatusUUID != prevStatusUUID {
					delta := time.Time(partialStepRecord.Now).Sub(time.Time(partialStepRecord.Heartbeat))
					if delta < 0 {
						delta = delta * -1
					}
					heartBeatInterval := step.GetHeartBeatTimeout()
					if delta <= heartBeatInterval {
						return api.NewError(api.ErrStepAlreadyInProgress, "failed to update database stepRecord row, stepRecord is already in progress and has a heartbeat with an interval of %d", heartBeatInterval)
					}
				}
			}
			switch newStatus {
			case api.StepIdle:
			case api.StepPending:
				toEnqueue = true
			case api.StepFailed:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
				toEnqueue = true
			case api.StepDone:
			}
		case api.StepFailed:
			switch newStatus {
			case api.StepIdle:
			case api.StepPending:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
				toEnqueue = true
			case api.StepInProgress:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
			case api.StepDone:
			}
		case api.StepDone:
			switch newStatus {
			case api.StepIdle:
			case api.StepPending:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
				toEnqueue = true
			case api.StepInProgress:
				var toReturn bool
				toReturn, softError = failStepIfNoRetries(tx, &partialStepRecord)
				if toReturn {
					return nil
				}
			case api.StepFailed:
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
			if newStatus == api.StepInProgress || newStatus == api.StepPending {
				if partialStepRecord.Status == api.StepIdle || partialStepRecord.Status == api.StepDone || partialStepRecord.Status == api.StepFailed {
					return api.NewError(api.ErrRunIsAlreadyDone, "failed to update database stepRecord status, run is already done and no change is possible")
				}
				newStatus = api.StepIdle // we want to finish this so it won't be revived.
				newState = &dao.StepState{}
			}
			//otherwise let it finish setting the status.
			toEnqueue = false
		}

		updatedStepRecord = partialStepRecord
		var retriesLeft *int
		var retriesLeftVar int
		var completeBy *int64
		if newStatus == api.StepPending || toEnqueue {
			completeBy = &dao.CompleteByPendingInterval
		} else if newStatus == api.StepInProgress {
			completeBy = step.GetCompleteBy()
			retriesLeftVar = updatedStepRecord.RetriesLeft - 1
			retriesLeft = &retriesLeftVar
		} else if newStatus == api.StepIdle {
			retriesLeftVar = step.Retries + 1
			retriesLeft = &retriesLeftVar
		}

		if newState == nil {
			updatedStepRecord.StatusUUID = dao.UpdateStepStatusAndHeartBeatTx(tx, partialStepRecord.RunId, partialStepRecord.Index, newStatus, completeBy, retriesLeft).StatusUUID
		} else {
			var newStateBytes []byte
			newStateBytes, err = json.Marshal(newState)
			if err != nil {
				panic(err)
			}
			updatedStepRecord.StatusUUID = dao.DB.UpdateStepStateAndStatusAndHeartBeatTx(tx, partialStepRecord.RunId, partialStepRecord.Index, newStatus, string(newStateBytes), completeBy, retriesLeft)
			updatedStepRecord.State = string(newStateBytes)
		}
		updatedStepRecord.Status = newStatus

		return nil
	})
	if tErr == nil && softError != nil {
		tErr = softError
	} else if tErr == nil && toEnqueue {
		tErr = Enqueue(&DoWork{
			UUID: updatedStepRecord.UUID,
		})
	}
	return &updatedStepRecord, tErr
}

func failStepIfNoRetries(tx *sqlx.Tx, partialStepRecord *api.StepRecord) (bool, *api.Error) {
	if partialStepRecord.RetriesLeft < 1 {
		if partialStepRecord.Status != api.StepFailed {
			_ = dao.UpdateStepStatusAndHeartBeatTx(tx, partialStepRecord.RunId, partialStepRecord.Index, api.StepFailed, nil, nil)
		}
		return true, api.NewError(api.ErrStepNoRetriesLeft, "failed to change step status")
	}
	return false, nil
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
func (t *Template) StartDo(runId string, stepUUID string) (*api.StepRecord, error) {
	updatedPartialStepRecord, err := t.TransitionStateAndStatus(runId, stepUUID, "", api.StepInProgress, nil, false)
	if err != nil {
		return nil, fmt.Errorf("failed to start do: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(updatedPartialStepRecord.State)))
	decoder.DisallowUnknownFields()
	var prevState dao.StepState
	err = decoder.Decode(&prevState)
	if err != nil {
		panic(err)
	}
	var newState *dao.StepState
	var doErr error
	step := t.Steps[updatedPartialStepRecord.Index-1]
	newState, doErr = do(step.doType, step.Do, &prevState)
	var newStepStatus api.StepStatusType
	if doErr != nil {
		newStepStatus = api.StepFailed
	} else {
		newStepStatus = api.StepDone
		if step.On.PreDone != nil {
			for _, rule := range step.On.PreDone.Rules {
				if rule.Then != nil {
					if len(rule.Then.Do) > 0 {
						var indices []int64
						for _, thenDo := range rule.Then.Do {
							index, ok := step.template.labelsToIndices[thenDo.Label]
							if !ok {
								panic(fmt.Errorf("label should have an index"))
							}
							indices = append(indices, index)
						}
						if len(indices) > 0 {
							var uuidsToEnqueue []dao.UUIDAndStatusUUID
							if tErr := dao.Transactional(func(tx *sqlx.Tx) error {
								uuidsToEnqueue = dao.DB.UpdateManyStatusAndHeartBeatTx(tx, runId, indices, api.StepPending, []api.StepStatusType{api.StepIdle}, &dao.CompleteByPendingInterval, nil)
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
	updatedPartialStepRecord, err = t.TransitionStateAndStatus(runId, stepUUID, updatedPartialStepRecord.StatusUUID, newStepStatus, newState, false)
	return updatedPartialStepRecord, err
}

func DoStep(params *api.DoStepParams, synchronous bool) (*api.DoStepResult, error) {
	if dao.IsRemote {
		return client.RemoteDoStep(params) //always async
	} else {
		return doStep(params, synchronous)
	}
}

var emptyDoStepResult = api.DoStepResult{}

func doStep(params *api.DoStepParams, synchronous bool) (*api.DoStepResult, error) {
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
	return doStepSynchronous(params)
}

func doStepSynchronous(params *api.DoStepParams) (*api.DoStepResult, error) {
	var run *api.RunRecord
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		runs, err := dao.GetRunsByStepUUIDsTx(tx, &api.GetRunsQuery{
			Ids:              []string{params.UUID},
			ReturnAttributes: []string{dao.Id, dao.Status, dao.Template},
		})
		if err != nil || len(runs) != 1 {
			return api.NewError(api.ErrRecordNotFound, "failed to locate run by step uuid [%s]", params.UUID)
		}
		run = &runs[0]
		return nil
	})
	if tErr != nil {
		return nil, fmt.Errorf("failed to do step synchronously: %w", tErr)
	}
	if run.Status == api.RunDone {
		return nil, api.NewError(api.ErrRunIsAlreadyDone, "failed to do step, run is already done and no change is possible")
	}

	template := Template{}
	err := template.LoadFromBytes(false, []byte(run.Template))
	if err != nil {
		return nil, fmt.Errorf("failed to do step, failed to convert step record to step: %w", err)
	}
	_, err = template.StartDo(run.Id, params.UUID)
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
