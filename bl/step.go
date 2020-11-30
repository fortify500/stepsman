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
	"errors"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/dao"
	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
	"time"
)

const defaultHeartBeatInterval = 10 * time.Second

func (b *BL) GetSteps(query *api.GetStepsQuery) ([]api.StepRecord, error) {
	if dao.IsRemote {
		return b.Client.RemoteGetSteps(query)
	} else {
		return b.getStepsByQuery(query)
	}
}
func (b *BL) ListSteps(query *api.ListQuery) ([]api.StepRecord, *api.RangeResult, error) {
	if dao.IsRemote {
		return b.Client.RemoteListSteps(query)
	}
	return b.listStepsByQuery(query)
}

func (b *BL) UpdateStep(query *api.UpdateQuery) error {
	if dao.IsRemote {
		return b.Client.RemoteUpdateStep(query)
	} else {
		return b.updateStep(query)
	}
}

func (b *BL) updateStep(query *api.UpdateQuery) error {
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

			stepRecords, err := b.GetSteps(&api.GetStepsQuery{
				UUIDs: []string{query.Id},
			})
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			if len(stepRecords) != 1 {
				return api.NewError(api.ErrRecordNotFound, "failed to locate step record for uuid [%s]", query.Id)
			}

			stepRecord := stepRecords[0]
			run, err := b.GetRun(stepRecord.RunId)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			template := Template{}
			err = template.LoadFromBytes(b, stepRecord.RunId, false, []byte(run.Template))
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			_, err = template.TransitionStateAndStatus(b, run.Id, stepRecord.UUID, "", newStatus, "", nil, nil, query.Force)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
		} else {
			val, ok = query.Changes["heartbeat"]
			if !ok {
				return api.NewError(api.ErrInvalidParams, "unsupported update fields provided")
			}
			var statusOwnerStr string
			statusOwnerStr, ok = val.(string)
			if !ok {
				return api.NewError(api.ErrInvalidParams, "status owner uuid4 must be of string type")
			}
			err := b.DAO.UpdateStepHeartBeat(query.Id, statusOwnerStr)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
		}
	}
	return nil
}
func (t *Template) TransitionStateAndStatus(BL *BL, runId string, stepUUID string, prevStatusOwner string, newStatus api.StepStatusType, newStatusOwner string, context api.Context, newState *dao.StepState, force bool) (*api.StepRecord, error) {
	var updatedStepRecord api.StepRecord
	var softError *api.Error
	toEnqueue := false
	tErr := BL.DAO.Transactional(func(tx *sqlx.Tx) error {
		var err error
		partialSteps, err := dao.GetStepsTx(tx, &api.GetStepsQuery{
			UUIDs:            []string{stepUUID},
			ReturnAttributes: []string{dao.HeartBeat, dao.StatusOwner, dao.Status, dao.State, dao.Index, dao.RetriesLeft},
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
		var stepContext api.Context
		statusOwner := newStatusOwner
		switch partialStepRecord.Status {
		case api.StepIdle:
			switch newStatus {
			case api.StepPending:
				toEnqueue = true
				stepContext = context
			case api.StepInProgress:
				stepContext = context
				var toReturn bool
				toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, stepContext, newState)
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
				statusOwner = partialStepRecord.StatusOwner
				var toReturn bool
				toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, nil, newState)
				if toReturn {
					return nil
				}
			case api.StepFailed:
				statusOwner = partialStepRecord.StatusOwner
				var toReturn bool
				toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, nil, newState)
				if toReturn {
					return nil
				}
				toEnqueue = true
			case api.StepDone:
			}
		case api.StepInProgress:
			if !force {
				if partialStepRecord.StatusOwner != prevStatusOwner {
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
				statusOwner = partialStepRecord.StatusOwner
				toEnqueue = true
			case api.StepFailed:
				statusOwner = partialStepRecord.StatusOwner
				var toReturn bool
				toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, nil, newState)
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
				statusOwner = partialStepRecord.StatusOwner
				var toReturn bool
				toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, nil, newState)
				if toReturn {
					return nil
				}
				toEnqueue = true
			case api.StepInProgress:
				statusOwner = partialStepRecord.StatusOwner
				var toReturn bool
				toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, nil, newState)
				if toReturn {
					return nil
				}
			case api.StepDone:
			}
		case api.StepDone:
			if force {
				switch newStatus {
				case api.StepIdle:
				case api.StepPending:
					var toReturn bool
					toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, nil, newState)
					if toReturn {
						return nil
					}
					toEnqueue = true
				case api.StepInProgress:
					var toReturn bool
					toReturn, softError = BL.failStepIfNoRetries(tx, &partialStepRecord, statusOwner, nil, newState)
					if toReturn {
						return nil
					}
				case api.StepFailed:
				}
			} else {
				return api.NewError(api.ErrStepDoneCannotBeChanged, "failed to update database stepRecord row, step is already done, but can be forced (however unrecommended since caches will not be updated)")
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
			completeBy = &BL.DAO.CompleteByPendingInterval
		} else if newStatus == api.StepInProgress {
			completeBy = step.GetCompleteBy(BL)
			retriesLeftVar = updatedStepRecord.RetriesLeft - 1
			retriesLeft = &retriesLeftVar
		} else if newStatus == api.StepIdle {
			retriesLeftVar = step.Retries + 1
			retriesLeft = &retriesLeftVar
		}

		if newState == nil {
			updatedStepRecord.StatusOwner = BL.DAO.UpdateStepPartsTx(tx, partialStepRecord.RunId, partialStepRecord.Index, newStatus, statusOwner, completeBy, retriesLeft, stepContext, nil).StatusOwner
		} else {
			var newStateBytes []byte
			newStateBytes, err = json.Marshal(newState)
			if err != nil {
				panic(err)
			}
			newStateStr := string(newStateBytes)
			updatedStepRecord.StatusOwner = BL.DAO.UpdateStepPartsTx(tx, partialStepRecord.RunId, partialStepRecord.Index, newStatus, statusOwner, completeBy, retriesLeft, stepContext, &newStateStr).StatusOwner
			updatedStepRecord.State = string(newStateBytes)
		}
		updatedStepRecord.Status = newStatus

		return nil
	})
	if tErr == nil && softError != nil {
		tErr = softError
	} else if tErr == nil && toEnqueue {
		work := doWork(updatedStepRecord.UUID)
		tErr = BL.Enqueue(&work)
	}
	return &updatedStepRecord, tErr
}

func (b *BL) failStepIfNoRetries(tx *sqlx.Tx, partialStepRecord *api.StepRecord, newStatusOwner string, context api.Context, newState *dao.StepState) (bool, *api.Error) {
	if partialStepRecord.RetriesLeft < 1 {
		if partialStepRecord.Status != api.StepFailed {
			var newStateBytes []byte
			newStateBytes, err := json.Marshal(newState)
			if err != nil {
				panic(err)
			}
			newStateStr := string(newStateBytes)
			_ = b.DAO.UpdateStepPartsTx(tx, partialStepRecord.RunId, partialStepRecord.Index, api.StepFailed, newStatusOwner, nil, nil, context, &newStateStr)
		}
		return true, api.NewError(api.ErrStepNoRetriesLeft, "failed to change step status")
	}
	return false, nil
}
func (s *Step) GetCompleteBy(BL *BL) *int64 {
	if s.stepDo.CompleteBy > 0 {
		return &s.stepDo.CompleteBy
	}
	return &BL.completeByInProgressInterval
}

func (s *Step) GetHeartBeatTimeout() time.Duration {
	if s.stepDo.HeartBeatTimeout > 0 {
		return time.Duration(s.stepDo.HeartBeatTimeout) * time.Second
	}
	return defaultHeartBeatInterval
}
func (t *Template) StartDo(BL *BL, runId string, stepUUID string, newStatusOwner string, context api.Context) (*api.StepRecord, error) {
	updatedPartialStepRecord, err := t.TransitionStateAndStatus(BL, runId, stepUUID, "", api.StepInProgress, newStatusOwner, context, nil, false)
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
	var newState dao.StepState
	var doErr error
	step := t.Steps[updatedPartialStepRecord.Index-1]
	newState, doErr = BL.do(step.doType, step.Do, &prevState)
	var newStepStatus api.StepStatusType
	if doErr != nil {
		newStepStatus = api.StepFailed
	} else {
		newStepStatus = api.StepDone
		if step.On.PreDone != nil {
		BREAKOUT:
			for _, rule := range step.On.PreDone.Rules {
				if rule.Then != nil {
					if len(rule.Then.Do) > 0 {
						var indicesAndContext []struct {
							index           int64
							context         string
							resolvedContext api.Context
						}
						for _, thenDo := range rule.Then.Do {
							index, ok := t.labelsToIndices[thenDo.Label]
							if !ok {
								panic(fmt.Errorf("label should have an index"))
							}
							indicesAndContext = append(indicesAndContext, struct {
								index           int64
								context         string
								resolvedContext api.Context
							}{index, thenDo.Context, nil})
						}
						if len(indicesAndContext) > 0 {
							var uuidsToEnqueue []dao.UUIDAndStatusOwner
							for i := range indicesAndContext {
								indicesAndContext[i].resolvedContext, err = t.ResolveContext(BL, indicesAndContext[i].context)
								if err != nil {
									break BREAKOUT
								}
							}
							if tErr := BL.DAO.Transactional(func(tx *sqlx.Tx) error {
								for _, indexAndContext := range indicesAndContext {
									uuids := BL.DAO.UpdateManyStepsPartsBeatTx(tx, runId, []int64{indexAndContext.index}, api.StepPending, "", []api.StepStatusType{api.StepIdle}, &BL.DAO.CompleteByPendingInterval, nil, indexAndContext.resolvedContext, nil)
									if len(uuids) == 1 {
										uuidsToEnqueue = append(uuidsToEnqueue, uuids[0])
									}
								}
								return nil
							}); tErr != nil {
								panic(tErr)
							}
							for _, item := range uuidsToEnqueue {
								work := doWork(item.UUID)
								if eErr := BL.Enqueue(&work); eErr != nil {
									log.Error(eErr)
								}
							}
						}
						break BREAKOUT
					}
				}
			}
		}
	}
	if err != nil {
		newStepStatus = api.StepFailed
		//newState may contain err even if doErr is not nil
		//goland:noinspection GoNilness
		if newState.Error == "" {
			newState.Error = err.Error()
		}
	}
	var tErr error
	updatedPartialStepRecord, tErr = t.TransitionStateAndStatus(BL, runId, stepUUID, updatedPartialStepRecord.StatusOwner, newStepStatus, newStatusOwner, nil, &newState, false)
	if tErr != nil {
		defer log.Error(tErr)
		err = fmt.Errorf("many errors, first: %s, next: failed to update step status: %w", tErr, err)
	}
	return updatedPartialStepRecord, err
}

func (b *BL) DoStep(params *api.DoStepParams, synchronous bool) (*api.DoStepResult, error) {
	if dao.IsRemote {
		return b.Client.RemoteDoStep(params) //always async
	} else {
		return b.doStep(params, synchronous)
	}
}

func (b *BL) doStep(params *api.DoStepParams, synchronous bool) (*api.DoStepResult, error) {
	if !synchronous {
		var owner dao.UUIDAndStatusOwner
		var toEnqueue = true
		tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
			updated := b.DAO.UpdateManyStatusAndHeartBeatByUUIDTx(tx, []string{params.UUID}, api.StepPending, params.StatusOwner, []api.StepStatusType{api.StepIdle}, params.Context, &b.DAO.CompleteByPendingInterval)
			if len(updated) != 1 {
				partialSteps, err := dao.GetStepsTx(tx, &api.GetStepsQuery{
					UUIDs:            []string{params.UUID},
					ReturnAttributes: []string{dao.HeartBeat, dao.StatusOwner, dao.Status, dao.State, dao.Index, dao.RetriesLeft},
				})
				if err != nil {
					return fmt.Errorf("failed to do step when investigating a failed enqueue: %w", err)
				}
				if len(partialSteps) != 1 {
					panic("only 1 record expected on search for uuid in do step failed enqueue investigation")
				}
				partialStepRecord := &partialSteps[0]
				if partialStepRecord.StatusOwner != params.StatusOwner || (partialStepRecord.Status != api.StepPending && partialStepRecord.Status != api.StepInProgress) {
					return api.NewError(api.ErrPrevStepStatusDoesNotMatch, "enqueue failed because it is highly probably the step with uuid:%s, is not in a pending state or the record is missing")
				}
				owner = dao.UUIDAndStatusOwner{
					UUID:        params.UUID,
					StatusOwner: params.StatusOwner,
				}
				toEnqueue = false
				return nil
			}
			owner = updated[0]
			return nil
		})
		var apiErr *api.Error
		if tErr != nil && errors.As(tErr, &apiErr) && apiErr.Code() == api.ErrPrevStepStatusDoesNotMatch {
			return nil, tErr
		} else if tErr != nil {
			panic(tErr)
		}
		if toEnqueue {
			err := b.Enqueue((*doWork)(&params.UUID))
			if err != nil {
				return nil, fmt.Errorf("failed to equeue step:%w", err)
			}
		}
		return &api.DoStepResult{StatusOwner: owner.StatusOwner}, nil
	}
	return b.doStepSynchronous(params)
}

func (b *BL) doStepSynchronous(params *api.DoStepParams) (*api.DoStepResult, error) {
	var run *api.RunRecord
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		runs, err := dao.GetRunsByStepUUIDsTx(tx, &api.GetRunsQuery{
			Ids:              []string{params.UUID},
			ReturnAttributes: []string{dao.Id, dao.Status, dao.Template},
		})
		if err != nil || len(runs) != 1 {
			if err != nil {
				return api.NewWrapError(api.ErrRecordNotFound, err, "failed to locate run by step uuid [%s]: %w", params.UUID, err)
			} else {
				return api.NewError(api.ErrRecordNotFound, "failed to locate run by step uuid [%s]", params.UUID)
			}

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
	err := template.LoadFromBytes(b, run.Id, false, []byte(run.Template))
	if err != nil {
		return nil, fmt.Errorf("failed to do step, failed to convert step record to step: %w", err)
	}
	template.RefreshInput(b, run.Id)
	var updatedStepRecord *api.StepRecord
	updatedStepRecord, err = template.StartDo(b, run.Id, params.UUID, params.StatusOwner, params.Context)
	if err != nil {
		return nil, fmt.Errorf("failed to start do: %w", err)
	}
	return &api.DoStepResult{
		StatusOwner: updatedStepRecord.StatusOwner,
	}, nil
}

func (b *BL) getStepsByQuery(query *api.GetStepsQuery) ([]api.StepRecord, error) {
	var stepRecords []api.StepRecord
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		var err error
		stepRecords, err = dao.GetStepsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to get steps by query: %w", err)
		}
		return nil
	})
	return stepRecords, tErr
}

func (b *BL) listStepsByQuery(query *api.ListQuery) ([]api.StepRecord, *api.RangeResult, error) {
	var stepRecords []api.StepRecord
	var rangeResult *api.RangeResult
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		var err error
		stepRecords, rangeResult, err = dao.ListStepsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to list steps by query: %w", err)
		}
		return nil
	})
	return stepRecords, rangeResult, tErr
}
