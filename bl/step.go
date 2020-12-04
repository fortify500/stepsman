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
	"github.com/mitchellh/mapstructure"
	log "github.com/sirupsen/logrus"
	"strings"
	"time"
)

const defaultHeartBeatInterval = 10 * time.Second
const (
	OnPhasePreTransaction = iota
	OnPhaseInTransaction
	OnPhasePostTransaction
)

type onCookieContext struct {
	index           int64
	context         string
	resolvedContext api.Context
}
type onCookie struct {
	resolvedContexts []onCookieContext
	uuidsToEnqueue   []dao.UUIDAndStatusOwner
}

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

func (b *BL) UpdateStepByUUID(query *api.UpdateQueryByUUID) error {
	if dao.IsRemote {
		return b.Client.RemoteUpdateStepByUUID(query)
	} else {
		return b.updateStep(query, nil)
	}
}

func (b *BL) UpdateStepByLabel(query *api.UpdateQueryByLabel) error {
	if dao.IsRemote {
		return b.Client.RemoteUpdateStepByLabel(query)
	} else {
		return b.updateStep(nil, query)
	}
}

func (b *BL) updateStep(queryByUUID *api.UpdateQueryByUUID, queryByLabel *api.UpdateQueryByLabel) error {
	vetErr := dao.VetIds([]string{queryByUUID.UUID})
	if vetErr != nil {
		return fmt.Errorf("failed to update step: %w", vetErr)
	}
	wasUpdate := false
	var changes map[string]interface{}
	var statusOwner string
	var force bool
	if queryByUUID != nil {
		changes = queryByUUID.Changes
		statusOwner = queryByUUID.StatusOwner
		force = queryByUUID.Force
	} else if queryByLabel != nil {
		changes = queryByLabel.Changes
		statusOwner = queryByLabel.StatusOwner
		force = queryByLabel.Force
	} else {
		panic(fmt.Errorf("update step code bit should never be reached"))
	}
	if len(changes) > 0 {
		for k := range changes {
			switch k {
			case "status":
			case "state":
			default:
				return api.NewError(api.ErrInvalidParams, fmt.Sprintf("unsupported change type: %s, either do not specify changes to update the heartbeat, or you must specify changes with a 'status' and optionally 'state'", k))
			}
		}
		val, ok := changes["status"]
		if ok {
			var statusStr string
			statusStr, ok = val.(string)
			if !ok {
				return api.NewError(api.ErrInvalidParams, "status must be of string type")
			}
			var newState *dao.StepState
			val, ok = changes["state"]
			if ok {
				var newStateTmp dao.StepState
				var md mapstructure.Metadata
				decoder, err := mapstructure.NewDecoder(
					&mapstructure.DecoderConfig{
						Metadata: &md,
						Result:   &newStateTmp,
					})
				if err != nil {
					return api.NewWrapError(api.ErrInvalidParams, err, "failed to update step and parse state: %w", err)
				}
				err = decoder.Decode(changes["state"])
				if err != nil {
					return api.NewWrapError(api.ErrInvalidParams, err, "failed to update step and decode state: %w", err)
				}
				if len(md.Unused) > 0 {
					return api.NewError(api.ErrInvalidParams, "unsupported attributes provided in update state: %s", strings.Join(md.Unused, ","))
				}
				newState = &newStateTmp
			}
			var stepUUID string
			var run api.RunRecord
			var label string
			var context api.Context
			newStatus, err := api.TranslateToStepStatus(statusStr)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			err = b.DAO.Transactional(func(tx *sqlx.Tx) error {
				var e error
				if queryByUUID != nil {
					run, context, label, e = dao.GetRunLabelAndContextByStepUUIDTx(tx, queryByUUID.UUID,
						[]string{dao.Id, dao.Template})
					if e != nil {
						return fmt.Errorf("failed to update step, failed to locate run: %w", e)
					}
					stepUUID = queryByUUID.UUID
				} else if queryByLabel != nil {
					run, stepUUID, context, err = dao.GetRunAndStepUUIDByLabelTx(tx, queryByLabel.RunId, queryByLabel.Label, []string{dao.Id, dao.Template})
					if err != nil {
						return fmt.Errorf("failed to update step by step run-id and label [%s:%s]: %w", queryByLabel.RunId, queryByLabel.Label, err)
					}
					label = queryByLabel.Label
				} else {
					panic(fmt.Errorf("update step code bit should never be reached"))
				}
				return e
			})
			if err != nil {
				return err
			}
			template := Template{}
			err = template.LoadFromBytes(b, run.Id, false, []byte(run.Template))
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			_, err = template.TransitionStateAndStatus(b, run.Id, label, stepUUID, statusOwner, newStatus, "", context, newState, force)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			wasUpdate = true
		} else {
			return api.NewError(api.ErrInvalidParams, "either do not specify changes to update the heartbeat, or you must specify changes with a 'status' and optionally 'state'")
		}
	}
	if !wasUpdate {
		err := b.DAO.UpdateStepHeartBeat(queryByUUID.UUID, statusOwner)
		if err != nil {
			return fmt.Errorf("failed to update step: %w", err)
		}
	}
	return nil
}

func (t *Template) TransitionStateAndStatus(BL *BL, runId string, label string, stepUUID string, prevStatusOwner string, newStatus api.StepStatusType, newStatusOwner string, currentContext api.Context, newState *dao.StepState, force bool) (*api.StepRecord, error) {
	var updatedStepRecord api.StepRecord
	var softError *api.Error
	toEnqueue := false
	var cookie onCookie
	cookie, newStatus = t.on(nil, BL, OnPhasePreTransaction, cookie, runId, label, newStatus, currentContext, newState)
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
				stepContext = currentContext
			case api.StepInProgress:
				stepContext = currentContext
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
		cookie, newStatus = t.on(tx, BL, OnPhaseInTransaction, cookie, runId, label, newStatus, currentContext, newState)
		return nil
	})
	if tErr == nil && softError != nil {
		tErr = softError
	} else if tErr == nil && toEnqueue {
		work := doWork(updatedStepRecord.UUID)
		tErr = BL.Enqueue(&work)
	}
	if tErr == nil {
		_, _ = t.on(nil, BL, OnPhasePostTransaction, cookie, runId, label, newStatus, currentContext, newState)
	}
	return &updatedStepRecord, tErr
}

func (t *Template) on(tx *sqlx.Tx, BL *BL, phase int, onCookie onCookie, runId string, label string, newStatus api.StepStatusType, currentContext api.Context, newState *dao.StepState) (onCookie, api.StepStatusType) {
	switch phase {
	case OnPhasePreTransaction:
		switch newStatus {
		case api.StepInProgress:
		case api.StepDone:
			var err error
			step := t.Steps[t.labelsToIndices[label]-1]
			if step.On.Done != nil {
			BREAKOUT:
				for _, rule := range step.On.Done.Rules {
					if rule.Then != nil {
						if len(rule.Then.Do) > 0 {

							for _, thenDo := range rule.Then.Do {
								index, ok := t.labelsToIndices[thenDo.Label]
								if !ok {
									panic(fmt.Errorf("label should have an index"))
								}
								onCookie.resolvedContexts = append(onCookie.resolvedContexts, onCookieContext{
									index,
									thenDo.Context,
									nil,
								})
							}
							if len(onCookie.resolvedContexts) > 0 {
								for i := range onCookie.resolvedContexts {
									onCookie.resolvedContexts[i].resolvedContext, err = t.ResolveContext(BL, onCookie.resolvedContexts[i].context, currentContext)
									if err != nil {
										break BREAKOUT
									}
								}
							}
							break BREAKOUT
						}
					}
				}
				if err != nil {
					newStatus = api.StepFailed
					//newState may contain err even if doErr is not nil
					//goland:noinspection GoNilness
					if newState.Error == "" {
						newState.Error = err.Error()
					}
				}
			}
		}
	case OnPhaseInTransaction:
		if len(onCookie.resolvedContexts) > 0 {
			for _, indexAndContext := range onCookie.resolvedContexts {
				uuids := BL.DAO.UpdateManyStepsPartsBeatTx(tx, runId, []int64{indexAndContext.index}, api.StepPending, "", []api.StepStatusType{api.StepIdle}, &BL.DAO.CompleteByPendingInterval, nil, indexAndContext.resolvedContext, nil)
				if len(uuids) == 1 {
					onCookie.uuidsToEnqueue = append(onCookie.uuidsToEnqueue, uuids[0])
				}
			}
		}
	case OnPhasePostTransaction:
		if len(onCookie.uuidsToEnqueue) > 0 {
			for _, item := range onCookie.uuidsToEnqueue {
				work := doWork(item.UUID)
				if eErr := BL.Enqueue(&work); eErr != nil {
					log.Error(eErr)
				}
			}
		}
	default:
		panic(fmt.Errorf("no such on phase, this code bit should never be reached"))
	}
	return onCookie, newStatus
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
func (t *Template) StartDo(BL *BL, runId string, label string, stepUUID string, newStatusOwner string, currentContext api.Context) (*api.StepRecord, error) {
	updatedPartialStepRecord, err := t.TransitionStateAndStatus(BL, runId, label, stepUUID, "", api.StepInProgress, newStatusOwner, currentContext, nil, false)
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
	newState, doErr = BL.do(t, &step, updatedPartialStepRecord, step.doType, step.Do, &prevState, currentContext)
	var newStepStatus api.StepStatusType
	if doErr != nil {
		newStepStatus = api.StepFailed
	} else {
		newStepStatus = api.StepDone
	}
	var tErr error
	updatedPartialStepRecord, tErr = t.TransitionStateAndStatus(BL, runId, label, stepUUID, updatedPartialStepRecord.StatusOwner, newStepStatus, newStatusOwner, currentContext, &newState, false)
	if tErr != nil {
		defer log.Error(tErr)
		err = fmt.Errorf("many errors, first: %s, next: failed to update step status: %w", tErr, err)
	}
	return updatedPartialStepRecord, err
}

func (b *BL) DoStepByUUID(params *api.DoStepByUUIDParams, synchronous bool) (api.DoStepByUUIDResult, error) {
	if dao.IsRemote {
		return b.Client.RemoteDoStepByUUID(params) //always async
	} else {
		result, _, err := b.doStep(nil, params, synchronous)
		return result, err
	}
}

func (b *BL) DoStepByLabel(params *api.DoStepByLabelParams, synchronous bool) (api.DoStepByLabelResult, error) {
	if dao.IsRemote {
		return b.Client.RemoteDoStepByLabel(params)
	} else {
		_, result, err := b.doStep(params, nil, synchronous)
		return result, err
	}
}

func (b *BL) doStep(byLabelParams *api.DoStepByLabelParams, byUUIDParams *api.DoStepByUUIDParams, synchronous bool) (api.DoStepByUUIDResult, api.DoStepByLabelResult, error) {
	if !synchronous {
		var owner dao.UUIDAndStatusOwner
		var toEnqueue = true

		tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
			var updated []dao.UUIDAndStatusOwner
			if byUUIDParams != nil {
				updated = b.DAO.UpdateManyStatusAndHeartBeatByUUIDTx(tx,
					[]string{byUUIDParams.UUID},
					api.StepPending,
					byUUIDParams.StatusOwner,
					[]api.StepStatusType{api.StepIdle},
					byUUIDParams.Context,
					&b.DAO.CompleteByPendingInterval)
			} else if byLabelParams != nil {
				updated = b.DAO.UpdateManyStatusAndHeartBeatByLabelTx(tx,
					byLabelParams.RunId,
					[]string{byLabelParams.Label},
					api.StepPending,
					byLabelParams.StatusOwner,
					[]api.StepStatusType{api.StepIdle},
					byLabelParams.Context,
					&b.DAO.CompleteByPendingInterval)
			} else {
				panic(fmt.Errorf("label or uuid params cannot both be null"))
			}

			if len(updated) != 1 {
				var err error
				var partialSteps []api.StepRecord
				var paramOwner dao.UUIDAndStatusOwner
				var partialStepRecord api.StepRecord
				if byUUIDParams != nil {
					paramOwner = dao.UUIDAndStatusOwner{
						UUID:        byUUIDParams.UUID,
						StatusOwner: byUUIDParams.StatusOwner,
					}
					partialSteps, err = dao.GetStepsTx(tx, &api.GetStepsQuery{
						UUIDs:            []string{byUUIDParams.UUID},
						ReturnAttributes: []string{dao.HeartBeat, dao.StatusOwner, dao.Status, dao.State, dao.Index, dao.RetriesLeft},
					})
					if err != nil {
						return fmt.Errorf("failed to do step when investigating a failed enqueue: %w", err)
					}
					if len(partialSteps) != 1 {
						panic("only 1 record expected on search for uuid in do step failed enqueue investigation")
					}
					partialStepRecord = partialSteps[0]
				} else if byLabelParams != nil {
					paramOwner = dao.UUIDAndStatusOwner{
						StatusOwner: byLabelParams.StatusOwner,
					}
					partialStepRecord, err = dao.GetStepByLabelTx(tx,
						byLabelParams.RunId,
						byLabelParams.Label,
						[]string{dao.HeartBeat, dao.StatusOwner, dao.Status, dao.State, dao.Index, dao.RetriesLeft, dao.UUID})
					if err != nil {
						return err
					}
					paramOwner.UUID = partialStepRecord.UUID
				}
				if partialStepRecord.StatusOwner != paramOwner.StatusOwner || (partialStepRecord.Status != api.StepPending && partialStepRecord.Status != api.StepInProgress) {
					return api.NewError(api.ErrPrevStepStatusDoesNotMatch, "enqueue failed because it is highly probably the step with uuid:%s, is not in a pending state or the record is missing")
				}
				owner = dao.UUIDAndStatusOwner{
					UUID:        paramOwner.UUID,
					StatusOwner: paramOwner.StatusOwner,
				}
				toEnqueue = false
				return nil
			}
			owner = updated[0]
			return nil
		})
		var apiErr *api.Error
		if tErr != nil && errors.As(tErr, &apiErr) && apiErr.Code() == api.ErrPrevStepStatusDoesNotMatch {
			return api.DoStepByUUIDResult{}, api.DoStepByLabelResult{}, tErr
		} else if tErr != nil {
			panic(tErr)
		}
		if toEnqueue {
			err := b.Enqueue((*doWork)(&owner.UUID))
			if err != nil {
				return api.DoStepByUUIDResult{}, api.DoStepByLabelResult{}, fmt.Errorf("failed to equeue step:%w", err)
			}
		}
		return api.DoStepByUUIDResult{StatusOwner: owner.StatusOwner}, api.DoStepByLabelResult{StatusOwner: owner.StatusOwner, UUID: owner.UUID}, nil
	}
	return b.doStepSynchronous(byLabelParams, byUUIDParams)
}

func (b *BL) doStepSynchronous(byLabelParams *api.DoStepByLabelParams, byUUIDParams *api.DoStepByUUIDParams) (api.DoStepByUUIDResult, api.DoStepByLabelResult, error) {
	var run api.RunRecord
	var stepUUID string
	var statusOwner string
	var context api.Context
	var label string
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		if byUUIDParams != nil {
			var err error
			run, context, label, err = dao.GetRunLabelAndContextByStepUUIDTx(tx, byUUIDParams.UUID,
				[]string{dao.Id, dao.Status, dao.Template})
			if err != nil {
				return fmt.Errorf("failed to locate run by step uuid [%s]: %w", byUUIDParams.UUID, err)
			}
			stepUUID = byUUIDParams.UUID
			statusOwner = byUUIDParams.StatusOwner
			if byUUIDParams.Context != nil {
				context = byUUIDParams.Context
			}
		} else if byLabelParams != nil {
			var err error
			run, stepUUID, context, err = dao.GetRunAndStepUUIDByLabelTx(tx, byLabelParams.RunId, byLabelParams.Label, []string{dao.Id, dao.Status, dao.Template})
			if err != nil {
				return fmt.Errorf("failed to locate run by step run-id and label [%s:%s]: %w", byLabelParams.RunId, byLabelParams.Label, err)
			}
			if byLabelParams.Context != nil {
				context = byLabelParams.Context
			}
			label = byLabelParams.Label
			statusOwner = byLabelParams.StatusOwner
			//context always comes from either the caller or internally with uuid so this will not be reached.
			if byLabelParams.Context == nil {
				panic(fmt.Errorf("context cannot be nil in this code position"))
			}
			context = byLabelParams.Context
		} else {
			panic(fmt.Errorf("one of label or uuid params must not be nil"))
		}
		return nil
	})
	if tErr != nil {
		return api.DoStepByUUIDResult{}, api.DoStepByLabelResult{}, fmt.Errorf("failed to do step synchronously: %w", tErr)
	}
	if run.Status == api.RunDone {
		return api.DoStepByUUIDResult{}, api.DoStepByLabelResult{}, api.NewError(api.ErrRunIsAlreadyDone, "failed to do step, run is already done and no change is possible")
	}

	template := Template{}
	err := template.LoadFromBytes(b, run.Id, false, []byte(run.Template))
	if err != nil {
		return api.DoStepByUUIDResult{}, api.DoStepByLabelResult{}, fmt.Errorf("failed to do step, failed to convert step record to step: %w", err)
	}
	template.RefreshInput(b, run.Id)
	var updatedStepRecord *api.StepRecord
	updatedStepRecord, err = template.StartDo(b, run.Id, label, stepUUID, statusOwner, context)
	if err != nil {
		return api.DoStepByUUIDResult{}, api.DoStepByLabelResult{}, fmt.Errorf("failed to start do: %w", err)
	}
	return api.DoStepByUUIDResult{
			StatusOwner: updatedStepRecord.StatusOwner,
		}, api.DoStepByLabelResult{
			UUID:        stepUUID,
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
