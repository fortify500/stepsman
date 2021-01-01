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
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/dao"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/mitchellh/mapstructure"
	"github.com/open-policy-agent/opa/rego"
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
	index           int
	context         string
	label           string
	resolvedContext api.Context
}
type onCookie struct {
	newRunStatus     *api.RunStatusType
	safetyDepth      int
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
	wasUpdate := false
	var changes map[string]interface{}
	var statusOwner string
	var options api.Options
	var force bool
	if queryByUUID != nil {
		options = queryByUUID.Options
		changes = queryByUUID.Changes
		statusOwner = queryByUUID.StatusOwner
		force = queryByUUID.Force
	} else if queryByLabel != nil {
		options = queryByLabel.Options
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
			var newState *api.State
			val, ok = changes["state"]
			if ok {
				var newStateTmp api.State
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
			var stepUUID uuid.UUID
			var run api.RunRecord
			var label string
			var currentContext api.Context
			newStatus, err := api.TranslateToStepStatus(statusStr)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			err = b.DAO.Transactional(func(tx *sqlx.Tx) error {
				var e error
				if queryByUUID != nil {
					run, currentContext, label, e = dao.GetRunLabelAndContextByStepUUIDTx(tx, options, queryByUUID.UUID,
						[]string{dao.Id, dao.Template})
					if e != nil {
						return fmt.Errorf("failed to update step, failed to locate run: %w", e)
					}
					stepUUID = queryByUUID.UUID
				} else if queryByLabel != nil {
					run, stepUUID, currentContext, err = dao.GetRunAndStepUUIDByLabelTx(tx, options, queryByLabel.RunId, queryByLabel.Label, []string{dao.Id, dao.Template})
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
			_, err = template.TransitionStateAndStatus(b, options, run.Id, label, stepUUID, nil, newStatus, "", currentContext, newState, force)
			if err != nil {
				return fmt.Errorf("failed to update step: %w", err)
			}
			wasUpdate = true
		} else {
			return api.NewError(api.ErrInvalidParams, "either do not specify changes to update the heartbeat, or you must specify changes with a 'status' and optionally 'state'")
		}
	}
	if !wasUpdate {
		err := b.DAO.UpdateStepHeartBeat(options, queryByUUID.UUID, statusOwner)
		if err != nil {
			return fmt.Errorf("failed to update step: %w", err)
		}
	}
	return nil
}

func (t *Template) TransitionStateAndStatus(BL *BL, options api.Options, runId uuid.UUID, label string, stepUUID uuid.UUID, prevStatus *api.StepStatusType, newStatus api.StepStatusType, newStatusOwner string, currentContext api.Context, newState *api.State, force bool) (*api.StepRecord, error) {
	var updatedStepRecord api.StepRecord
	var softError error
	var tErr error
	var run *api.RunRecord
	toEnqueue := false
	checkRetries := false
	var cookie onCookie
	var uncommitted *uncommittedResult
	if newStatus != api.StepDone && newStatus != api.StepFailed {
		newState = &api.State{}
	}
	if newStatus == api.StepDone {
		if newState == nil {
			panic(fmt.Errorf("newState must not be nil at this point"))
		}
		uncommitted = &uncommittedResult{
			label: label,
			state: *newState,
		}
	}
	cookie, newStatus, _, softError = t.on(nil, BL, options, OnPhasePreTransaction, cookie, runId, label, newStatus, currentContext, newState, uncommitted)
	if newStatus != api.StepDone {
		uncommitted = nil
	}
	if softError == nil {
		tErr = BL.DAO.Transactional(func(tx *sqlx.Tx) error {
			var err error
			startingStatus := newStatus
			partialSteps, err := dao.GetStepsTx(tx, &api.GetStepsQuery{
				UUIDs:            []uuid.UUID{stepUUID},
				ReturnAttributes: []string{dao.HeartBeat, dao.StatusOwner, dao.Status, dao.State, dao.Index, dao.RetriesLeft},
				Options:          options,
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
			prevStatusOwner := partialStepRecord.StatusOwner
			if prevStatus != nil && partialStepRecord.Status != *prevStatus {
				return api.NewError(api.ErrPrevStepStatusDoesNotMatch, "transitioning to status failed because it is highly probably the step with uuid:%s, is not in a pending state or the record is missing - assumed %s but actually %s", partialStepRecord.UUID, (*prevStatus).TranslateStepStatus(), partialStepRecord.Status.TranslateStepStatus())
			}
			// don't change done if the status did not change.
			if newStatus == partialStepRecord.Status {
				return api.NewError(api.ErrStatusNotChanged, "step status have not changed")
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
					checkRetries = true
				case api.StepFailed:
				case api.StepDone:
				}
			case api.StepPending:
				switch newStatus {
				case api.StepIdle:
				case api.StepInProgress:
					statusOwner = partialStepRecord.StatusOwner
					checkRetries = true
				case api.StepFailed:
					statusOwner = partialStepRecord.StatusOwner
					toEnqueue = true
					checkRetries = true
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
							return api.NewError(api.ErrStepAlreadyInProgress, "failed to update database stepRecord row, stepRecord is already in progress and has a heartbeat with an interval of %f seconds and %f seconds has passed", heartBeatInterval.Seconds(), delta.Seconds())
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
					toEnqueue = true
					checkRetries = true
				case api.StepDone:
				}
			case api.StepFailed:
				switch newStatus {
				case api.StepIdle:
				case api.StepPending:
					statusOwner = partialStepRecord.StatusOwner
					toEnqueue = true
					checkRetries = true
				case api.StepInProgress:
					statusOwner = partialStepRecord.StatusOwner
					checkRetries = true
				case api.StepDone:
				}
			case api.StepDone:
				if force {
					switch newStatus {
					case api.StepIdle:
					case api.StepPending:
						toEnqueue = true
						checkRetries = true
					case api.StepInProgress:
						checkRetries = true
					case api.StepFailed:
					}
				} else {
					return api.NewError(api.ErrStepDoneCannotBeChanged, "failed to update database stepRecord row, step is already done, but can be forced (however unrecommended since caches will not be updated)")
				}
			}

			if checkRetries && partialStepRecord.RetriesLeft < 1 {
				if partialStepRecord.Status != api.StepFailed {
					var onErr error
					if startingStatus != newStatus {
						cookie, newStatus, _, onErr = t.on(nil, BL, options, OnPhasePreTransaction, cookie, runId, label, newStatus, currentContext, newState, uncommitted)
						if onErr != nil {
							newState.Error = fmt.Sprintf("%s: %s", onErr.Error(), newState.Error)
						}
						if newStatus != api.StepDone {
							uncommitted = nil
						}
					}
					if onErr == nil {
						cookie, _, _, onErr = t.on(tx, BL, options, OnPhaseInTransaction, cookie, runId, label, newStatus, currentContext, newState, uncommitted)
						if onErr != nil {
							newState.Error = fmt.Sprintf("%s: %s", onErr.Error(), newState.Error)
						}
						if newStatus != api.StepDone {
							uncommitted = nil
						}
					}
					_ = BL.DAO.UpdateStepPartsTx(tx, options, partialStepRecord.RunId, partialStepRecord.Index, api.StepFailed, statusOwner, nil, nil, stepContext, newState)
				}
				softError = api.NewError(api.ErrStepNoRetriesLeft, "failed to change step status")
				return nil
			}
			run, err = GetRunByIdTx(tx, options, runId)
			if err != nil {
				return fmt.Errorf("failed to update database stepRecord row: %w", err)
			}
			if run.Status == api.RunIdle {
				BL.DAO.UpdateRunStatusTx(tx, options, run.Id, api.RunInProgress, nil, nil)
			} else if run.Status == api.RunDone {
				if newStatus == api.StepInProgress || newStatus == api.StepPending {
					if partialStepRecord.Status == api.StepIdle || partialStepRecord.Status == api.StepDone || partialStepRecord.Status == api.StepFailed {
						return api.NewError(api.ErrRunIsAlreadyDone, "failed to update database stepRecord status, run is already done and no change is possible")
					}
					newStatus = api.StepIdle // we want to finish this so it won't be revived.
					newState = &api.State{}
					uncommitted = nil
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
				updatedStepRecord.StatusOwner = BL.DAO.UpdateStepPartsTx(tx, options, partialStepRecord.RunId, partialStepRecord.Index, newStatus, statusOwner, completeBy, retriesLeft, stepContext, nil).StatusOwner
			} else {
				updatedStepRecord.StatusOwner = BL.DAO.UpdateStepPartsTx(tx, options, partialStepRecord.RunId, partialStepRecord.Index, newStatus, statusOwner, completeBy, retriesLeft, stepContext, newState).StatusOwner
				updatedStepRecord.State = *newState
			}
			updatedStepRecord.Status = newStatus
			if startingStatus != newStatus {
				// this part should only be triggered by the run condition from in-progress,pending to idle. so nothing should happen.
				var onErr error
				cookie, newStatus, _, onErr = t.on(nil, BL, options, OnPhasePreTransaction, cookie, runId, label, newStatus, currentContext, newState, uncommitted)
				if onErr != nil {
					panic(onErr) // no place for failure here, it should not occur.
				}
				if newStatus != api.StepDone {
					uncommitted = nil
				}
			}
			var onErr error
			cookie, newStatus, _, onErr = t.on(tx, BL, options, OnPhaseInTransaction, cookie, runId, label, newStatus, currentContext, newState, uncommitted)
			if onErr != nil {
				return fmt.Errorf("failed in transaction: %w", onErr)
			}
			if newStatus != api.StepDone {
				uncommitted = nil
			}
			return nil
		})
	}
	if tErr == nil && softError != nil {
		tErr = softError
	} else if tErr == nil && toEnqueue {
		work := doWork{
			Item:     updatedStepRecord.UUID,
			ItemType: StepWorkType,
			Options:  options,
		}
		tErr = BL.Enqueue(&work)
	}
	if tErr == nil {
		var onErr error
		_, _, _, onErr = t.on(nil, BL, options, OnPhasePostTransaction, cookie, runId, label, newStatus, currentContext, newState, uncommitted)
		if onErr != nil {
			//there is nothing we can do that haven't been tried, it will be recovered later.
			_ = api.ResolveErrorAndLog(onErr, true)
		}
	}
	if run != nil && run.CompleteBy != nil && time.Time(run.CreatedAt).
		Add(time.Duration(t.Expiration.CompleteBy)*time.Second).
		Before(time.Time(run.Now)) {
		recoverable(func() error {
			return BL.doRun(options, run.Id)
		})
	}
	return &updatedStepRecord, tErr
}

func (t *Template) on(tx *sqlx.Tx, BL *BL, options api.Options, phase int, cookie onCookie, runId uuid.UUID, label string, newStatus api.StepStatusType, currentContext api.Context, newState *api.State, uncommitted *uncommittedResult) (onCookie, api.StepStatusType, bool, error) {
	var err error
	var errPropagation = true
	switch phase {
	case OnPhasePreTransaction:
		safetyDepth := cookie.safetyDepth
		zeroIndex := t.labelsToIndices[label] - 1
		step := t.Steps[zeroIndex]
		cookie = onCookie{
			safetyDepth: safetyDepth,
		}
		var event Event
		switch newStatus {
		case api.StepPending:
			event = step.On.Pending
		case api.StepFailed:
			event = step.On.Failed
		case api.StepInProgress:
			event = step.On.InProgress
		case api.StepDone:
			event = step.On.Done
		}
		var decisionsInput = make(api.Input)
		if len(event.Rules) > 0 {
			decisionsInput, err = t.evaluateDecisions(BL, event.Decisions, step, currentContext, uncommitted, decisionsInput)
		}
		if err == nil {
			err, errPropagation, cookie = t.evaluateRules(BL, event.Rules, zeroIndex, currentContext, step, decisionsInput, uncommitted, errPropagation, cookie)
		}
	case OnPhaseInTransaction:
		if cookie.safetyDepth > BL.PendingRecursionDepthLimit { //minimum is 0 for no recursion.
			err = api.NewError(api.ErrTemplateEvaluationFailed, "seems the then do recursion is out of control and reached a depth of:%d while configured limit is: %d", cookie.safetyDepth, BL.PendingRecursionDepthLimit)
			break
		}
		cookie.safetyDepth++
		if cookie.newRunStatus != nil {
			switch *cookie.newRunStatus {
			case api.RunDone:
				prev := api.RunInProgress
				var completeBy int64 = -1
				BL.DAO.UpdateRunStatusTx(tx, options, runId, api.RunDone, &prev, &completeBy)
			default:
				panic(fmt.Errorf("unsupported cookie status type: %s", (*cookie.newRunStatus).TranslateRunStatus()))
			}
		}
		if len(cookie.resolvedContexts) > 0 {
		BREAKOUT:
			for _, indexContextAndLabel := range cookie.resolvedContexts {
				var innerCookie onCookie
				var emptyState api.State
				var propagate bool
				innerCookie.safetyDepth = cookie.safetyDepth
				innerCookie, newStatus, propagate, err = t.on(tx, BL, options, OnPhasePreTransaction, innerCookie, runId, indexContextAndLabel.label, api.StepPending, indexContextAndLabel.resolvedContext, &emptyState, uncommitted)
				if err != nil {
					if !propagate {
						_ = api.ResolveErrorAndLog(err, propagate)
						err = nil
						continue
					}
					break BREAKOUT
				}
				uuids := BL.DAO.UpdateManyStepsPartsBeatTx(tx, options, runId, []int{indexContextAndLabel.index}, api.StepPending, "", []api.StepStatusType{api.StepIdle}, &BL.DAO.CompleteByPendingInterval, nil, indexContextAndLabel.resolvedContext, nil)
				if len(uuids) == 1 {
					cookie.uuidsToEnqueue = append(cookie.uuidsToEnqueue, uuids[0])
					innerCookie, newStatus, _, err = t.on(tx, BL, options, OnPhaseInTransaction, innerCookie, runId, indexContextAndLabel.label, api.StepPending, indexContextAndLabel.resolvedContext, &emptyState, uncommitted)
					if err != nil {
						break BREAKOUT
					}
					cookie.uuidsToEnqueue = append(cookie.uuidsToEnqueue, innerCookie.uuidsToEnqueue...)
				}
			}
		}
	case OnPhasePostTransaction:
		if len(cookie.uuidsToEnqueue) > 0 {
			for _, item := range cookie.uuidsToEnqueue {
				work := doWork{
					Item:     item.UUID,
					ItemType: StepWorkType,
					Options:  options,
				}
				if eErr := BL.Enqueue(&work); eErr != nil {
					log.Error(eErr)
				}
			}
		}
	default:
		panic(fmt.Errorf("no such on phase, this code bit should never be reached"))
	}
	if err != nil {
		newStatus = api.StepFailed
		cookie = onCookie{}
		//goland:noinspection GoNilness
		if newState.Error == "" {
			newState.Error = err.Error()
		}
	}
	return cookie, newStatus, errPropagation, err
}

func (t *Template) evaluateDecisions(BL *BL, decisions []RulesDecision, step Step, currentContext api.Context, uncommitted *uncommittedResult, decisionsInput api.Input) (api.Input, error) {
	var err error
BREAKOUT1:
	for _, decision := range decisions {
		var resolvedInput api.Input
		decisionModule := regoModuleNameForDecision(decision.Label)
		resolvedInput, err = t.ResolveInput(BL, &step, decision.Input, currentContext, uncommitted)
		if err != nil {
			err = fmt.Errorf("failed to resolve input for decision %s: %w", decision.Label, err)
			break BREAKOUT1
		}
		ctx, cancel := context.WithTimeout(BL.ValveCtx, time.Duration(BL.maxRegoEvaluationTimeoutSeconds)*time.Second)
		var eval rego.ResultSet
		eval, err = t.rego.preparedModuleQueries[decisionModule].Eval(ctx, rego.EvalInput(resolvedInput))
		if err != nil {
			err = api.NewWrapErrorWithInput(api.ErrTemplateEvaluationFailed, err, resolvedInput, "failed to evaluate decision %s: %w", decisionModule, err)
			cancel()
			break BREAKOUT1
		}
		cancel()
		log.Tracef("decision evaluation - %s: %v\n", decisionModule, eval)
		if len(eval) > 0 &&
			len(eval[0].Expressions) > 0 &&
			eval[0].Expressions[0].Value != nil {
			value := eval[0].Expressions[0].Value
			switch value.(type) {
			case map[string]interface{}:
				var ok bool
				var result interface{}
				var errors interface{}
				results := value.(map[string]interface{})
				errors, ok = results["errors"]
				if ok {
					var errorsStr []byte
					errorsStr, err = json.Marshal(errors)
					if err != nil {
						err = api.NewWrapErrorWithInput(api.ErrTemplateEvaluationFailed, err, resolvedInput, "failed to evaluate decision %s: %w", decisionModule, err)
						break BREAKOUT1
					}
					if len(string(errorsStr)) > 0 && string(errorsStr) != "[]" && string(errorsStr) != "{}" {
						err = api.NewErrorWithInput(api.ErrTemplateEvaluationFailed, resolvedInput, "failed to evaluate decision %s, due to errors: %s", decisionModule, errorsStr)
						break BREAKOUT1
					}
				}
				result, ok = results["result"]
				if !ok {
					err = api.NewErrorWithInput(api.ErrTemplateEvaluationFailed, resolvedInput, "failed to evaluate decision %s, no result given", decisionModule)
					break BREAKOUT1
				}
				decisionsInput[decision.Label] = result
			default:
				panic(fmt.Sprintf("expected results are in a fixed map when evaluating decisions"))
			}
		} else {
			err = api.NewErrorWithInput(api.ErrTemplateEvaluationFailed, resolvedInput, "failed to evaluate decision: %s", decisionModule)
			break BREAKOUT1
		}
	}
	return decisionsInput, err
}

func (t *Template) evaluateRules(BL *BL, rules []Rule, stepIndex int, currentContext api.Context, step Step, decisionsInput api.Input, uncommitted *uncommittedResult, errPropagation bool, cookie onCookie) (error, bool, onCookie) {
	var err error
BREAKOUT2:
	for r, rule := range rules {
		if rule.Then == nil {
			continue
		}
		var input map[string]interface{}
		if rule.If != "" {
			ifModule := regoModuleNameForIf(stepIndex, r)
			var eval rego.ResultSet
			input = t.fillInput(currentContext, &step, decisionsInput, uncommitted)
			ctx, cancel := context.WithTimeout(BL.ValveCtx, time.Duration(BL.maxRegoEvaluationTimeoutSeconds)*time.Second)
			eval, err = t.rego.preparedModuleQueries[ifModule].Eval(ctx, rego.EvalInput(input))
			if err != nil {
				cancel()
				err = api.NewWrapErrorWithInput(api.ErrTemplateEvaluationFailed, err, input, "failed to evaluate rule %s: %w", rule.If, err)
				break BREAKOUT2
			}
			cancel()
			if len(eval) > 0 &&
				len(eval[0].Expressions) > 0 &&
				eval[0].Expressions[0].Value != nil {
				switch eval[0].Expressions[0].Value.(type) {
				case bool:
					if !eval[0].Expressions[0].Value.(bool) {
						continue
					}
				default:
					err = api.NewErrorWithInput(api.ErrTemplateEvaluationFailed, input, "failed to evaluate rule %s, result must be a boolean, given: %v", rule.If, eval[0].Expressions[0].Value)
					break BREAKOUT2
				}
			} else {
				err = api.NewErrorWithInput(api.ErrTemplateEvaluationFailed, input, "failed to evaluate rule %s, no result given", rule.If)
				break BREAKOUT2
			}
		}
		if rule.Then.Error != nil {
			if rule.Then.Error.Propagate != nil {
				errPropagation = *rule.Then.Error.Propagate
			}
			var resolvedMessage string
			var resolvedData string
			resolvedMessage, err = t.EvaluateCurlyPercent(BL, &step, rule.Then.Error.Message, currentContext, decisionsInput, uncommitted)
			if err != nil {
				err = fmt.Errorf("failed to evaluate rule->then->error->message %s->%s: %w", rule.If, rule.Then.Error.Message, err)
				break BREAKOUT2
			}
			if rule.Then.Error.Data != "" {
				resolvedData, err = t.EvaluateCurlyPercent(BL, &step, rule.Then.Error.Data, currentContext, decisionsInput, uncommitted)
				if err != nil {
					err = fmt.Errorf("failed to evaluate rule->then->error->data %s->%s: %w", rule.If, rule.Then.Error.Data, err)
					break BREAKOUT2
				}
				var data interface{}
				err = json.Unmarshal([]byte(resolvedData), &data)
				if err != nil {
					err = api.NewWrapError(api.ErrCustomTemplateErrorThrown, err, "failed to evaluate rule->then->error->data %s->%s: %w", rule.If, rule.Then.Error.Data, err)
					break BREAKOUT2
				}
				err = api.NewErrorWithData(api.ErrCustomTemplateErrorThrown, data, resolvedMessage)
			} else {
				err = api.NewError(api.ErrCustomTemplateErrorThrown, resolvedMessage)
			}
			break BREAKOUT2
		}
		if rule.Then.SetRunStatus != nil {
			switch *rule.Then.SetRunStatus {
			case api.RunDone:
				s := api.RunDone
				cookie.newRunStatus = &s
			default:
				panic(fmt.Errorf("unsupported type: %s", rule.Then.SetRunStatus.TranslateRunStatus()))
			}
		}
		if len(rule.Then.Do) > 0 {
			for _, thenDo := range rule.Then.Do {
				index, ok := t.labelsToIndices[thenDo.Label]
				if !ok {
					panic(fmt.Errorf("label should have an index"))
				}
				cookie.resolvedContexts = append(cookie.resolvedContexts, onCookieContext{
					index:           index,
					context:         thenDo.Context,
					label:           thenDo.Label,
					resolvedContext: nil,
				})
			}
			if len(cookie.resolvedContexts) > 0 {
				for i := range cookie.resolvedContexts {
					cookie.resolvedContexts[i].resolvedContext, err = t.ResolveContext(BL, &step, cookie.resolvedContexts[i].context, currentContext, decisionsInput, uncommitted)
					if err != nil {
						break BREAKOUT2
					}
				}
			}
			break BREAKOUT2
		}
	}
	return err, errPropagation, cookie
}

func (b *BL) failStepIfNoRetries(tx *sqlx.Tx, options api.Options, partialStepRecord *api.StepRecord, newStatusOwner string, context api.Context, newState *api.State) (bool, *api.Error) {
	if partialStepRecord.RetriesLeft < 1 {
		if partialStepRecord.Status != api.StepFailed {
			_ = b.DAO.UpdateStepPartsTx(tx, options, partialStepRecord.RunId, partialStepRecord.Index, api.StepFailed, newStatusOwner, nil, nil, context, newState)
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
func (t *Template) StartDo(BL *BL, options api.Options, runId uuid.UUID, label string, stepUUID uuid.UUID, newStatusOwner string, currentContext api.Context) (*api.StepRecord, error) {
	updatedPartialStepRecord, err := t.TransitionStateAndStatus(BL, options, runId, label, stepUUID, nil, api.StepInProgress, newStatusOwner, currentContext, nil, false)
	if err != nil {
		return nil, fmt.Errorf("failed to start do: %w", err)
	}
	var prevState = updatedPartialStepRecord.State
	var newState api.State
	var doErr error
	var async bool
	step := t.Steps[updatedPartialStepRecord.Index-1]
	newState, async, doErr = BL.do(t, &step, updatedPartialStepRecord, step.doType, step.Do, &prevState, currentContext)
	var newStepStatus api.StepStatusType
	if doErr != nil {
		newStepStatus = api.StepFailed
		_ = api.ResolveErrorAndLog(fmt.Errorf("failed to perform do: %w", doErr), true)
	} else {
		newStepStatus = api.StepDone
		if async {
			return updatedPartialStepRecord, nil
		}
	}
	updatedPartialStepRecord, err = t.TransitionStateAndStatus(BL, options, runId, label, stepUUID, &updatedPartialStepRecord.Status, newStepStatus, newStatusOwner, currentContext, &newState, false)
	if err != nil {
		return nil, err
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
func (b *BL) doRun(options api.Options, runId uuid.UUID) error {
	run, err := b.GetRun(options, runId)
	if err != nil {
		return fmt.Errorf("failed to do run - failed to get run: %w", err)
	}
	if run.CompleteBy == nil {
		return nil
	}
	template := Template{}
	err = template.LoadFromBytes(b, run.Id, false, []byte(run.Template))
	if err != nil {
		return fmt.Errorf("failed to do run - failed to load template: %w", err)
	}
	template.RefreshInput(b, options, run.Id)
	if time.Time(run.CreatedAt).
		Add(time.Duration(template.Expiration.CompleteBy) * time.Second).
		Before(time.Time(run.Now)) {
		var cookie onCookie
		recoverable(func() error {
			var decisionsInput = make(api.Input)
			decisionsInput, err = template.evaluateDecisions(b, template.Expiration.Decisions, Step{}, api.Context{}, nil, decisionsInput)
			if err == nil {
				err, _, cookie = template.evaluateRules(b, template.Expiration.Rules, ExpirationStepIndex, api.Context{}, Step{}, decisionsInput, nil, true, cookie)
			}
			return err
		})
		tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
			dao.ResetRunCompleteByTx(tx, runId)
			cookie, _, _, err = template.on(tx, b, options, OnPhaseInTransaction, cookie, runId, "", api.StepPending, api.Context{}, nil, nil)
			return err
		})
		if tErr != nil {
			return fmt.Errorf("failed to do run - failed to finish expiration: %w", err)
		}
		recoverable(func() error {
			_, _, _, err = template.on(nil, b, options, OnPhasePostTransaction, cookie, runId, "", api.StepPending, api.Context{}, nil, nil)
			return err
		})
	}
	return nil
}
func (b *BL) doStep(byLabelParams *api.DoStepByLabelParams, byUUIDParams *api.DoStepByUUIDParams, synchronous bool) (api.DoStepByUUIDResult, api.DoStepByLabelResult, error) {
	var run api.RunRecord
	var stepUUID uuid.UUID
	var statusOwner string
	var options api.Options
	var currentContext api.Context
	var label string
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		if byUUIDParams != nil {
			var err error
			options = byUUIDParams.Options
			run, currentContext, label, err = dao.GetRunLabelAndContextByStepUUIDTx(tx, options, byUUIDParams.UUID,
				[]string{dao.Id, dao.Status, dao.Template})
			if err != nil {
				return fmt.Errorf("failed to locate run by step uuid [%s]: %w", byUUIDParams.UUID, err)
			}
			stepUUID = byUUIDParams.UUID
			statusOwner = byUUIDParams.StatusOwner
			if byUUIDParams.Context != nil {
				currentContext = byUUIDParams.Context
			}
		} else if byLabelParams != nil {
			var err error
			options = byLabelParams.Options
			run, stepUUID, currentContext, err = dao.GetRunAndStepUUIDByLabelTx(tx, options, byLabelParams.RunId, byLabelParams.Label, []string{dao.Id, dao.Status, dao.Template})
			if err != nil {
				return fmt.Errorf("failed to locate run by step run-id and label [%s:%s]: %w", byLabelParams.RunId, byLabelParams.Label, err)
			}
			if byLabelParams.Context != nil {
				currentContext = byLabelParams.Context
			}
			label = byLabelParams.Label
			statusOwner = byLabelParams.StatusOwner
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
	template.RefreshInput(b, options, run.Id)
	var updatedPartialStepRecord *api.StepRecord
	if synchronous {
		updatedPartialStepRecord, err = template.StartDo(b, options, run.Id, label, stepUUID, statusOwner, currentContext)
	} else {
		var prevStatus = api.StepIdle
		updatedPartialStepRecord, err = template.TransitionStateAndStatus(b, options, run.Id, label, stepUUID, &prevStatus, api.StepPending, statusOwner, currentContext, nil, false)
	}
	if err != nil {
		return api.DoStepByUUIDResult{}, api.DoStepByLabelResult{}, fmt.Errorf("failed to start do: %w", err)
	}
	return api.DoStepByUUIDResult{
			StatusOwner: updatedPartialStepRecord.StatusOwner,
		}, api.DoStepByLabelResult{
			UUID:        stepUUID,
			StatusOwner: updatedPartialStepRecord.StatusOwner,
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
