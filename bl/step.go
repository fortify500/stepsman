/*
Copyright © 2020 stepsman authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bl

import (
	"fmt"
	"github.com/fortify500/stepsman/dao"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const HeartBeatInterval = 10

const (
	StepDone dao.StepStatusType = 5
)

func TranslateStepStatus(status dao.StepStatusType) (string, error) {
	switch status {
	case dao.StepNotStarted:
		return "Not Started", nil
	case dao.StepInProgress:
		return "In Progress", nil
	case dao.StepCanceled:
		return "Canceled", nil
	case dao.StepFailed:
		return "Failed", nil
	case StepDone:
		return "Done", nil
	case dao.StepSkipped:
		return "Skipped", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}
func ListSteps(runId int64) ([]*dao.StepRecord, error) {
	return dao.ListSteps(runId)
}

func ToStep(stepRecord *dao.StepRecord) (*Step, error) {
	step := Step{
		Script: stepRecord.Script,
	}
	err := step.AdjustUnmarshalStep(true)
	if err != nil {
		return nil, err
	}
	step.stepRecord = stepRecord
	return &step, nil
}

func UpdateStepStatus(stepRecord *dao.StepRecord, newStatus dao.StepStatusType, doFinish bool) error {
	tx, err := dao.DB.SQL().Beginx()
	if err != nil {
		return fmt.Errorf("failed to start a database transaction: %w", err)
	}
	runId := stepRecord.RunId
	step, err := dao.GetStepTx(tx, runId, stepRecord.StepId)
	if err != nil {
		err = dao.Rollback(tx, err)
		return fmt.Errorf("failed to update database step row: %w", err)
	}

	if !doFinish && step.Status == dao.StepInProgress {
		delta := time.Now().Sub(time.Unix(step.HeartBeat, 0))
		if delta < 0 {
			delta = delta * -1
		}
		// +5 for slow downs
		if delta <= (HeartBeatInterval+5)*time.Second {
			err = dao.Rollback(tx, fmt.Errorf("step is already in progress and has a heartbeat with an interval of %d", HeartBeatInterval))
			return fmt.Errorf("failed to update database step row: %w", err)
		}
	}
	// don't change done if the status did not change.
	// must be after (because we want to raise StepInProgress=StepInProgress error
	if newStatus == stepRecord.Status {
		err = tx.Rollback()
		return err
	}
	heartBeat := step.HeartBeat
	if newStatus == dao.StepInProgress {
		if doFinish {
			heartBeat = 0
		} else {
			heartBeat = time.Now().Unix()
		}
	}
	_, err = stepRecord.UpdateStatusAndHeartBeatTx(tx, newStatus, heartBeat)
	if err != nil {
		err = dao.Rollback(tx, err)
		return fmt.Errorf("failed to update database step row: %w", err)
	}

	if newStatus != dao.StepNotStarted {
		var run *dao.RunRecord
		run, err = dao.GetRun(runId)
		if err != nil {
			err = dao.Rollback(tx, err)
			return fmt.Errorf("failed to update database step row: %w", err)
		}
		if run.Status != dao.RunInProgress {
			_, err = dao.UpdateRunStatus(run.Id, dao.RunInProgress)
			if err != nil {
				err = dao.Rollback(tx, err)
				return fmt.Errorf("failed to update database run row: %w", err)
			}
		}
	}

	if newStatus == StepDone || newStatus == dao.StepSkipped {
		var steps []*dao.StepRecord

		//TODO: need to replace with more efficient method.
		steps, err = dao.ListStepsTx(tx, runId)
		if err != nil {
			err = dao.Rollback(tx, err)
			return fmt.Errorf("failed to list database run rows: %w", err)
		}
		allDoneOrSkipped := true
		nextCursorPosition := -1
		for i, stepRecord := range steps {
			if stepRecord.Status != StepDone && stepRecord.Status != dao.StepSkipped {
				allDoneOrSkipped = false
				if nextCursorPosition > 0 {
					break
				}
			}
			if stepRecord.StepId == stepRecord.StepId {
				if i < len(steps) {
					nextCursorPosition = i + 1
				}
				if !allDoneOrSkipped {
					break
				}
			}
		}
		if allDoneOrSkipped {
			_, err = dao.UpdateRunStatus(runId, dao.RunDone)
			if err != nil {
				err = dao.Rollback(tx, err)
				return fmt.Errorf("failed to update database run row status: %w", err)
			}
		} else if nextCursorPosition > 0 {
			_, err = dao.UpdateRunCursorTx(tx, steps[nextCursorPosition].StepId, runId)
			if err != nil {
				err = dao.Rollback(tx, err)
				return fmt.Errorf("failed to update database run row cursor: %w", err)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
	stepRecord.Status = newStatus
	return nil
}

func (s *Step) StartDo() (dao.StepStatusType, error) {
	err := UpdateStepStatus(s.stepRecord, dao.StepInProgress, false)
	if err != nil {
		return dao.StepFailed, err
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
			case <-time.After(HeartBeatInterval * time.Second):
				errBeat := s.stepRecord.UpdateHeartBeat()
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
	newStatus, err := do(s.DoType, s.Do)
	if err != nil {
		return dao.StepFailed, err
	}
	close(heartBeatDone2)
	wg.Wait()
	err = UpdateStepStatus(s.stepRecord, newStatus, true)
	return newStatus, err
}
