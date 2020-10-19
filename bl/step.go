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
	"github.com/fortify500/stepsman/dao"
	log "github.com/sirupsen/logrus"
	"sync"
	"time"
)

const HeartBeatInterval = 10

func MustTranslateStepStatus(status dao.StepStatusType) string {
	stepStatus, err := TranslateStepStatus(status)
	if err != nil {
		log.Error(err)
	}
	return stepStatus
}
func TranslateStepStatus(status dao.StepStatusType) (string, error) {
	switch status {
	case dao.StepIdle:
		return "Idle", nil
	case dao.StepInProgress:
		return "In Progress", nil
	case dao.StepDone:
		return "Done", nil
	default:
		return "Error", fmt.Errorf("failed to translate step status: %d", status)
	}
}
func TranslateToStepStatus(status string) (dao.StepStatusType, error) {
	switch status {
	case "Idle":
		return dao.StepIdle, nil
	case "In Progress":
		return dao.StepInProgress, nil
	case "Done":
		return dao.StepDone, nil
	default:
		return dao.StepIdle, fmt.Errorf("failed to translate statys to step status")
	}
}
func ListSteps(runId string) ([]*dao.StepRecord, error) {
	return dao.ListSteps(runId)
}

//func ToStep(stepRecord *dao.StepRecord) (*Step, error) {
//	step := Step{
//		Template: stepRecord.Template,
//	}
//	err := step.AdjustUnmarshalStep(true)
//	if err != nil {
//		return nil, err
//	}
//	step.stepRecord = stepRecord
//	return &step, nil
//}

func UpdateStepStatus(prevStepRecord *dao.StepRecord, newStatus dao.StepStatusType, doFinish bool) error {
	tx, err := dao.DB.SQL().Beginx()
	if err != nil {
		return fmt.Errorf("failed to start a database transaction: %w", err)
	}
	runId := prevStepRecord.RunId
	stepRecord, err := dao.GetStepTx(tx, runId, prevStepRecord.Index)
	if err != nil {
		err = dao.Rollback(tx, err)
		return fmt.Errorf("failed to update database stepRecord row: %w", err)
	}

	// if we are starting check if the stepRecord is already in-progress.
	if !doFinish && stepRecord.Status == dao.StepInProgress {
		delta := stepRecord.Now.(time.Time).Sub(stepRecord.HeartBeat.(time.Time))
		if delta < 0 {
			delta = delta * -1
		}
		// +5 for slow downs
		if delta <= (HeartBeatInterval+5)*time.Second {
			err = dao.Rollback(tx, fmt.Errorf("stepRecord is already in progress and has a heartbeat with an interval of %d: %w", HeartBeatInterval, ErrStepAlreadyInProgress))
			return fmt.Errorf("failed to update database stepRecord row: %w", err)
		}
	}

	// don't change done if the status did not change.
	if newStatus == stepRecord.Status {
		err = tx.Rollback()
		return ErrStatusNotChanged
	}
	_, err = stepRecord.UpdateStatusAndHeartBeatTx(tx, newStatus)
	if err != nil {
		err = dao.Rollback(tx, err)
		return fmt.Errorf("failed to update database stepRecord row: %w", err)
	}

	if newStatus != dao.StepIdle {
		var run *dao.RunRecord
		run, err = dao.GetRunTx(tx, runId)
		if err != nil {
			err = dao.Rollback(tx, err)
			return fmt.Errorf("failed to update database stepRecord row: %w", err)
		}
		if run.Status == dao.RunIdle {
			_, err = dao.UpdateRunStatusTx(tx, run.Id, dao.RunInProgress)
			if err != nil {
				err = dao.Rollback(tx, err)
				return fmt.Errorf("failed to update database run row: %w", err)
			}
		} else if run.Status == dao.RunDone {
			err = dao.Rollback(tx, ErrRunIsAlreadyDone)
			return fmt.Errorf("failed to update database stepRecord status: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
	*prevStepRecord = *stepRecord
	return nil
}

func (s *Step) StartDo(stepRecord *dao.StepRecord) error {
	err := UpdateStepStatus(stepRecord, dao.StepInProgress, false)
	if err != nil {
		return err
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
				errBeat := stepRecord.UpdateHeartBeat()
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
	_ = do(s.doType, s.Do)
	close(heartBeatDone2)
	wg.Wait()
	err = UpdateStepStatus(stepRecord, dao.StepDone, true)
	return err
}
