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
	"github.com/google/uuid"
)

func ListRuns() ([]*dao.RunRecord, error) {
	if IsRemote {
		return RemoteListRuns()
	} else {
		return dao.ListRuns()
	}
}

func GetCursorStep(runRecord *dao.RunRecord) (*dao.StepRecord, error) {
	step, err := dao.GetStep(runRecord.Id, runRecord.Cursor)
	return step, err
}

func UpdateRunStatus(runRecord *dao.RunRecord, newStatus dao.RunStatusType) error {
	tx, err := dao.DB.SQL().Beginx()
	if newStatus == dao.RunStopped {
		//TODO: make a more efficient query.
		steps, err := dao.ListStepsTx(tx, runRecord.Id)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to update database run status: %w", err)
		}
		for _, stepRecord := range steps {
			if stepRecord.Status == dao.StepInProgress {
				err = Rollback(tx, err)
				return fmt.Errorf("failed to update database run status: %w", err)
			}
		}

	} else {
		err = Rollback(tx, fmt.Errorf("not allowed to set status done or in progress directly"))
		return fmt.Errorf("failed to update database run status: %w", err)
	}
	_, err = dao.UpdateRunStatus(runRecord.Id, newStatus)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to update database run status: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
	return err
}

func (s *Script) CreateRun(yamlBytes []byte) (*dao.RunRecord, error) {
	title := s.Title
	tx, err := dao.DB.SQL().Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to create database transaction: %w", err)
	}

	count := -1
	err = dao.GetTitleInProgressTx(tx, count, title)
	if err != nil {
		err = Rollback(tx, err)
		return nil, fmt.Errorf("failed to query database runs table count for in progress rows: %w", err)
	}
	if count != 0 {
		err = Rollback(tx, dao.ErrActiveRunsWithSameTitleExists)
		return nil, err
	}

	uuid4, err := uuid.NewRandom()
	if err != nil {
		err = Rollback(tx, err)
		return nil, fmt.Errorf("failed to generate uuid: %w", err)
	}
	runRecord := dao.RunRecord{
		UUID:   uuid4.String(),
		Title:  title,
		Cursor: 1,
		Status: dao.RunInProgress,
		Script: string(yamlBytes),
	}

	runRecord.Id, err = dao.DB.CreateRun(tx, &runRecord)
	if err != nil {
		err = Rollback(tx, err)
		return nil, fmt.Errorf("failed to create runs row: %w", err)
	}

	for i, step := range s.Steps {
		uuid4, err := uuid.NewRandom()
		stepRecord := &dao.StepRecord{
			RunId:  runRecord.Id,
			StepId: int64(i) + 1,
			UUID:   uuid4.String(),
			Name:   step.Name,
			Status: dao.StepNotStarted,
			Script: step.Script,
		}
		_, err = dao.CreateStepTx(tx, stepRecord)
		if err != nil {
			err = Rollback(tx, err)
			return nil, fmt.Errorf("failed to insert database steps row: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit database transaction: %w", err)
	}
	return &runRecord, nil
}
