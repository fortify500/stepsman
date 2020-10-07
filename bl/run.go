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
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/dao"
	"github.com/google/uuid"
)

func ListRuns(query *dao.Query) ([]*dao.RunRecord, *dao.RangeResult, error) {
	if dao.IsRemote {
		return dao.RemoteListRuns(query)
	} else {
		return dao.ListRuns(query)
	}
}

func GetRun(id string) (*dao.RunRecord, error) {
	if dao.IsRemote {
		runs, err := dao.RemoteGetRuns([]string{id})
		if err != nil {
			return nil, err
		}
		return runs[0], nil
	} else {
		return dao.GetRun(id)
	}
}

func GetRuns(ids []string) ([]*dao.RunRecord, error) {
	if dao.IsRemote {
		return dao.RemoteGetRuns(ids)
	} else {
		return dao.GetRuns(ids)
	}
}

func GetNotDoneAndNotSkippedStep(runRecord *dao.RunRecord) (*dao.StepRecord, error) {
	steps, err := ListSteps(runRecord.Id)
	for _, step := range steps {
		//TODO: find a more efficient query
		if step.Status != dao.StepDone && step.Status != dao.StepSkipped {
			return step, nil
		}
	}
	return nil, err
}

func UpdateRunStatus(runRecord *dao.RunRecord, newStatus dao.RunStatusType) error {
	tx, err := dao.DB.SQL().Beginx()
	if newStatus == dao.RunIdle {
		//TODO: make a more efficient query.
		steps, err := dao.ListStepsTx(tx, runRecord.Id)
		if err != nil {
			err = dao.Rollback(tx, err)
			return fmt.Errorf("failed to update database run status: %w", err)
		}
		for _, stepRecord := range steps {
			if stepRecord.Status == dao.StepInProgress {
				err = dao.Rollback(tx, err)
				return fmt.Errorf("failed to update database run status: %w", err)
			}
		}

	} else {
		err = dao.Rollback(tx, fmt.Errorf("not allowed to set status done or in progress directly"))
		return fmt.Errorf("failed to update database run status: %w", err)
	}
	_, err = dao.UpdateRunStatus(runRecord.Id, newStatus)
	if err != nil {
		err = dao.Rollback(tx, err)
		return fmt.Errorf("failed to update database run status: %w", err)
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
	return err
}

func (s *Template) CreateRun(key string) (*dao.RunRecord, error) {
	title := s.Title
	tx, err := dao.DB.SQL().Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to create database transaction: %w", err)
	}

	uuid4, err := uuid.NewRandom()
	if err != nil {
		err = dao.Rollback(tx, err)
		return nil, fmt.Errorf("failed to generate uuid: %w", err)
	}
	jsonBytes, err := json.Marshal(s)
	runRecord := dao.RunRecord{
		Id:              uuid4.String(),
		Key:             key,
		TemplateVersion: s.Version,
		TemplateTitle:   title,
		Status:          dao.RunIdle,
		Template:        string(jsonBytes),
		State:           "{}",
	}

	err = dao.CreateRunTx(tx, &runRecord)
	if err != nil {
		err = dao.Rollback(tx, err)
		return nil, fmt.Errorf("failed to create runs row: %w", err)
	}

	for i, step := range s.Steps {
		uuid4, err := uuid.NewRandom()
		if err != nil {
			err = dao.Rollback(tx, err)
			return nil, fmt.Errorf("failed to create runs row and generate uuid4: %w", err)
		}
		//	"run_id" UUid NOT NULL,
		//	"index" Bigint NOT NULL,
		//	"uuid" UUid NOT NULL,
		//	"status" Bigint NOT NULL,
		//	"heartbeat" TIMESTAMP NOT NULL,
		//	"label" Text NOT NULL,
		//	"name" Text,
		//	"state" jsonb,
		stepRecord := &dao.StepRecord{
			RunId:  runRecord.Id,
			Index:  int64(i) + 1,
			UUID:   uuid4.String(),
			Status: dao.StepNotStarted,
			Label:  step.Label,
			Name:   step.Name,
			State:  "{}",
		}
		_, err = dao.CreateStepTx(tx, stepRecord)
		if err != nil {
			err = dao.Rollback(tx, err)
			return nil, fmt.Errorf("failed to insert database steps row: %w", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit database transaction: %w", err)
	}
	return &runRecord, nil
}
