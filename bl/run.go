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
		return dao.GetRunsTx(nil, ids)
	}
}

func UpdateRunStatus(runRecord *dao.RunRecord, newStatus dao.RunStatusType) error {
	tx, err := dao.DB.SQL().Beginx()
	runRecord, err = dao.GetRunTx(tx, runRecord.Id)
	if err != nil {
		err = dao.Rollback(tx, err)
		return fmt.Errorf("failed to update database run status: %w", err)
	}
	if newStatus == runRecord.Status {
		return nil
	}
	// if idle and a step is started it will change to in progress
	// if done then it is ok, there are steps in progress but new steps will not be started.
	_, err = dao.UpdateRunStatusTx(tx, runRecord.Id, newStatus)
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
	if err != nil {
		return nil, err
	}
	runRecord := dao.RunRecord{
		Id:              uuid4.String(),
		Key:             key,
		TemplateVersion: s.Version,
		TemplateTitle:   title,
		Status:          dao.RunIdle,
		Template:        string(jsonBytes),
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
			RunId:      runRecord.Id,
			Index:      int64(i) + 1,
			UUID:       uuid4.String(),
			Status:     dao.StepIdle,
			StatusUUID: uuid4.String(),
			Label:      step.Label,
			Name:       step.Name,
			State:      "{}",
		}
		_, err = dao.DB.CreateStepTx(tx, stepRecord)
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
