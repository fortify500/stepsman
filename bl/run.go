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
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/dao"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func ListRuns(query *api.ListQuery) ([]api.RunRecord, *api.RangeResult, error) {
	if dao.IsRemote {
		return client.RemoteListRuns(query)
	} else {
		return listRuns(query)
	}
}

func listRuns(query *api.ListQuery) ([]api.RunRecord, *api.RangeResult, error) {
	var runRecords []api.RunRecord
	var rangeResult *api.RangeResult
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runRecords, rangeResult, err = dao.ListRunsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to list runs: %w", err)
		}
		return nil
	})
	return runRecords, rangeResult, tErr
}

func GetRun(id string) (*api.RunRecord, error) {
	if dao.IsRemote {
		runs, err := client.RemoteGetRuns(&api.GetRunsQuery{
			Ids:              []string{id},
			ReturnAttributes: nil,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get run: %w", err)
		}
		return &runs[0], nil
	} else {
		return getRunById(id)
	}
}

func GetRuns(query *api.GetRunsQuery) ([]api.RunRecord, error) {
	if dao.IsRemote {
		return client.RemoteGetRuns(query)
	} else {
		return getRuns(query)
	}
}

func getRuns(query *api.GetRunsQuery) ([]api.RunRecord, error) {
	var runRecords []api.RunRecord
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runRecords, err = dao.GetRunsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to get runs: %w", err)
		}
		return nil
	})
	return runRecords, tErr
}
func UpdateRunStatus(runId string, newStatus api.RunStatusType) error {
	if dao.IsRemote {
		return client.RemoteUpdateRun(&api.UpdateQuery{
			Id: runId,
			Changes: map[string]interface{}{
				"status": newStatus.TranslateRunStatus(),
			},
		})
	} else {
		return UpdateRunStatusLocal(runId, newStatus)
	}
}
func UpdateRunStatusLocal(runId string, newStatus api.RunStatusType) error {
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runRecord, err := GetRunByIdTx(tx, runId)
		if err != nil {
			return fmt.Errorf("failed to update database run status: %w", err)
		}
		if newStatus == runRecord.Status {
			return api.NewError(api.ErrStatusNotChanged, "update run status have not changed")
		}
		dao.UpdateRunStatusTx(tx, runRecord.Id, newStatus)
		return nil
	})
	return tErr
}

func (s *Template) CreateRun(key string) (*api.RunRecord, error) {
	title := s.Title
	var runRecord *api.RunRecord
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		var err error
		{
			var uuid4 uuid.UUID
			uuid4, err = uuid.NewRandom()
			if err != nil {
				panic(fmt.Errorf("failed to generate uuid: %w", err))
			}
			var jsonBytes []byte
			jsonBytes, err = json.Marshal(s)
			if err != nil {
				panic(err)
			}
			runRecord = &api.RunRecord{
				Id:              uuid4.String(),
				Key:             key,
				TemplateVersion: s.Version,
				TemplateTitle:   title,
				Status:          api.RunIdle,
				Template:        string(jsonBytes),
			}
		}
		dao.CreateRunTx(tx, runRecord)

		for i, step := range s.Steps {
			var uuid4 uuid.UUID
			var statusUuid4 uuid.UUID
			uuid4, err = uuid.NewRandom()
			if err != nil {
				panic(fmt.Errorf("failed to create runs row and generate uuid4: %w", err))
			}
			statusUuid4, err = uuid.NewRandom()
			if err != nil {
				panic(fmt.Errorf("failed to create runs row and generate status uuid4: %w", err))
			}

			stepRecord := &api.StepRecord{
				RunId:      runRecord.Id,
				Index:      int64(i) + 1,
				UUID:       uuid4.String(),
				Status:     api.StepIdle,
				StatusUUID: statusUuid4.String(),
				Label:      step.Label,
				Name:       step.Name,
				State:      "{}",
			}
			dao.DB.CreateStepTx(tx, stepRecord)
		}
		return nil
	})
	return runRecord, tErr
}

func getRunById(id string) (*api.RunRecord, error) {
	var result *api.RunRecord
	tErr := dao.Transactional(func(tx *sqlx.Tx) error {
		runs, err := dao.GetRunsTx(tx, &api.GetRunsQuery{
			Ids:              []string{id},
			ReturnAttributes: nil,
		})
		if err != nil {
			return fmt.Errorf("failed to get run: %w", err)
		}
		result = &runs[0]
		return nil
	})
	return result, tErr
}

func GetRunByIdTx(tx *sqlx.Tx, id string) (*api.RunRecord, error) {
	runs, err := dao.GetRunsTx(tx, &api.GetRunsQuery{
		Ids:              []string{id},
		ReturnAttributes: nil,
	})
	if err != nil {
		return nil, err
	}
	return &runs[0], nil
}
