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
	"github.com/fortify500/stepsman/dao"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
)

func (b *BL) ListRuns(query *api.ListQuery) ([]api.RunRecord, *api.RangeResult, error) {
	if dao.IsRemote {
		return b.Client.RemoteListRuns(query)
	} else {
		return b.listRuns(query)
	}
}

func (b *BL) listRuns(query *api.ListQuery) ([]api.RunRecord, *api.RangeResult, error) {
	var runRecords []api.RunRecord
	var rangeResult *api.RangeResult
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runRecords, rangeResult, err = dao.ListRunsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to list runs: %w", err)
		}
		return nil
	})
	return runRecords, rangeResult, tErr
}

func (b *BL) DeleteRuns(query *api.DeleteQuery) error {
	if dao.IsRemote {
		return b.Client.RemoteDeleteRuns(query)
	} else {
		return b.deleteRunInternal(query)
	}
}
func (b *BL) deleteRunInternal(query *api.DeleteQuery) error {
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		return dao.DeleteRunsTx(tx, query)
	})
	return tErr

}

func (b *BL) GetRun(options api.Options, id uuid.UUID) (*api.RunRecord, error) {
	if dao.IsRemote {
		runs, err := b.Client.RemoteGetRuns(&api.GetRunsQuery{
			Ids:              []uuid.UUID{id},
			ReturnAttributes: nil,
			Options:          options,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to get run: %w", err)
		}
		return &runs[0], nil
	} else {
		return b.getRunById(options, id)
	}
}

func (b *BL) GetRuns(query *api.GetRunsQuery) ([]api.RunRecord, error) {
	if dao.IsRemote {
		return b.Client.RemoteGetRuns(query)
	} else {
		return b.getRuns(query)
	}
}

func (b *BL) getRuns(query *api.GetRunsQuery) ([]api.RunRecord, error) {
	var runRecords []api.RunRecord
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runRecords, err = dao.GetRunsTx(tx, query)
		if err != nil {
			return fmt.Errorf("failed to get runs: %w", err)
		}
		return nil
	})
	return runRecords, tErr
}
func (b *BL) UpdateRunStatus(options api.Options, runId uuid.UUID, newStatus api.RunStatusType) error {
	if dao.IsRemote {
		return b.Client.RemoteUpdateRun(&api.UpdateQueryById{
			Id: runId,
			Changes: map[string]interface{}{
				"status": newStatus.TranslateRunStatus(),
			},
			Options: options,
		})
	} else {
		return b.updateRunStatusLocal(options, runId, newStatus)
	}
}
func (b *BL) updateRunStatusLocal(options api.Options, runId uuid.UUID, newStatus api.RunStatusType) error {
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		var err error
		runRecord, err := GetRunByIdTx(tx, options, runId)
		if err != nil {
			return fmt.Errorf("failed to update database run status: %w", err)
		}
		if newStatus == runRecord.Status {
			return api.NewError(api.ErrStatusNotChanged, "update run status have not changed")
		}
		b.DAO.UpdateRunStatusTx(tx, options, runRecord.Id, newStatus, nil, nil)
		return nil
	})
	return tErr
}

var emptyContext = make(api.Context)

func (t *Template) CreateRun(BL *BL, options api.Options, key string) (*api.RunRecord, error) {
	title := t.Title
	var runRecord *api.RunRecord
	tErr := BL.DAO.Transactional(func(tx *sqlx.Tx) error {
		var err error
		{
			var uuid4 uuid.UUID
			uuid4, err = uuid.NewRandom()
			if err != nil {
				panic(fmt.Errorf("failed to generate uuid: %w", err))
			}
			var jsonBytes []byte
			jsonBytes, err = json.Marshal(t)
			if err != nil {
				panic(err)
			}
			runRecord = &api.RunRecord{
				GroupId:         options.GroupId,
				Id:              uuid4,
				Key:             key,
				Tags:            t.Tags,
				CreatedAt:       api.AnyTime{},
				TemplateVersion: t.Version,
				TemplateTitle:   title,
				Status:          api.RunIdle,
				Template:        string(jsonBytes),
			}
		}
		BL.DAO.DB.CreateRunTx(tx, runRecord, t.Expiration.CompleteBy)

		for i, step := range t.Steps {
			var uuid4 uuid.UUID
			var statusOwner uuid.UUID
			uuid4, err = uuid.NewRandom()
			if err != nil {
				panic(fmt.Errorf("failed to create runs row and generate uuid4: %w", err))
			}
			statusOwner, err = uuid.NewRandom()
			if err != nil {
				panic(fmt.Errorf("failed to create runs row and generate status owner uuid4: %w", err))
			}
			retriesLeft := step.Retries + 1

			if retriesLeft < 1 {
				retriesLeft = 1
			}

			stepRecord := &api.StepRecord{
				GroupId:     options.GroupId,
				RunId:       runRecord.Id,
				Index:       i + 1,
				UUID:        uuid4,
				Status:      api.StepIdle,
				StatusOwner: statusOwner.String(),
				Tags:        step.Tags,
				Label:       step.Label,
				Name:        step.Name,
				State:       api.State{},
				Context:     emptyContext,
				RetriesLeft: retriesLeft,
			}
			BL.DAO.DB.CreateStepTx(tx, stepRecord)
		}
		return nil
	})
	return runRecord, tErr
}

func (b *BL) getRunById(options api.Options, id uuid.UUID) (*api.RunRecord, error) {
	var result *api.RunRecord
	tErr := b.DAO.Transactional(func(tx *sqlx.Tx) error {
		runs, err := dao.GetRunsTx(tx, &api.GetRunsQuery{
			Ids:              []uuid.UUID{id},
			ReturnAttributes: nil,
			Options:          options,
		})
		if err != nil {
			return fmt.Errorf("failed to get run: %w", err)
		}
		result = &runs[0]
		return nil
	})
	return result, tErr
}

func GetRunByIdTx(tx *sqlx.Tx, options api.Options, id uuid.UUID) (*api.RunRecord, error) {
	runs, err := dao.GetRunsTx(tx, &api.GetRunsQuery{
		Ids:              []uuid.UUID{id},
		ReturnAttributes: nil,
		Options:          options,
	})
	if err != nil {
		return nil, err
	}
	return &runs[0], nil
}
