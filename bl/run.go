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
	"github.com/google/uuid"
)

type RunStatusType int64

const (
	RunStopped    RunStatusType = 10
	RunInProgress RunStatusType = 12
	RunDone       RunStatusType = 15
)

type RunRecord struct {
	Id     int64
	UUID   string
	Title  string
	Cursor int64
	Status RunStatusType
	Script string
}

func TranslateRunStatus(status RunStatusType) (string, error) {
	switch status {
	case RunStopped:
		return "Stopped", nil
	case RunInProgress:
		return "In Progress", nil
	case RunDone:
		return "Done", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}

func ListRuns() ([]*RunRecord, error) {
	var result []*RunRecord
	rows, err := DB.SQL().Queryx("SELECT * FROM runs ORDER BY id DESC")
	if err != nil {
		return nil, fmt.Errorf("failed to query database runs table: %w", err)
	}

	for rows.Next() {
		var run RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database runs row: %w", err)
		}
		result = append(result, &run)
	}
	return result, nil
}

func GetRun(runId int64) (*RunRecord, error) {
	var result *RunRecord
	rows, err := DB.SQL().Queryx("SELECT * FROM runs where id=$1", runId)
	if err != nil {
		return nil, fmt.Errorf("failed to query database runs table - get: %w", err)
	}

	for rows.Next() {
		var run RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database runs row - get: %w", err)
		}
		result = &run
	}
	if result == nil {
		return nil, ErrRecordNotFound
	}
	return result, nil
}

func (r *RunRecord) GetCursorStep() (*StepRecord, error) {
	step, err := getStep(nil, r.Id, r.Cursor)
	return step, err
}

func (r *RunRecord) UpdateStatus(newStatus RunStatusType) error {
	tx, err := DB.SQL().Beginx()
	if newStatus == RunStopped {
		steps, err := listSteps(tx, r.Id)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to update database run status: %w", err)
		}
		for _, stepRecord := range steps {
			if stepRecord.Status == StepInProgress {
				err = Rollback(tx, err)
				return fmt.Errorf("failed to update database run status: %w", err)
			}
		}

	} else {
		err = Rollback(tx, fmt.Errorf("not allowed to set status done or in progress directly"))
		return fmt.Errorf("failed to update database run status: %w", err)
	}
	_, err = DB.SQL().Exec("update runs set status=$1 where id=$2", newStatus, r.Id)
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
func (r *RunRecord) Create(err error, s *Script, yamlBytes []byte) error {
	tx, err := DB.SQL().Beginx()
	if err != nil {
		return fmt.Errorf("failed to create database transaction: %w", err)
	}

	count := -1
	err = tx.Get(&count, "SELECT count(*) FROM runs where status=$1 and title=$2", RunInProgress, s.Title)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to query database runs table count for in progress rows: %w", err)
	}
	if count != 0 {
		err = Rollback(tx, ErrActiveRunsWithSameTitleExists)
		return err
	}
	count = 0
	err = tx.Get(&count, "SELECT count(*) FROM runs limit 1")
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to query database runs table count: %w", err)
	}

	uuid4, err := uuid.NewRandom()
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to generate uuid: %w", err)
	}
	runRecord := RunRecord{
		UUID:   uuid4.String(),
		Title:  s.Title,
		Cursor: 1,
		Status: RunInProgress,
		Script: string(yamlBytes),
	}

	runRecord.Id, err = DB.CreateRun(tx, &runRecord)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to create runs row: %w", err)
	}

	for i, step := range s.Steps {
		uuid4, err := uuid.NewRandom()
		stepRecord := StepRecord{
			RunId:  runRecord.Id,
			StepId: int64(i) + 1,
			UUID:   uuid4.String(),
			Name:   step.Name,
			Status: StepNotStarted,
			Script: step.Script,
		}
		_, err = tx.NamedExec("INSERT INTO steps(run_id, step_id, uuid, name, status, heartbeat, script) values(:run_id,:step_id,:uuid,:name,:status,0,:script)", &stepRecord)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to insert database steps row: %w", err)
		}
	}

	*r = runRecord

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
	return nil
}
