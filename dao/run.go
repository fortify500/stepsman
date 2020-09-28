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
package dao

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
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

func GetTitleInProgressTx(tx *sqlx.Tx, title string) (int, error) {
	var count int
	err := tx.Get(&count, "SELECT count(*) FROM runs where status=$1 and title=$2", RunInProgress, title)
	return count, err
}

func UpdateRunStatus(runId int64, newStatus RunStatusType) (sql.Result, error) {
	return DB.SQL().Exec("update runs set status=$1 where id=$2", newStatus, runId)
}

func UpdateRunCursorTx(tx *sqlx.Tx, stepId int64, runId int64) (sql.Result, error) {
	return tx.Exec("update runs set cursor=$1 where id=$2", stepId, runId)
}

func TranslateToRunStatus(status string) (RunStatusType, error) {
	switch status {
	case "Stopped":
		return RunStopped, nil
	case "In Progress":
		return RunInProgress, nil
	case "Done":
		return RunDone, nil
	default:
		return RunStopped, fmt.Errorf("failed to translate run status: %s", status)
	}
}
func (s RunStatusType) TranslateRunStatus() (string, error) {
	switch s {
	case RunStopped:
		return "Stopped", nil
	case RunInProgress:
		return "In Progress", nil
	case RunDone:
		return "Done", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", s)
	}
}
