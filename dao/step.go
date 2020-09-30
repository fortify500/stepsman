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
package dao

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"time"
)

type StepStatusType int64

const StepNotStarted StepStatusType = 0

const StepInProgress StepStatusType = 2

const StepCanceled StepStatusType = 3

const StepFailed StepStatusType = 4

const StepSkipped StepStatusType = 6

type StepRecord struct {
	RunId     int64 `db:"run_id"`
	StepId    int64 `db:"step_id"`
	UUID      string
	Name      string
	Status    StepStatusType
	HeartBeat int64
}

func CreateStepTx(tx *sqlx.Tx, stepRecord *StepRecord) (sql.Result, error) {
	return tx.NamedExec("INSERT INTO steps(run_id, step_id, uuid, name, status, heartbeat, script) values(:run_id,:step_id,:uuid,:name,:status,0,:script)", stepRecord)
}

func (s *StepRecord) UpdateHeartBeat() error {
	_, err := DB.SQL().Exec("update steps set heartbeat=$1 where run_id=$2 and step_id=$3", time.Now().Unix(), s.RunId, s.StepId)
	if err != nil {
		return fmt.Errorf("failed to update database step heartbeat: %w", err)
	}
	return nil
}

func GetStep(runId int64, stepId int64) (*StepRecord, error) {
	return GetStepTx(nil, runId, stepId)
}
func GetStepTx(tx *sqlx.Tx, runId int64, stepId int64) (*StepRecord, error) {
	var result *StepRecord

	const query = "SELECT * FROM steps where run_id=$1 and step_id=$2"
	var rows *sqlx.Rows
	var err error
	if tx == nil {
		rows, err = DB.SQL().Queryx(query, runId, stepId)
	} else {
		rows, err = tx.Queryx(query, runId, stepId)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query database steps table - get: %w", err)
	}

	for rows.Next() {
		var step StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row - get: %w", err)
		}
		result = &step
	}
	if result == nil {
		return nil, ErrRecordNotFound
	}
	return result, nil
}

func ListSteps(runId int64) ([]*StepRecord, error) {
	return ListStepsTx(nil, runId)
}

func ListStepsTx(tx *sqlx.Tx, runId int64) ([]*StepRecord, error) {
	var result []*StepRecord
	var rows *sqlx.Rows
	var err error
	const query = "SELECT * FROM steps WHERE run_id=$1"
	if tx == nil {
		rows, err = DB.SQL().Queryx(query, runId)
	} else {
		rows, err = tx.Queryx(query, runId)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query database steps table: %w", err)
	}

	for rows.Next() {
		var step StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row: %w", err)
		}
		result = append(result, &step)
	}
	return result, nil
}

func (s *StepRecord) UpdateStatusAndHeartBeatTx(tx *sqlx.Tx, newStatus StepStatusType, heartBeat int64) (sql.Result, error) {
	return tx.Exec("update steps set status=$1, heartbeat=$2 where run_id=$3 and step_id=$4", newStatus, heartBeat, s.RunId, s.StepId)
}
