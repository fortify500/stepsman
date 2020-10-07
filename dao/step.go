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

const (
	StepNotStarted StepStatusType = 0
	StepInProgress StepStatusType = 2
	StepCanceled   StepStatusType = 3
	StepFailed     StepStatusType = 4
	StepSkipped    StepStatusType = 6
	StepDone       StepStatusType = 5
)

type StepRecord struct {
	RunId     string `db:"run_id"`
	Index     int64  `db:"index"`
	Label     string
	UUID      string
	Name      string
	Status    StepStatusType
	Now       time.Time
	HeartBeat time.Time
	State     string
}

func CreateStepTx(tx *sqlx.Tx, stepRecord *StepRecord) (sql.Result, error) {
	return tx.NamedExec("INSERT INTO steps(run_id, index, label, uuid, name, status, heartbeat, state) values(:run_id,:index,:label,:uuid,:name,:status,to_timestamp(0),:state)", stepRecord)
}

func (s *StepRecord) UpdateHeartBeat() error {
	_, err := DB.SQL().Exec("update steps set heartbeat=LOCALTIMESTAMP where run_id=$1 and index=$2", s.RunId, s.Index)
	if err != nil {
		return fmt.Errorf("failed to update database step heartbeat: %w", err)
	}
	return nil
}

func GetStep(runId string, index int64) (*StepRecord, error) {
	return GetStepTx(nil, runId, index)
}
func GetStepTx(tx *sqlx.Tx, runId string, index int64) (*StepRecord, error) {
	var result *StepRecord

	const query = "SELECT *,LOCALTIMESTAMP as now FROM steps where run_id=$1 and index=$2"
	var rows *sqlx.Rows
	var err error
	if tx == nil {
		rows, err = DB.SQL().Queryx(query, runId, index)
	} else {
		rows, err = tx.Queryx(query, runId, index)
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

func ListSteps(runId string) ([]*StepRecord, error) {
	return ListStepsTx(nil, runId)
}

func ListStepsTx(tx *sqlx.Tx, runId string) ([]*StepRecord, error) {
	var result []*StepRecord
	var rows *sqlx.Rows
	var err error
	const query = "SELECT *,LOCALTIMESTAMP as now FROM steps WHERE run_id=$1"
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

func (s *StepRecord) UpdateStatusAndHeartBeatTx(tx *sqlx.Tx, newStatus StepStatusType) (sql.Result, error) {
	return tx.Exec("update steps set status=$1, heartbeat=LOCALTIMESTAMP where run_id=$2 and index=$3", newStatus, s.RunId, s.Index)
}
