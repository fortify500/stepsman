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
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
	"time"
)

const CurrentTimeStamp = "2006-01-02 15:04:05"

type StepStatusType int64

const (
	StepIdle       StepStatusType = 0
	StepInProgress StepStatusType = 2
	StepFailed     StepStatusType = 4
	StepDone       StepStatusType = 5
)

type StepState struct {
	DoType string      `json:"do-type,omitempty" mapstructure:"do-type" yaml:"do-type,omitempty"`
	Result interface{} `json:"result,omitempty" mapstructure:"result" yaml:"result"`
	Error  string      `json:"error,omitempty" mapstructure:"error" yaml:"error,omitempty"`
}

type StepRecord struct {
	RunId      string `db:"run_id"`
	Index      int64  `db:"index"`
	Label      string
	UUID       string
	Name       string
	Status     StepStatusType
	StatusUUID string `db:"status_uuid"`
	Now        interface{}
	HeartBeat  interface{}
	State      string
}

func (s *StepRecord) PrettyJSONState() (string, error) {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(s.State)))
	decoder.DisallowUnknownFields()
	var tmp interface{}
	err := decoder.Decode(&tmp)
	if err != nil {
		return "", err
	}
	prettyBytes, err := json.MarshalIndent(&tmp, "", "  ")
	if err != nil {
		return "", err
	}
	return string(prettyBytes), err
}
func (s *StepRecord) PrettyYamlState() (string, error) {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(s.State)))
	decoder.DisallowUnknownFields()
	var tmp interface{}
	err := decoder.Decode(&tmp)
	if err != nil {
		return "", err
	}
	prettyBytes, err := yaml.Marshal(&tmp)
	if err != nil {
		return "", err
	}
	return string(prettyBytes), err
}

func (s *StepRecord) UpdateHeartBeat(uuid string) error {
	if s.StatusUUID != uuid {
		return fmt.Errorf("cannot update heartbeat for a different status_uuid in order to prevent a race condition")
	}
	res, err := DB.SQL().Exec("update steps set heartbeat=CURRENT_TIMESTAMP where run_id=$1 and \"index\"=$2 and status_uuid=$3", s.RunId, s.Index, s.StatusUUID)
	if err == nil {
		affected, err := res.RowsAffected()
		if err == nil {
			if affected < 1 {
				err = fmt.Errorf("no rows where affecting, suggesting status_uuid has changed (but possibly the record have been deleted)")
			}
		}
	}
	if err != nil {
		return fmt.Errorf("failed to update database step heartbeat: %w", err)
	}
	return nil
}

func GetStep(runId string, index int64) (*StepRecord, error) {
	return GetStepTx(nil, runId, index)
}
func GetStepByUUID(uuid4 string) (*StepRecord, error) {
	return GetStepByUUIDTx(nil, uuid4)
}
func GetStepByUUIDTx(tx *sqlx.Tx, uuid4 string) (*StepRecord, error) {
	var result *StepRecord

	const query = "SELECT *,CURRENT_TIMESTAMP as now FROM steps where uuid=$1"
	var rows *sqlx.Rows
	var err error
	if tx == nil {
		rows, err = DB.SQL().Queryx(query, uuid4)
	} else {
		rows, err = tx.Queryx(query, uuid4)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query database steps table - get: %w", err)
	}

	defer rows.Close()
	for rows.Next() {
		var step StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row - get: %w", err)
		}
		err = adjustStepNow(step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row: %w", err)
		}
		result = &step
		break
	}
	if result == nil {
		return nil, ErrRecordNotFound
	}
	return result, nil
}

func GetStepTx(tx *sqlx.Tx, runId string, index int64) (*StepRecord, error) {
	var result *StepRecord

	const query = "SELECT *,CURRENT_TIMESTAMP as now FROM steps where run_id=$1 and \"index\"=$2"
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

	defer rows.Close()
	for rows.Next() {
		var step StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row - get: %w", err)
		}
		err = adjustStepNow(step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row: %w", err)
		}
		result = &step
		break
	}
	if result == nil {
		return nil, ErrRecordNotFound
	}
	return result, nil
}

func ListSteps(runId string) ([]*StepRecord, error) {
	tx, err := DB.SQL().Beginx()
	if err != nil {
		return nil, fmt.Errorf("failed to start a database transaction: %w", err)
	}
	stepRecords, err := ListStepsTx(tx, runId)
	if err != nil {
		err = Rollback(tx, err)
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit list steps transaction: %w", err)
	}
	return stepRecords, nil
}

func ListStepsTx(tx *sqlx.Tx, runId string) ([]*StepRecord, error) {
	var result []*StepRecord
	var rows *sqlx.Rows
	var err error
	rows, err = DB.ListStepsTx(tx, runId, rows, err)
	if err != nil {
		return nil, fmt.Errorf("failed to query database steps table: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var step StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row: %w", err)
		}
		err = adjustStepNow(step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row: %w", err)
		}
		result = append(result, &step)
	}
	return result, nil
}

func adjustStepNow(step StepRecord) error {
	var err error
	switch v := step.Now.(type) {
	case time.Time:
	case string:
		step.Now, err = time.Parse(CurrentTimeStamp, v)
	case []byte:
		step.Now, err = time.Parse(CurrentTimeStamp, string(step.Now.([]byte)))
	default:
		err = fmt.Errorf("invalid type for current_timestamp")
	}
	return err
}

func (s *StepRecord) UpdateStatusAndHeartBeatTx(tx *sqlx.Tx, newStatus StepStatusType) (sql.Result, error) {
	uuid4, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate uuid: %w", err)
	}
	s.StatusUUID = uuid4.String()
	return tx.Exec("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP where run_id=$2 and \"index\"=$3", newStatus, s.RunId, s.Index)
}

func (s *StepRecord) UpdateStateAndStatusAndHeartBeatTx(tx *sqlx.Tx, newStatus StepStatusType, newState *StepState) (sql.Result, error) {
	uuid4, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate uuid: %w", err)
	}
	newStateStr, err := json.Marshal(newState)
	if err != nil {
		return nil, err
	}
	s.StatusUUID = uuid4.String()
	return tx.Exec("update steps set status=$1, state=$2, heartbeat=CURRENT_TIMESTAMP where run_id=$3 and \"index\"=$4", newStatus, newStateStr, s.RunId, s.Index)
}
