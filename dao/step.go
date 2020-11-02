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
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"strings"
)

type StepState struct {
	DoType string      `json:"do-type,omitempty" mapstructure:"do-type" yaml:"do-type,omitempty"`
	Result interface{} `json:"result,omitempty" mapstructure:"result" yaml:"result"`
	Error  string      `json:"error,omitempty" mapstructure:"error" yaml:"error,omitempty"`
}

func UpdateHeartBeat(s *api.StepRecord, uuid string) error {
	if s.StatusUUID != uuid {
		return fmt.Errorf("cannot update heartbeat for a different status_uuid in order to prevent a race condition")
	}
	res, err := DB.SQL().Exec("update steps set heartbeat=CURRENT_TIMESTAMP where run_id=$1 and \"index\"=$2 and status_uuid=$3", s.RunId, s.Index, s.StatusUUID)
	if err == nil {
		var affected int64
		affected, err = res.RowsAffected()
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

func GetStepsTx(tx *sqlx.Tx, getQuery *api.GetStepsQuery) ([]api.StepRecord, error) {
	var result []api.StepRecord
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(getQuery.ReturnAttributes)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT %s,CURRENT_TIMESTAMP as now FROM steps where uuid IN %s", attributesStr, "('"+strings.Join(getQuery.UUIDs, "','")+"')")

	rows, err = tx.Queryx(query)
	if err != nil {
		panic(fmt.Errorf("failed to query database steps table - get: %w", err))
	}
	defer rows.Close()
	for rows.Next() {
		var step api.StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row - get: %w", err))
		}
		result = append(result, step)
	}
	if result == nil || len(result) != len(getQuery.UUIDs) {
		return nil, ErrRecordNotFound
	}
	return result, nil
}

func GetStepTx(tx *sqlx.Tx, runId string, index int64) (*api.StepRecord, error) {
	var result *api.StepRecord

	const query = "SELECT *,CURRENT_TIMESTAMP as now FROM steps where run_id=$1 and \"index\"=$2"
	var rows *sqlx.Rows
	var err error
	rows, err = tx.Queryx(query, runId, index)
	if err != nil {
		panic(fmt.Errorf("failed to query database steps table - get: %w", err))
	}

	defer rows.Close()
	for rows.Next() {
		var step api.StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row - get: %w", err))
		}
		result = &step
		break
	}
	if result == nil {
		return nil, ErrRecordNotFound
	}
	return result, nil
}

func buildStepsReturnAttributesStrAndVet(attributes []string) (string, error) {
	if attributes == nil || len(attributes) == 0 {
		return "*", nil
	}
	set := make(map[string]bool)
	for _, attribute := range attributes {
		switch attribute {
		case RunId:
		case Index:
		case UUID:
		case Status:
		case StatusUUID:
		case HeartBeat:
		case Label:
		case Name:
		case State:
		case Now:
		default:
			return "", fmt.Errorf("invalid attribute name in return-attributes: %s", attribute)
		}
		set[attribute] = true
	}
	var sb strings.Builder
	first := true
	for str := range set {
		if !first {
			sb.WriteString(",")
		} else {
			first = false
		}
		_, err := sb.WriteString(str)
		if err != nil {
			return "", fmt.Errorf("failed to concatenate return-attribtues")
		}
	}
	return sb.String(), nil
}

func ListStepsTx(tx *sqlx.Tx, query *api.ListQuery) ([]api.StepRecord, *api.RangeResult, error) {
	var result []api.StepRecord
	var rows *sqlx.Rows
	var err error
	var count int64 = -1
	sqlQuery := ""
	sqlNoRange := ""
	params := make([]interface{}, 0)
	sqlQuery = "SELECT *,CURRENT_TIMESTAMP as now FROM steps"
	if query.ReturnAttributes != nil && len(query.ReturnAttributes) > 0 {
		var attributesStr string
		attributesStr, err = buildStepsReturnAttributesStrAndVet(query.ReturnAttributes)
		if err != nil {
			return nil, nil, err
		}
		sqlQuery = fmt.Sprintf("SELECT %s,CURRENT_TIMESTAMP as now FROM steps", attributesStr)
	}

	{
		filterQuery := ""
		for i, expression := range query.Filters {
			queryExpression := ""
			attributeName := expression.AttributeName
			switch expression.AttributeName {
			case RunId:
				attributeName = "run_id"
			case Index:
			case UUID:
			case Status:
			case StatusUUID:
				attributeName = "status_uuid"
			case Label:
			case Name:
			default:
				return nil, nil, fmt.Errorf("invalid attribute name in filter: %s", expression.AttributeName)
			}
			queryExpression = fmt.Sprintf("\"%s\"", attributeName)
			switch expression.Operator {
			case "<":
				fallthrough
			case ">":
				fallthrough
			case "<=":
				fallthrough
			case ">=":
				switch expression.AttributeName {
				case Index:
				default:
					return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += expression.Operator
				queryExpression += fmt.Sprintf("$%d", +i+1)
			case "<>":
				fallthrough
			case "=":
				queryExpression += expression.Operator
				queryExpression += fmt.Sprintf("$%d", +i+1)
			case "startsWith":
				switch expression.AttributeName {
				case Label:
				case Name:
				default:
					return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE $%d || '%%'", i+1)
			case "endsWith":
				switch expression.AttributeName {
				case Label:
				case Name:
				default:
					return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE '%%' || $%d", i+1)
			case "contains":
				switch expression.AttributeName {
				case Label:
				case Name:
				default:
					return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE '%%' || $%d || '%%'", i+1)
			default:
				return nil, nil, fmt.Errorf("invalud operator in filter: %s", expression.Operator)
			}
			params = append(params, expression.Value)
			if filterQuery != "" {
				filterQuery += " AND "
			}
			filterQuery += queryExpression
		}
		if filterQuery != "" {
			sqlQuery += " WHERE " + filterQuery
		}
	}
	{
		if len(query.Sort.Fields) > 0 {
			const orderBy = " ORDER BY "
			sort := orderBy
			for _, field := range query.Sort.Fields {
				fieldDB := field
				switch field {
				case RunId:
					fieldDB = "run_id"
				case Index:
				case UUID:
				case Status:
				case StatusUUID:
					fieldDB = "status_uuid"
				case Label:
				case Name:
				default:
					return nil, nil, fmt.Errorf("invalid attribute name in sort fields: %s", field)
				}
				if sort != orderBy {
					sort += ","
				}
				sort += fmt.Sprintf("\"%s\"", fieldDB)
			}
			switch query.Sort.Order {
			case "desc":
			case "asc":
			default:
				return nil, nil, fmt.Errorf("invalid sort order: %s", query.Sort.Order)
			}
			sqlQuery += sort + " " + query.Sort.Order
		} else {
			sqlQuery += " ORDER BY run_id,\"index\" asc"
		}
	}
	sqlNoRange = sqlQuery
	{
		if query.Range.End >= query.Range.Start && query.Range.Start > 0 {
			offset := query.Range.Start - 1
			limit := query.Range.End - query.Range.Start + 1
			sqlQuery += fmt.Sprintf(" OFFSET %d LIMIT %d", offset, limit)
		}
	}
	if query != nil && query.Range.ReturnTotal {
		err = tx.Get(&count, "select count(*) from ("+sqlNoRange+") C", params...)
		if err != nil {
			panic(fmt.Errorf("failed to query count database steps table: %w", err))
		}
		rows, err = tx.Queryx(sqlQuery, params...)
		if err != nil {
			panic(fmt.Errorf("failed to query database steps table: %w", err))
		}
	} else {
		rows, err = tx.Queryx(sqlQuery, params...)
		if err != nil {
			panic(fmt.Errorf("failed to query database steps table: %w", err))
		}
	}

	defer rows.Close()
	for rows.Next() {
		var step api.StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row: %w", err))
		}
		result = append(result, step)
	}
	{
		rangeResult := api.RangeResult{
			Range: api.Range{
				Start: 0,
				End:   -1,
			},
			Total: count,
		}
		if len(result) > 0 {
			if query != nil && query.Range.End >= query.Range.Start && query.Range.Start > 0 {
				rangeResult.Start = query.Range.Start
			} else {
				rangeResult.Start = 1
			}
			rangeResult.End = rangeResult.Start + int64(len(result)) - 1
		}
		return result, &rangeResult, nil
	}
}

func UpdateStatusAndHeartBeatTx(tx *sqlx.Tx, s *api.StepRecord, newStatus api.StepStatusType) {
	uuid4, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Errorf("failed to generate uuid: %w", err))
	}
	s.StatusUUID = uuid4.String()
	if _, err = tx.Exec("update steps set status=$1, heartbeat=CURRENT_TIMESTAMP where run_id=$2 and \"index\"=$3", newStatus, s.RunId, s.Index); err != nil {
		panic(err)
	}
}

func UpdateStateAndStatusAndHeartBeatTx(tx *sqlx.Tx, s *api.StepRecord, newStatus api.StepStatusType, newState *StepState) {
	uuid4, err := uuid.NewRandom()
	if err != nil {
		panic(fmt.Errorf("failed to generate uuid: %w", err))
	}
	newStateStr, err := json.Marshal(newState)
	if err != nil {
		panic(err)
	}
	s.StatusUUID = uuid4.String()
	if _, err = tx.Exec("update steps set status=$1, state=$2, heartbeat=CURRENT_TIMESTAMP where run_id=$3 and \"index\"=$4", newStatus, newStateStr, s.RunId, s.Index); err != nil {
		panic(err)
	}
}
