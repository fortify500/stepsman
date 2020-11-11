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
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/jmoiron/sqlx"
	"strings"
)

type StepState struct {
	DoType string      `json:"do-type,omitempty" mapstructure:"do-type" yaml:"do-type,omitempty"`
	Result interface{} `json:"result,omitempty" mapstructure:"result" yaml:"result"`
	Error  string      `json:"error,omitempty" mapstructure:"error" yaml:"error,omitempty"`
}

func UpdateStepHeartBeat(stepUUID string, statusUUID string) error {
	res, err := DB.SQL().Exec("update steps set heartbeat=CURRENT_TIMESTAMP where uuid=$1 and status_uuid=$2", stepUUID, statusUUID)
	if err == nil {
		var affected int64
		affected, err = res.RowsAffected()
		if err != nil {
			panic(err)
		}
		if affected != 1 {
			return api.NewError(api.ErrRecordNotAffected, "while updating step heartbeat, no rows where affected, suggesting status_uuid has changed (but possibly the record have been deleted) for step uuid: %s, and status uuid: %s", stepUUID, statusUUID)
		}
	}
	return nil
}
func UpdateStepStatusAndHeartBeatTx(tx *sqlx.Tx, runId string, index int64, newStatus api.StepStatusType, completeBy *int64) UUIDAndStatusUUID {
	updated := DB.UpdateManyStatusAndHeartBeatTx(tx, runId, []int64{index}, newStatus, nil, completeBy)
	if len(updated) != 1 {
		panic(fmt.Errorf("illegal state, 1 updated record expected for runId:%s and index:%d", runId, index))
	}
	return updated[0]
}

type UUIDAndStatusUUID struct {
	UUID       string
	StatusUUID string
}

func UpdateStepStatusAndHeartBeatByUUIDTx(tx *sqlx.Tx, uuid string, newStatus api.StepStatusType, completeBy *int64) UUIDAndStatusUUID {
	updated := DB.UpdateManyStatusAndHeartBeatByUUIDTx(tx, []string{uuid}, newStatus, nil, completeBy)
	if len(updated) != 1 {
		panic(fmt.Errorf("illegal state, 1 updated record expected for uuid:%s", uuid))
	}
	return updated[0]
}

func GetStepsTx(tx *sqlx.Tx, getQuery *api.GetStepsQuery) ([]api.StepRecord, error) {
	var result []api.StepRecord
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildStepsReturnAttributesStrAndVet(getQuery.ReturnAttributes)
	if err != nil {
		return nil, fmt.Errorf("failed to get steps transcational: %w", err)
	}
	queryStr := fmt.Sprintf("SELECT %s,CURRENT_TIMESTAMP as now FROM steps where uuid IN (?)", attributesStr)

	var query string
	var args []interface{}
	query, args, err = sqlx.In(queryStr, getQuery.UUIDs)
	if err != nil {
		panic(err)
	}
	query = tx.Rebind(query)
	rows, err = tx.Queryx(query, args...)
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
		return nil, api.NewError(api.ErrRecordNotFound, "failed to get steps records, at least one record is missing")
	}
	return result, nil
}

func GetStepTx(tx *sqlx.Tx, runId string, index int64, attributes []string) (*api.StepRecord, error) {
	var result *api.StepRecord
	attributesStr, err := buildStepsReturnAttributesStrAndVet(attributes)
	if err != nil {
		return nil, err
	}
	query := "SELECT %s,CURRENT_TIMESTAMP as now FROM steps where run_id=$1 and \"index\"=$2"
	query = fmt.Sprintf(query, attributesStr)
	var rows *sqlx.Rows
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
		return nil, api.NewError(api.ErrRecordNotFound, "failed to get step record, no record found")
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
		case CompleteBy:
		case Label:
		case Name:
		case State:
		case Now:
		default:
			return "", api.NewError(api.ErrInvalidParams, "invalid attribute name in return-attributes: %s", attribute)
		}
		set[strings.ReplaceAll(attribute, "-", "_")] = true
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
				return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name in filter: %s", expression.AttributeName)
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
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
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
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE $%d || '%%'", i+1)
			case "endsWith":
				switch expression.AttributeName {
				case Label:
				case Name:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE '%%' || $%d", i+1)
			case "contains":
				switch expression.AttributeName {
				case Label:
				case Name:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE '%%' || $%d || '%%'", i+1)
			default:
				return nil, nil, api.NewError(api.ErrInvalidParams, "invalid operator in filter: %s", expression.Operator)
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
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name in sort fields: %s", field)
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
				return nil, nil, api.NewError(api.ErrInvalidParams, "invalid sort order: %s", query.Sort.Order)
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
