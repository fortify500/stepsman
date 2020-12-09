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
	"github.com/jmoiron/sqlx"
	"strings"
)

func (d *DAO) UpdateStepHeartBeat(stepUUID string, statusOwner string) error {
	res, err := d.DB.SQL().Exec("update steps set heartbeat=CURRENT_TIMESTAMP where uuid=$1 and status_owner=$2", stepUUID, statusOwner)
	if err == nil {
		var affected int64
		affected, err = res.RowsAffected()
		if err != nil {
			panic(err)
		}
		if affected != 1 {
			return api.NewError(api.ErrRecordNotAffected, "while updating step heartbeat, no rows where affected, suggesting status-owner has changed (but possibly the record have been deleted) for step uuid: %s, and status owner: %s", stepUUID, statusOwner)
		}
	}
	return nil
}
func (d *DAO) UpdateStepPartsTx(tx *sqlx.Tx, runId string, index int64, newStatus api.StepStatusType, newStatusOwner string, completeBy *int64, retriesLeft *int, context api.Context, state *api.State) UUIDAndStatusOwner {
	updated := d.UpdateManyStepsPartsBeatTx(tx, runId, []int64{index}, newStatus, newStatusOwner, nil, completeBy, retriesLeft, context, state)
	if len(updated) != 1 {
		panic(fmt.Errorf("illegal state, 1 updated record expected for runId:%s and index:%d", runId, index))
	}
	return updated[0]
}

type UUIDAndStatusOwner struct {
	UUID        string
	StatusOwner string
}

//func UpdateStepStatusAndHeartBeatByUUIDTx(tx *sqlx.Tx, uuid string, newStatus api.StepStatusType, completeBy *int64) UUIDAndStatusOwner {
//	updated := DB.UpdateManyStatusAndHeartBeatByUUIDTx(tx, []string{uuid}, newStatus, nil, completeBy)
//	if len(updated) != 1 {
//		panic(fmt.Errorf("illegal state, 1 updated record expected for uuid:%s", uuid))
//	}
//	return updated[0]
//}

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

func GetStepByLabelTx(tx *sqlx.Tx, runId string, label string, attributes []string) (api.StepRecord, error) {
	var result api.StepRecord
	attributesStr, err := buildStepsReturnAttributesStrAndVet(attributes)
	if err != nil {
		return api.StepRecord{}, err
	}
	query := "SELECT %s,CURRENT_TIMESTAMP as now FROM steps where run_id=$1 and label=$2"
	query = fmt.Sprintf(query, attributesStr)
	var rows *sqlx.Rows
	rows, err = tx.Queryx(query, runId, label)
	if err != nil {
		panic(fmt.Errorf("failed to query database steps table - get: %w", err))
	}

	var found = false
	defer rows.Close()
	for rows.Next() {
		found = true
		var step api.StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row - get: %w", err))
		}
		result = step
		break
	}
	if !found {
		return api.StepRecord{}, api.NewError(api.ErrRecordNotFound, "failed to get step record, no record found")
	}
	return result, nil
}

func GetStepTx(tx *sqlx.Tx, runId string, index int64, attributes []string) (api.StepRecord, error) {
	var result api.StepRecord
	attributesStr, err := buildStepsReturnAttributesStrAndVet(attributes)
	if err != nil {
		return api.StepRecord{}, err
	}
	query := "SELECT %s,CURRENT_TIMESTAMP as now FROM steps where run_id=$1 and \"index\"=$2"
	query = fmt.Sprintf(query, attributesStr)
	var rows *sqlx.Rows
	rows, err = tx.Queryx(query, runId, index)
	if err != nil {
		panic(fmt.Errorf("failed to query database steps table - get: %w", err))
	}

	found := false
	defer rows.Close()
	for rows.Next() {
		found = true
		var step api.StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			panic(fmt.Errorf("failed to parse database steps row - get: %w", err))
		}
		result = step
		break
	}
	if !found {
		return api.StepRecord{}, api.NewError(api.ErrRecordNotFound, "failed to get step record, no record found")
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
			attribute = "\"index\""
		case UUID:
		case Status:
		case StatusOwner:
		case RetriesLeft:
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
func (d *DAO) UpdateManyStatusAndHeartBeatByUUIDTx(tx *sqlx.Tx, uuids []string, newStatus api.StepStatusType, newStatusOwner string, prevStatus []api.StepStatusType, context api.Context, completeBy *int64) []UUIDAndStatusOwner {
	var indicesUuids []indicesUUIDs
	for _, stepUUID := range uuids {
		indicesUuids = append(indicesUuids, indicesUUIDs{
			uuid: stepUUID,
		})
	}
	return d.updateManyStepsPartsTxInternal(tx, indicesUuids, newStatus, newStatusOwner, prevStatus, completeBy, nil, context, nil)
}
func (d *DAO) UpdateManyStepsPartsBeatTx(tx *sqlx.Tx, runId string, indices []int64, newStatus api.StepStatusType, newStatusOwner string, prevStatus []api.StepStatusType, completeBy *int64, retriesLeft *int, context api.Context, state *api.State) []UUIDAndStatusOwner {
	var indicesUuids []indicesUUIDs
	for _, index := range indices {
		indicesUuids = append(indicesUuids, indicesUUIDs{
			runId: runId,
			index: index,
		})
	}
	return d.updateManyStepsPartsTxInternal(tx, indicesUuids, newStatus, newStatusOwner, prevStatus, completeBy, retriesLeft, context, state)
}

func (d *DAO) UpdateManyStatusAndHeartBeatByLabelTx(tx *sqlx.Tx, runId string, labels []string, newStatus api.StepStatusType, newStatusOwner string, prevStatus []api.StepStatusType, context api.Context, completeBy *int64) []UUIDAndStatusOwner {
	var indicesUuids []indicesUUIDs
	for _, label := range labels {
		indicesUuids = append(indicesUuids, indicesUUIDs{
			runId: runId,
			label: label,
		})
	}
	return d.updateManyStepsPartsTxInternal(tx, indicesUuids, newStatus, newStatusOwner, prevStatus, completeBy, nil, context, nil)
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
		var i int
		for _, expression := range query.Filters {
			i++
			queryExpression := ""
			attributeName := expression.AttributeName
			switch expression.AttributeName {
			case RunId:
				attributeName = "run_id"
			case Index:
			case UUID:
			case Status:
			case StatusOwner:
				attributeName = "status_owner"
			case Label:
			case Name:
			case Tags:
			case HeartBeat:
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
				case HeartBeat:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += expression.Operator
				queryExpression += fmt.Sprintf("$%d", i)
			case "<>":
				fallthrough
			case "=":
				queryExpression += expression.Operator
				queryExpression += fmt.Sprintf("$%d", i)
			case "startsWith":
				switch expression.AttributeName {
				case Label:
				case Name:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE $%d || '%%'", i)
			case "endsWith":
				switch expression.AttributeName {
				case Label:
				case Name:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				queryExpression += fmt.Sprintf(" LIKE '%%' || $%d", i)
			case "contains":
				switch expression.AttributeName {
				case Label:
				case Name:
				case Tags:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				switch expression.AttributeName {
				case Tags:
					var ok bool
					var tags []string
					{
						var tagsInterface []interface{}
						tagsInterface, ok = expression.Value.([]interface{})
						if !ok {
							return nil, nil, api.NewError(api.ErrInvalidParams, "invalid tags attribute value in filter: %s - %s", expression.AttributeName, expression.Value)
						}
						var tag string
						for _, v := range tagsInterface {
							tag, ok = v.(string)
							if !ok {
								return nil, nil, api.NewError(api.ErrInvalidParams, "invalid tags attribute value in filter: %s - %s", expression.AttributeName, expression.Value)
							}
							tags = append(tags, tag)
						}
					}
					if len(tags) == 0 {
						return nil, nil, api.NewError(api.ErrInvalidParams, "invalid tags attribute value in filter: %s - %s", expression.AttributeName, expression.Value)
					}
					queryExpression += fmt.Sprintf(" @> $%d::jsonb", i)
				default:
					queryExpression += fmt.Sprintf(" LIKE '%%' || $%d || '%%'", i)
				}
			case "exists":
				switch expression.AttributeName {
				case Tags:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
				}
				var ok bool
				var tags []string
				{
					var tagsInterface []interface{}
					tagsInterface, ok = expression.Value.([]interface{})
					if !ok {
						return nil, nil, api.NewError(api.ErrInvalidParams, "invalid tags attribute value in filter: %s - %s", expression.AttributeName, expression.Value)
					}
					var tag string
					for _, v := range tagsInterface {
						tag, ok = v.(string)
						if !ok {
							return nil, nil, api.NewError(api.ErrInvalidParams, "invalid tags attribute value in filter: %s - %s", expression.AttributeName, expression.Value)
						}
						tags = append(tags, tag)
					}
				}
				if len(tags) == 0 {
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid tags attribute value in filter: %s - %s", expression.AttributeName, expression.Value)
				}
				var sb strings.Builder
				sb.WriteString(" ?| array[")
				for j := range tags {
					if j > 0 {
						i++
						sb.WriteString(fmt.Sprintf(",$%d", i))
					} else {
						sb.WriteString(fmt.Sprintf("$%d", i))
					}
				}
				sb.WriteString("]")
				queryExpression += sb.String()
			default:
				return nil, nil, api.NewError(api.ErrInvalidParams, "invalid operator in filter: %s", expression.Operator)
			}
			switch expression.AttributeName {
			case Tags:
				switch expression.Operator {
				case "contains":
					var tagsJson []byte
					tagsJson, err = json.Marshal(expression.Value)
					if err != nil {
						panic(err)
					}
					params = append(params, tagsJson)
				case "exists":
					params = append(params, expression.Value.([]interface{})...)
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid tags attribute value in filter: %s - %s", expression.AttributeName, expression.Value)
				}
			default:
				params = append(params, expression.Value)
			}
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
				case StatusOwner:
					fieldDB = "status_owner"
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
