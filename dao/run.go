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
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"strings"
)

func ListRunsTx(tx *sqlx.Tx, query *api.ListQuery) ([]api.RunRecord, *api.RangeResult, error) {
	var result []api.RunRecord
	var rows *sqlx.Rows
	var err error
	var count int64 = -1
	sqlQuery := ""
	sqlNoRange := ""
	params := make([]interface{}, 0)
	if query == nil {
		sqlNoRange = "SELECT *,CURRENT_TIMESTAMP as now FROM runs ORDER BY id DESC"
		sqlQuery = sqlNoRange + " LIMIT 20"
	} else {
		sqlQuery = "SELECT *,CURRENT_TIMESTAMP as now FROM runs"
		if query.ReturnAttributes != nil && len(query.ReturnAttributes) > 0 {
			var attributesStr string
			attributesStr, err = buildRunsReturnAttributesStrAndVet(false, query.ReturnAttributes)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to list runs: %w", err)
			}
			sqlQuery = fmt.Sprintf("SELECT %s,CURRENT_TIMESTAMP as now FROM runs", attributesStr)
		}

		{
			var i int
			i++
			filterQuery := fmt.Sprintf("group_id = $%d", i)
			params = append(params, query.Options.GroupId)
			for _, expression := range query.Filters {
				i++
				queryExpression := ""
				attributeName := expression.AttributeName
				switch expression.AttributeName {
				case Id:
				case Key:
				case CreatedAt:
				case TemplateVersion:
				case TemplateTitle:
				case Status:
				case Tags:
				default:
					return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name in filter: %s", expression.AttributeName)
				}
				attributeName = strings.ReplaceAll(attributeName, "-", "_")
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
					case TemplateVersion:
					case CreatedAt:
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
					case Key:
					case TemplateTitle:
					default:
						return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE $%d || '%%'", i)
				case "endsWith":
					switch expression.AttributeName {
					case Key:
					case TemplateTitle:
					default:
						return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE '%%' || $%d", i)
				case "contains":
					switch expression.AttributeName {
					case Key:
					case TemplateTitle:
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
						var sb strings.Builder
						sb.WriteString(" @> array[")
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
					sb.WriteString(" && array[")
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
						fallthrough
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
					case Id:
					case Key:
					case CreatedAt:
					case TemplateVersion:
					case TemplateTitle:
					case Status:
					default:
						return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name in sort fields: %s", field)
					}
					fieldDB = strings.ReplaceAll(fieldDB, "-", "_")
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
				sqlQuery += " ORDER BY id DESC"
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
	}
	if query != nil && query.Range.ReturnTotal {
		err = tx.Get(&count, "select count(*) from ("+sqlNoRange+") C", params...)
		if err != nil {
			panic(fmt.Errorf("failed to query count database run table: %w", err))
		}
		rows, err = tx.Queryx(sqlQuery, params...)
		if err != nil {
			panic(fmt.Errorf("failed to query database run table: %w", err))
		}
	} else {
		rows, err = tx.Queryx(sqlQuery, params...)
		if err != nil {
			panic(fmt.Errorf("failed to query database runs table: %w", err))
		}
	}

	defer rows.Close()
	for rows.Next() {
		var run api.RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			panic(fmt.Errorf("failed to parse database runs row: %w", err))
		}
		result = append(result, run)
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

func buildRunsReturnAttributesStrAndVet(addPrefix bool, attributes []string) (string, error) {
	if attributes == nil || len(attributes) == 0 {
		return "*", nil
	}
	set := make(map[string]bool)
	for _, attribute := range attributes {
		switch attribute {
		case Id:
		case Key:
		case TemplateVersion:
		case TemplateTitle:
		case CreatedAt:
		case Status:
		case Template:
		default:
			return "", api.NewError(api.ErrInvalidParams, "invalid attribute name in return-attributes: %s", attribute)
		}
		set[strings.ReplaceAll(attribute, "-", "_")] = true
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
		if addPrefix {
			_, err := sb.WriteString("runs.")
			if err != nil {
				panic(fmt.Errorf("failed to concatenate return-attribtues"))
			}
		}
		_, err := sb.WriteString(str)
		if err != nil {
			panic(fmt.Errorf("failed to concatenate return-attribtues"))
		}
	}
	return sb.String(), nil
}

func GetRunAndStepUUIDByLabelTx(tx *sqlx.Tx, options api.Options, runId uuid.UUID, label string, runReturnAttributes []string) (api.RunRecord, uuid.UUID, api.Context, error) {
	var result []api.RunRecord
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(true, runReturnAttributes)
	if err != nil {
		return api.RunRecord{}, uuid.UUID{}, nil, fmt.Errorf("failed to get run transactional: %w", err)
	}
	query := fmt.Sprintf("SELECT %s,CURRENT_TIMESTAMP as now, steps.uuid as step_uuid, steps.context FROM runs inner join steps on (runs.group_id=steps.group_id and runs.created_at=steps.created_at and runs.id=steps.run_id) where runs.group_id=$1 AND runs.id=$2 AND steps.label = $3", attributesStr)
	rows, err = tx.Queryx(query, options.GroupId, runId, label)
	if err != nil {
		panic(fmt.Errorf("failed to query database runs table - get: %w", err))
	}
	stepUUIDStruct := struct {
		StepUUID uuid.UUID   `db:"step_uuid"`
		Context  api.Context `db:"context"`
		api.RunRecord
	}{}
	defer rows.Close()
	for rows.Next() {
		err = rows.StructScan(&stepUUIDStruct)
		if err != nil {
			panic(fmt.Errorf("failed to parse database runs row - get: %w", err))
		}
		if stepUUIDStruct.StepUUID == (uuid.UUID{}) {
			panic(fmt.Errorf("failed to parse step uuid when retrieving runs row"))
		}
		result = append(result, stepUUIDStruct.RunRecord)
	}
	if result == nil || len(result) != 1 {
		return api.RunRecord{}, uuid.UUID{}, nil, api.NewError(api.ErrRecordNotFound, "failed to get run record, no record found")
	}
	return result[0], stepUUIDStruct.StepUUID, stepUUIDStruct.Context, nil
}
func GetRunLabelAndContextByStepUUIDTx(tx *sqlx.Tx, options api.Options, stepUUID uuid.UUID, runReturnAttributes []string) (api.RunRecord, api.Context, string, error) {
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(true, runReturnAttributes)
	if err != nil {
		return api.RunRecord{}, nil, "", fmt.Errorf("failed to get run transactional: %w", err)
	}
	query := fmt.Sprintf("SELECT %s,CURRENT_TIMESTAMP as now, steps.context, steps.label FROM runs inner join steps on (runs.group_id=steps.group_id and runs.created_at=steps.created_at and runs.id=steps.run_id) where runs.group_id=$1 AND steps.uuid=$2", attributesStr)
	rows, err = tx.Queryx(query, options.GroupId, stepUUID)
	if err != nil {
		panic(fmt.Errorf("failed to query database runs table - get: %w", err))
	}
	contextAndRunStruct := struct {
		Context api.Context `db:"context"`
		Label   string      `db:"label"`
		api.RunRecord
	}{}
	i := 0
	defer rows.Close()
	for rows.Next() {
		err = rows.StructScan(&contextAndRunStruct)
		if err != nil {
			panic(fmt.Errorf("failed to parse database runs row - get: %w", err))
		}
		i++
		break
	}
	if i != 1 {
		return api.RunRecord{}, nil, "", api.NewError(api.ErrRecordNotFound, "failed to get run record, no record found")
	}
	return contextAndRunStruct.RunRecord, contextAndRunStruct.Context, contextAndRunStruct.Label, nil
}

func GetRunsTx(tx *sqlx.Tx, getQuery *api.GetRunsQuery) ([]api.RunRecord, error) {
	var result []api.RunRecord
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(false, getQuery.ReturnAttributes)
	if err != nil {
		return nil, fmt.Errorf("failed to get run transactional: %w", err)
	}
	queryStr := fmt.Sprintf("SELECT %s,CURRENT_TIMESTAMP as now FROM runs where group_id=? AND id IN (?)", attributesStr)
	var query string
	var args []interface{}
	query, args, err = sqlx.In(queryStr, getQuery.Options.GroupId, getQuery.Ids)
	if err != nil {
		panic(err)
	}
	query = tx.Rebind(query)
	rows, err = tx.Queryx(query, args...)
	if err != nil {
		panic(fmt.Errorf("failed to query database runs table - get: %w", err))
	}
	defer rows.Close()
	for rows.Next() {
		var run api.RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			panic(fmt.Errorf("failed to parse database runs row - get: %w", err))
		}
		result = append(result, run)
	}
	if result == nil || len(result) != len(getQuery.Ids) {
		return nil, api.NewError(api.ErrRecordNotFound, "failed to get run record, no record found")
	}
	return result, nil
}

func DeleteRunsTx(tx *sqlx.Tx, deleteRunsQuery *api.DeleteQuery) error {
	var err error
	var query string
	var args []interface{}

	if !deleteRunsQuery.Force {
		query, args, err = sqlx.In("SELECT COUNT(*) FROM RUNS WHERE group_id=? AND status<>? AND id IN (?)", deleteRunsQuery.Options.GroupId, api.RunInProgress, deleteRunsQuery.Ids)
		if err != nil {
			panic(err)
		}
		query = tx.Rebind(query)
		var count int64
		err = tx.Get(&count, query, args...)
		if err != nil {
			panic(fmt.Errorf("failed to delete runs from database: %w", err))
		}
		if count != int64(len(deleteRunsQuery.Ids)) {
			return api.NewError(api.ErrRecordNotFound, "failed to delete run record, some records where not found or where in progress")
		}
	}

	query, args, err = sqlx.In("DELETE FROM steps WHERE group_id=? AND run_id IN (?)", deleteRunsQuery.Options.GroupId, deleteRunsQuery.Ids)
	if err != nil {
		panic(err)
	}
	query = tx.Rebind(query)
	_, err = tx.Exec(query, args...)
	if err != nil {
		panic(fmt.Errorf("failed to delete runs from database: %w", err))
	}

	query, args, err = sqlx.In("DELETE FROM runs where group_id=? and id IN (?)", deleteRunsQuery.Options.GroupId, deleteRunsQuery.Ids)
	if err != nil {
		panic(err)
	}
	query = tx.Rebind(query)
	_, err = tx.Exec(query, args...)
	if err != nil {
		panic(fmt.Errorf("failed to delete runs from database: %w", err))
	}

	return nil
}

func (d *DAO) UpdateRunStatusTx(tx *sqlx.Tx, options api.Options, id uuid.UUID, newStatus api.RunStatusType, prevStatus *api.RunStatusType, completeBy *int64) {
	completeByStr := ""
	if completeBy != nil {
		if *completeBy == -1 {
			completeByStr = ",complete_by=NULL"
		} else {
			completeByStr = d.DB.completeByUpdateStatement(completeBy)
		}
	}
	if prevStatus != nil {
		if _, err := tx.Exec(fmt.Sprintf("update runs set status=$1%s where group_id=$2 and id=$3 and status=$4", completeByStr), newStatus, options.GroupId, id, *prevStatus); err != nil {
			panic(err)
		}
	} else {
		if _, err := tx.Exec(fmt.Sprintf("update runs set status=$1%s where group_id=$2 and id=$3", completeByStr), newStatus, options.GroupId, id); err != nil {
			panic(err)
		}
	}
}

func ResetRunCompleteByTx(tx *sqlx.Tx, id uuid.UUID) {
	if _, err := tx.Exec("update runs set complete_by=NULL where id=$1", id); err != nil {
		panic(err)
	}
}
