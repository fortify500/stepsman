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

func ListRunsTx(tx *sqlx.Tx, query *api.ListQuery) ([]api.RunRecord, *api.RangeResult, error) {
	var result []api.RunRecord
	var rows *sqlx.Rows
	var err error
	var count int64 = -1
	sqlQuery := ""
	sqlNoRange := ""
	params := make([]interface{}, 0)
	if query == nil {
		sqlNoRange = "SELECT * FROM runs ORDER BY id DESC"
		sqlQuery = sqlNoRange + " LIMIT 20"
	} else {
		sqlQuery = "SELECT * FROM runs"
		if query.ReturnAttributes != nil && len(query.ReturnAttributes) > 0 {
			var attributesStr string
			attributesStr, err = buildRunsReturnAttributesStrAndVet(false, query.ReturnAttributes)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to list runs: %w", err)
			}
			sqlQuery = fmt.Sprintf("SELECT %s FROM runs", attributesStr)
		}

		{
			filterQuery := ""
			for i, expression := range query.Filters {
				queryExpression := ""
				attributeName := expression.AttributeName
				switch expression.AttributeName {
				case Id:
				case Key:
				case CreatedAt:
				case TemplateVersion:
				case TemplateTitle:
				case Status:
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
					queryExpression += fmt.Sprintf("$%d", +i+1)
				case "<>":
					fallthrough
				case "=":
					queryExpression += expression.Operator
					queryExpression += fmt.Sprintf("$%d", +i+1)
				case "startsWith":
					switch expression.AttributeName {
					case Key:
					case TemplateTitle:
					default:
						return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE $%d || '%%'", i+1)
				case "endsWith":
					switch expression.AttributeName {
					case Key:
					case TemplateTitle:
					default:
						return nil, nil, api.NewError(api.ErrInvalidParams, "invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE '%%' || $%d", i+1)
				case "contains":
					switch expression.AttributeName {
					case Key:
					case TemplateTitle:
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

func CreateRunTx(tx *sqlx.Tx, runRecord interface{}) {
	if _, err := tx.NamedExec("INSERT INTO runs(id, key, template_version, template_title, status, created_at, template) values(:id,:key,:template_version,:template_title,:status,CURRENT_TIMESTAMP,:template)", runRecord); err != nil {
		panic(err)
	}
}
func GetRunAndStepUUIDByLabelTx(tx *sqlx.Tx, runId string, label string, runReturnAttributes []string) (api.RunRecord, string, error) {
	var result []api.RunRecord
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(true, runReturnAttributes)
	if err != nil {
		return api.RunRecord{}, "", fmt.Errorf("failed to get run transactional: %w", err)
	}
	query := fmt.Sprintf("SELECT %s, steps.uuid as step_uuid FROM runs inner join steps on runs.id=steps.run_id where runs.id=$1 AND steps.label = $2", attributesStr)
	rows, err = tx.Queryx(query, runId, label)
	if err != nil {
		panic(fmt.Errorf("failed to query database runs table - get: %w", err))
	}
	stepUUIDStruct := struct {
		StepUUID string `db:"step_uuid"`
		api.RunRecord
	}{}
	defer rows.Close()
	for rows.Next() {
		err = rows.StructScan(&stepUUIDStruct)
		if err != nil {
			panic(fmt.Errorf("failed to parse database runs row - get: %w", err))
		}
		if stepUUIDStruct.StepUUID == "" {
			panic(fmt.Errorf("failed to parse step uuid when retrieving runs row"))
		}
		result = append(result, stepUUIDStruct.RunRecord)
	}
	if result == nil || len(result) != 1 {
		return api.RunRecord{}, "", api.NewError(api.ErrRecordNotFound, "failed to get run record, no record found")
	}
	return result[0], stepUUIDStruct.StepUUID, nil
}
func GetRunsByStepUUIDsTx(tx *sqlx.Tx, getQuery *api.GetRunsQuery) ([]api.RunRecord, []api.Context, error) {
	var result []api.RunRecord
	var contexts []api.Context
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(true, getQuery.ReturnAttributes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get run transactional: %w", err)
	}
	queryStr := fmt.Sprintf("SELECT %s, steps.context FROM runs inner join steps on runs.id=steps.run_id where steps.uuid IN (?)", attributesStr)
	var query string
	var args []interface{}
	query, args, err = sqlx.In(queryStr, getQuery.Ids)
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
		contextAndRunStruct := struct {
			Context api.Context `db:"context"`
			api.RunRecord
		}{}
		err = rows.StructScan(&contextAndRunStruct)
		if err != nil {
			panic(fmt.Errorf("failed to parse database runs row - get: %w", err))
		}
		result = append(result, contextAndRunStruct.RunRecord)
		contexts = append(contexts, contextAndRunStruct.Context)
	}
	if result == nil || len(result) != len(getQuery.Ids) {
		return nil, nil, api.NewError(api.ErrRecordNotFound, "failed to get run record, no record found")
	}
	return result, contexts, nil
}

func GetRunsTx(tx *sqlx.Tx, getQuery *api.GetRunsQuery) ([]api.RunRecord, error) {
	var result []api.RunRecord
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(false, getQuery.ReturnAttributes)
	if err != nil {
		return nil, fmt.Errorf("failed to get run transactional: %w", err)
	}
	queryStr := fmt.Sprintf("SELECT %s FROM runs where id IN (?)", attributesStr)
	var query string
	var args []interface{}
	query, args, err = sqlx.In(queryStr, getQuery.Ids)
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
		query, args, err = sqlx.In("SELECT COUNT(*) FROM RUNS WHERE status<>? AND id IN (?)", api.RunInProgress, deleteRunsQuery.Ids)
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

	query, args, err = sqlx.In("DELETE FROM steps WHERE run_id IN (?)", deleteRunsQuery.Ids)
	if err != nil {
		panic(err)
	}
	query = tx.Rebind(query)
	_, err = tx.Exec(query, args...)
	if err != nil {
		panic(fmt.Errorf("failed to delete runs from database: %w", err))
	}

	query, args, err = sqlx.In("DELETE FROM runs where id IN (?)", deleteRunsQuery.Ids)
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

func UpdateRunStatusTx(tx *sqlx.Tx, id string, newStatus api.RunStatusType) {
	if _, err := tx.Exec("update runs set status=$1 where id=$2", newStatus, id); err != nil {
		panic(err)
	}
}
