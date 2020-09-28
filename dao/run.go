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

func ListRuns(query *Query) ([]*RunRecord, *RangeResult, error) {
	var result []*RunRecord
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
		{
			filterQuery := ""
			for i, expression := range query.Filters {
				queryExpression := ""
				switch expression.AttributeName {
				case "id":
				case "uuid":
				case "title":
				case "cursor":
				case "status":
				default:
					return nil, nil, fmt.Errorf("invalid attribute name in filter: %s", expression.AttributeName)
				}
				queryExpression = expression.AttributeName
				switch expression.Operator {
				case "<":
					fallthrough
				case ">":
					fallthrough
				case "<=":
					fallthrough
				case ">=":
					switch expression.AttributeName {
					case "id":
					case "cursor":
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
					case "title":
					default:
						return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE $%d || '%%'", i+1)
				case "endsWith":
					switch expression.AttributeName {
					case "title":
					default:
						return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE '%%' || $%d", i+1)
				case "contains":
					switch expression.AttributeName {
					case "title":
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
					switch field {
					case "id":
					case "uuid":
					case "title":
					case "cursor":
					case "status":
					default:
						return nil, nil, fmt.Errorf("invalid attribute name in sort fields: %s", field)
					}
					if sort != orderBy {
						sort += ","
					}
					sort += field
				}
				switch query.Sort.Order {
				case "desc":
				case "asc":
				default:
					return nil, nil, fmt.Errorf("invalid sort order: %s", query.Sort.Order)
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
		tx, err := DB.SQL().Beginx()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query database runs table: %w", err)
		}
		err = tx.Get(&count, "select count(*) from ("+sqlNoRange+") C", params...)
		if err != nil {
			err = Rollback(tx, err)
			return nil, nil, fmt.Errorf("failed to query count database run table: %w", err)
		}
		rows, err = tx.Queryx(sqlQuery, params...)
		if err != nil {
			err = Rollback(tx, err)
			return nil, nil, fmt.Errorf("failed to query database run table: %w", err)
		}

		defer tx.Commit()
	} else {
		rows, err = DB.SQL().Queryx(sqlQuery, params...)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query database runs table: %w", err)
	}

	for rows.Next() {
		var run RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse database runs row: %w", err)
		}
		result = append(result, &run)
	}
	{
		rangeResult := RangeResult{
			Range: Range{
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
