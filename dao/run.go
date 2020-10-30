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
	"github.com/fortify500/stepsman/api"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
	"log"
	"strings"
)

type RunStatusType int64

const (
	RunIdle       RunStatusType = 10
	RunInProgress RunStatusType = 12
	RunDone       RunStatusType = 15
)

type RunRecord struct {
	Id              string
	Key             string
	TemplateVersion int64  `db:"template_version"`
	TemplateTitle   string `db:"template_title"`
	Status          RunStatusType
	Template        string
}

func (r *RunRecord) PrettyJSONTemplate() (string, error) {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(r.Template)))
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
func (r *RunRecord) PrettyYamlTemplate() (string, error) {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(r.Template)))
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
func ListRunsTx(tx *sqlx.Tx, query *api.ListQuery) ([]*RunRecord, *api.RangeResult, error) {
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
		if query.ReturnAttributes != nil && len(query.ReturnAttributes) > 0 {
			attributesStr, err := buildRunsReturnAttributesStrAndVet(query.ReturnAttributes)
			if err != nil {
				return nil, nil, err
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
				case TemplateVersion:
					attributeName = "template_version"
				case TemplateTitle:
					attributeName = "template_title"
				case Status:
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
					case TemplateVersion:
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
					case Key:
					case TemplateTitle:
					default:
						return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE $%d || '%%'", i+1)
				case "endsWith":
					switch expression.AttributeName {
					case Key:
					case TemplateTitle:
					default:
						return nil, nil, fmt.Errorf("invalid attribute name and operator combination in filter: %s - %s", expression.AttributeName, expression.Operator)
					}
					queryExpression += fmt.Sprintf(" LIKE '%%' || $%d", i+1)
				case "contains":
					switch expression.AttributeName {
					case Key:
					case TemplateTitle:
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
					case Id:
					case Key:
					case TemplateVersion:
						fieldDB = "template_version"
					case TemplateTitle:
						fieldDB = "template_title"
					case Status:
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
			return nil, nil, fmt.Errorf("failed to query count database run table: %w", err)
		}
		rows, err = tx.Queryx(sqlQuery, params...)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to query database run table: %w", err)
		}
	} else {
		rows, err = tx.Queryx(sqlQuery, params...)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to query database runs table: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var run RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse database runs row: %w", err)
		}
		result = append(result, &run)
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

func buildRunsReturnAttributesStrAndVet(attributes []string) (string, error) {
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
		case Status:
		case Template:
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

func CreateRunTx(tx *sqlx.Tx, runRecord interface{}) error {
	_, err := tx.NamedExec("INSERT INTO runs(id, key, template_version, template_title, status, template) values(:id,:key,:template_version,:template_title,:status,:template)", runRecord)
	return err
}

func GetRunsTx(tx *sqlx.Tx, getQuery *api.GetRunsQuery) ([]*RunRecord, error) {
	var result []*RunRecord
	var rows *sqlx.Rows
	var err error
	attributesStr, err := buildRunsReturnAttributesStrAndVet(getQuery.ReturnAttributes)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf("SELECT %s FROM runs where id IN %s", attributesStr, "('"+strings.Join(getQuery.Ids, "','")+"')")

	rows, err = tx.Queryx(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query database runs table - get: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var run RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database runs row - get: %w", err)
		}
		result = append(result, &run)
	}
	if result == nil || len(result) != len(getQuery.Ids) {
		return nil, ErrRecordNotFound
	}
	return result, nil
}

func UpdateRunStatusTx(tx *sqlx.Tx, id string, newStatus RunStatusType) (sql.Result, error) {
	return tx.Exec("update runs set status=$1 where id=$2", newStatus, id)
}

func TranslateToRunStatus(status string) (RunStatusType, error) {
	switch status {
	case "Stopped":
		return RunIdle, nil
	case "In Progress":
		return RunInProgress, nil
	case "Done":
		return RunDone, nil
	default:
		return RunIdle, fmt.Errorf("failed to translate run status: %s", status)
	}
}
func (s RunStatusType) TranslateRunStatus() (string, error) {
	switch s {
	case RunIdle:
		return "Stopped", nil
	case RunInProgress:
		return "In Progress", nil
	case RunDone:
		return "Done", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", s)
	}
}
func (s RunStatusType) MustTranslateRunStatus() string {
	status, err := s.TranslateRunStatus()
	if err != nil {
		log.Panic(err)
	}
	return status
}
