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

package cmd

import (
	"encoding/csv"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/dao"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"os"
	"strings"
	"time"
)

var listStepsCmd = &cobra.Command{
	Use:   "steps",
	Args:  cobra.NoArgs,
	Short: "List the steps of a run.",
	Long:  `List the steps of a run and their status.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandListSteps
		defer recoverAndLog("failed to list steps")
		syncListStepsParams()
		listStepsInternal()
	},
}

func listStepsInternal() {
	var err error
	t := table.NewWriter()
	t.SetStyle(NoBordersStyle)
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"", "Index", "UUID", "Title", "Status", "Status Owner", "Heartbeat", "Complete By", "Created At"})
	runId, err := parseRunId(Parameters.Run)
	if err != nil {
		Parameters.Err = fmt.Errorf("failed to list steps: %w", err)
		return
	}
	run, err := getRun(api.Options{GroupId: Parameters.GroupId}, runId)
	if err != nil {
		Parameters.Err = fmt.Errorf("failed to list steps: %w", err)
		return
	}
	Parameters.CurrentRunId = run.Id
	sortFields := Parameters.SortFields
	if sortFields == nil || len(sortFields) == 0 {
		sortFields = append(sortFields, dao.Index)
	} else {
		found := false
		for _, field := range sortFields {
			if field == dao.Index {
				found = true
				break
			}
		}
		if !found {
			sortFields = append(sortFields, dao.Index)
		}
	}
	query := api.ListQuery{
		Range: api.RangeQuery{
			Range: api.Range{
				Start: Parameters.RangeStart,
				End:   Parameters.RangeEnd,
			},
			ReturnTotal: Parameters.RangeReturnTotal,
		},
		Sort: api.Sort{
			Fields: sortFields,
			Order:  Parameters.SortOrder,
		},
		Filters: nil,
		Options: api.Options{GroupId: Parameters.GroupId},
	}

	if len(Parameters.Filters) > 0 {
		var exitErr bool
		query.Filters, exitErr = parseStepsFilters()
		if exitErr {
			return
		}
	}
	if query.Filters == nil || len(query.Filters) == 0 {
		query.Filters = append(query.Filters, api.Expression{
			AttributeName: dao.RunId,
			Operator:      "=",
			Value:         runId,
		})
	} else {
		found := false
		for _, filter := range query.Filters {
			if filter.AttributeName == dao.RunId {
				found = true
				break
			}
		}
		if !found {
			query.Filters = append(query.Filters, api.Expression{
				AttributeName: dao.RunId,
				Operator:      "=",
				Value:         runId,
			})
		}
	}
	steps, stepsRange, err := BL.ListSteps(&query)
	if err != nil {
		msg := "failed to list steps"
		Parameters.Err = &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
		return
	}
	for _, step := range steps {
		status := step.Status.TranslateStepStatus()
		checked := "[ ]"
		heartBeat := ""
		completeBy := ""
		switch step.Status {
		case api.StepDone:
			checked = "[V]"
		}
		createdAt := fmt.Sprintf("%s", time.Time(step.CreatedAt).Format(time.RFC3339))
		if step.CompleteBy != nil {
			completeBy = fmt.Sprintf("%s", time.Time(*step.CompleteBy).Format(time.RFC3339))
		}
		if step.Status == api.StepInProgress {
			heartBeat = fmt.Sprintf("%s", time.Time(step.Heartbeat).Format(time.RFC3339))
		}
		t.AppendRows([]table.Row{
			{checked, step.Index, step.UUID, strings.TrimSpace(text.WrapText(step.Name, 120)), status, step.StatusOwner, heartBeat, completeBy, createdAt},
		})
	}
	if stepsRange != nil && stepsRange.Total > 0 {
		t.AppendFooter(table.Row{"", "", "", "", "-----------"})
		t.AppendFooter(table.Row{"", "", "", "", fmt.Sprintf("%d-%d/%d", stepsRange.Start, stepsRange.End, stepsRange.Total)})
	}
	t.Render()
}

func parseStepsFilters() ([]api.Expression, bool) {
	expressions := make([]api.Expression, len(Parameters.Filters))
	for i, filter := range Parameters.Filters {
		name := ""
		value := ""
		operator := ""
		if strings.HasPrefix(filter, "id") {
			trimPrefix := strings.TrimPrefix(filter, dao.RunId)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "id"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.Index) {
			trimPrefix := strings.TrimPrefix(filter, dao.Index)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "uuid"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.UUID) {
			trimPrefix := strings.TrimPrefix(filter, dao.UUID)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "title"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.Status) {
			trimPrefix := strings.TrimPrefix(filter, dao.Status)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "status"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.StatusOwner) {
			trimPrefix := strings.TrimPrefix(filter, dao.StatusOwner)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "title"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.Label) {
			trimPrefix := strings.TrimPrefix(filter, dao.Label)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "title"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.Name) {
			trimPrefix := strings.TrimPrefix(filter, dao.Name)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "title"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, "startsWith(") && strings.HasSuffix(filter, ")") {
			operator = "startsWith"
			fields, errDone := detectStepsStringOperator(filter, operator)
			if errDone {
				return nil, true
			}
			name = fields[0]
			value = fields[1]
		} else if strings.HasPrefix(filter, "endsWith(") && strings.HasSuffix(filter, ")") {
			operator = "endsWith"
			fields, errDone := detectStepsStringOperator(filter, operator)
			if errDone {
				return nil, true
			}
			name = fields[0]
			value = fields[1]
		} else if strings.HasPrefix(filter, "contains(") && strings.HasSuffix(filter, ")") {
			operator = "contains"
			fields, errDone := detectStepsStringOperator(filter, operator)
			if errDone {
				return nil, true
			}
			name = fields[0]
			value = fields[1]
		} else {
			msg := "failed to parse filter"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+" %s", filter),
				Friendly:  msg,
			}
			return nil, true
		}
		expressions[i] = api.Expression{
			AttributeName: name,
			Operator:      operator,
			Value:         value,
		}
	}
	return expressions, false
}

func detectStepsStringOperator(filter string, operator string) ([]string, bool) {
	trimmed := strings.TrimPrefix(filter, operator+"(")
	trimmed = strings.TrimSuffix(trimmed, ")")
	r := csv.NewReader(strings.NewReader(trimmed))
	r.Comma = ','
	fields, err := r.Read()
	if err != nil {
		msg := "failed to parse filter"
		Parameters.Err = &Error{
			Technical: fmt.Errorf(msg+" %s", filter),
			Friendly:  msg,
		}
		return nil, true
	}
	if len(fields) != 2 {
		msg := "failed to parse filter"
		Parameters.Err = &Error{
			Technical: fmt.Errorf(msg+" %s", filter),
			Friendly:  msg,
		}
		return nil, true
	}
	switch fields[0] {
	case dao.RunId:
	case dao.UUID:
	case dao.Status:
	case dao.StatusOwner:
	case dao.Label:
	case dao.Name:
	default:
		msg := "failed to parse filter"
		Parameters.Err = &Error{
			Technical: fmt.Errorf(msg+" %s", filter),
			Friendly:  msg,
		}
		return nil, true
	}
	return fields, false
}

var listStepsParams AllParameters

func syncListStepsParams() {
	Parameters.Run = listStepsParams.Run
	Parameters.RangeStart = listStepsParams.RangeStart
	Parameters.RangeEnd = listStepsParams.RangeEnd
	Parameters.RangeReturnTotal = listStepsParams.RangeReturnTotal
	Parameters.SortFields = listStepsParams.SortFields
	Parameters.SortOrder = listStepsParams.SortOrder
	Parameters.Filters = listStepsParams.Filters
}
func init() {
	listCmd.AddCommand(listStepsCmd)
	initFlags := func() error {
		listStepsCmd.ResetFlags()
		listStepsCmd.Flags().StringVarP(&listStepsParams.Run, "run", "r", "", "Run Id")
		listStepsCmd.Flags().Int64Var(&listStepsParams.RangeStart, "range-start", 0, "Range Start")
		listStepsCmd.Flags().Int64Var(&listStepsParams.RangeEnd, "range-end", -1, "Range End")
		listStepsCmd.Flags().BoolVar(&listStepsParams.RangeReturnTotal, "range-return-total", false, "Range Return Total")
		listStepsCmd.Flags().StringArrayVar(&listStepsParams.SortFields, "sort-field", []string{dao.Index}, "Repeat sort-field for many fields")
		listStepsCmd.Flags().StringVar(&listStepsParams.SortOrder, "sort-order", "asc", "Sort order asc/desc which are a short for ascending/descending respectively")
		listStepsCmd.Flags().StringArrayVar(&listStepsParams.Filters, "filter", []string{}, "Repeat filter for many filters --filter=startsWith(\"name\",\"STEP\") \npossible operators:startsWith,endsWith,contains,>,<,>=,<=,=,<>")
		return listStepsCmd.MarkFlagRequired("run")
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
