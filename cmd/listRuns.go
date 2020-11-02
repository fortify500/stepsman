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
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var listRunsCmd = &cobra.Command{
	Use:   "runs",
	Args:  cobra.MaximumNArgs(0),
	Short: "Runs summary.",
	Long:  `A succinct list of runs and their status.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandListRuns
		defer recoverAndLog("failed to list runs")
		listRunsInternal("")
	},
}

func listRunsInternal(runId string) {
	var err error
	t := table.NewWriter()
	t.SetStyle(NoBordersStyle)
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "Key", "Template Title", "Status"})
	var runs []api.RunRecord
	var runRange *api.RangeResult
	if runId != "" {
		var run *api.RunRecord
		run, err = getRun(runId)
		if err != nil {
			Parameters.Err = err
			return
		}
		runs = append(runs, *run)
	} else {
		query := api.ListQuery{
			Range: api.RangeQuery{
				Range: api.Range{
					Start: Parameters.RangeStart,
					End:   Parameters.RangeEnd,
				},
				ReturnTotal: Parameters.RangeReturnTotal,
			},
			Sort: api.Sort{
				Fields: Parameters.SortFields,
				Order:  Parameters.SortOrder,
			},
			Filters: nil,
		}
		if len(Parameters.Filters) > 0 {
			var exitErr bool
			query.Filters, exitErr = parseRunsFilters()
			if exitErr {
				return
			}
		}
		runs, runRange, err = bl.ListRuns(&query)
	}

	if err != nil {
		msg := "failed to listRuns runs"
		Parameters.Err = &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
		return
	}
	for _, run := range runs {
		status := run.Status.TranslateRunStatus()
		t.AppendRows([]table.Row{
			{run.Id, run.Key, strings.TrimSpace(text.WrapText(run.TemplateTitle, 120)), status},
		})
	}
	if runRange != nil && runRange.Total > 0 {
		t.AppendFooter(table.Row{"", "", "", "", "-----------"})
		t.AppendFooter(table.Row{"", "", "", "", fmt.Sprintf("%d-%d/%d", runRange.Start, runRange.End, runRange.Total)})
	}
	t.Render()
}

func parseRunsFilters() ([]api.Expression, bool) {
	expressions := make([]api.Expression, len(Parameters.Filters))
	for i, filter := range Parameters.Filters {
		name := ""
		value := ""
		operator := ""
		if strings.HasPrefix(filter, "id") {
			trimPrefix := strings.TrimPrefix(filter, dao.Id)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "id"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.Key) {
			trimPrefix := strings.TrimPrefix(filter, dao.Key)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "uuid"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.TemplateTitle) {
			trimPrefix := strings.TrimPrefix(filter, dao.TemplateTitle)
			if operator = detectStartsWithGTLTEquals(trimPrefix, filter); operator == "" {
				return nil, true
			}
			name = "title"
			value = strings.TrimPrefix(trimPrefix, operator)
		} else if strings.HasPrefix(filter, dao.TemplateVersion) {
			trimPrefix := strings.TrimPrefix(filter, dao.TemplateVersion)
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
		} else if strings.HasPrefix(filter, "startsWith(") && strings.HasSuffix(filter, ")") {
			operator = "startsWith"
			fields, errDone := detectRunsStringOperator(filter, operator)
			if errDone {
				return nil, true
			}
			name = fields[0]
			value = fields[1]
		} else if strings.HasPrefix(filter, "endsWith(") && strings.HasSuffix(filter, ")") {
			operator = "endsWith"
			fields, errDone := detectRunsStringOperator(filter, operator)
			if errDone {
				return nil, true
			}
			name = fields[0]
			value = fields[1]
		} else if strings.HasPrefix(filter, "contains(") && strings.HasSuffix(filter, ")") {
			operator = "contains"
			fields, errDone := detectRunsStringOperator(filter, operator)
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

func detectRunsStringOperator(filter string, operator string) ([]string, bool) {
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
	case dao.Id:
	case dao.Key:
	case dao.TemplateTitle:
	case dao.Status:
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

func init() {
	listCmd.AddCommand(listRunsCmd)
	initFlags := func() error {
		listRunsCmd.ResetFlags()
		listRunsCmd.Flags().Int64Var(&Parameters.RangeStart, "range-start", 0, "Range Start")
		listRunsCmd.Flags().Int64Var(&Parameters.RangeEnd, "range-end", -1, "Range End")
		listRunsCmd.Flags().BoolVar(&Parameters.RangeReturnTotal, "range-return-total", false, "Range Return Total")
		listRunsCmd.Flags().StringArrayVar(&Parameters.SortFields, "sort-field", []string{}, "Repeat sort-field for many fields")
		listRunsCmd.Flags().StringVar(&Parameters.SortOrder, "sort-order", "desc", "Sort order asc/desc which are a short for ascending/descending respectively")
		listRunsCmd.Flags().StringArrayVar(&Parameters.Filters, "filter", []string{}, "Repeat filter for many filters --filter=startsWith(\"title\",\"STEP\") --filter=title=STEPSMAN\\ Hello\\ World\npossible operators:startsWith,endsWith,contains,>,<,>=,<=,=,<>")
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
