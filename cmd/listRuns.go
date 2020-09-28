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
package cmd

import (
	"fmt"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
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
		listRunsInternal(-1)
	},
}

func listRunsInternal(runId int64) {
	var err error
	Parameters.CurrentCommand = CommandListRuns
	t := table.NewWriter()
	t.SetStyle(NoBordersStyle)
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"ID", "UUID", "Title", "Cursor", "Status"})
	var runs []*dao.RunRecord
	if runId >= 0 {
		run, err := getRun(runId)
		if err != nil {
			Parameters.Err = err
			return
		}
		runs = append(runs, run)
	} else {
		runs, err = bl.ListRuns()
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
		status, err := run.Status.TranslateRunStatus()
		if err != nil {
			msg := "failed to listRuns runs"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}

		t.AppendRows([]table.Row{
			{run.Id, run.UUID, strings.TrimSpace(text.WrapText(run.Title, 70)), run.Cursor, status},
		})
	}
	t.Render()
}

func init() {
	listCmd.AddCommand(listRunsCmd)
}
