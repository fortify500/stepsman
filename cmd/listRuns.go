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
		var err error
		Parameters.CurrentCommand = CommandListRuns
		t := table.NewWriter()
		t.SetStyle(bl.NoBordersStyle)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"ID", "UUID", "Title", "Cursor", "Status"})
		var runs []*bl.RunRecord
		if strings.TrimSpace(Parameters.Run) != "" {
			runId, err := parseRunId(Parameters.Run)
			if err != nil {
				Parameters.Err = err
				return
			}
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
			status, err := bl.TranslateRunStatus(run.Status)
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
	},
}

func init() {
	listCmd.AddCommand(listRunsCmd)
	initFlags := func() {
		listRunsCmd.ResetFlags()
		listRunsCmd.Flags().StringVarP(&Parameters.Run, "run", "r", "", "Run Id")
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
