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
	"github.com/spf13/cobra"
	"os"
)

var listRunsCmd = &cobra.Command{
	Use:   "runs",
	Args:  cobra.MaximumNArgs(0),
	Short: "Runs summary.",
	Long: `A succinct list of runs and their status.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		t := table.NewWriter()
		t.SetStyle(NoBordersStyle)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"#", "UUID", "Title", "Status"})
		runs, err := bl.ListRuns()
		if err != nil {
			msg := "failed to listRuns runs"
			return &CMDError{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
		}
		for _, run := range runs {
			status, err := bl.TranslateRunStatus(run.Status)
			if err != nil {
				msg := "failed to listRuns runs"
				return &CMDError{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
			}

			t.AppendRows([]table.Row{
				{run.Id, run.UUID, run.Title, status},
			})
		}
		t.Render()
		return nil
	},
}



func init() {
	listCmd.AddCommand(listRunsCmd)
}
