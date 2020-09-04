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

var listRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "List the steps of a run.",
	Long: `List the steps of a run and their status.
Use run <run id>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandListRun
		t := table.NewWriter()
		t.SetStyle(bl.NoBordersStyle)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"", "", "ID", "UUID", "Title", "Status", "HeartBeat"})
		runId, err := parseRunId(args[0])
		if err != nil {
			Parameters.Err = err
			return
		}
		run, err := getRun(runId)
		if err != nil {
			Parameters.Err = err
			return
		}
		Parameters.CurrentRunId = run.Id
		steps, err := bl.ListSteps(run.Id)
		if err != nil {
			msg := "failed to list steps"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		for _, step := range steps {
			status, err := bl.TranslateStepStatus(step.Status)
			if err != nil {
				msg := "failed to list steps"
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
			cursor := " "
			checked := "[ ]"
			heartBeat := ""
			switch step.Status {
			case bl.StepDone:
				checked = "[V]"
			case bl.StepSkipped:
				checked = "[V]"
			}
			if step.StepId == run.Cursor {
				cursor = ">"
			}
			if step.Status == bl.StepInProgress {
				heartBeat = fmt.Sprintf("%d", step.HeartBeat)
			}
			t.AppendRows([]table.Row{
				{cursor, checked, step.StepId, step.UUID, strings.TrimSpace(text.WrapText(step.Name, 70)), status, heartBeat},
			})
		}
		t.Render()
	},
}

func init() {
	listCmd.AddCommand(listRunCmd)
}
