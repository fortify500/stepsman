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
	"fmt"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
	"github.com/spf13/cobra"
	"os"
	"strings"
)

var listStepsCmd = &cobra.Command{
	Use:   "steps",
	Args:  cobra.NoArgs,
	Short: "List the steps of a run.",
	Long:  `List the steps of a run and their status.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandListSteps
		t := table.NewWriter()
		t.SetStyle(NoBordersStyle)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"", "Index", "UUID", "Title", "Status", "HeartBeat"})
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
			checked := "[ ]"
			heartBeat := ""
			switch step.Status {
			case dao.StepDone:
				checked = "[V]"
			}
			if step.Status == dao.StepInProgress {
				heartBeat = fmt.Sprintf("%s", step.HeartBeat)
			}
			t.AppendRows([]table.Row{
				{checked, step.Index, step.UUID, strings.TrimSpace(text.WrapText(step.Name, 120)), status, heartBeat},
			})
		}
		t.Render()
	},
}

func init() {
	listCmd.AddCommand(listStepsCmd)
	initFlags := func() error {
		listStepsCmd.ResetFlags()
		listStepsCmd.Flags().StringVarP(&Parameters.Run, "run", "r", "", "Run Id")
		return listStepsCmd.MarkFlagRequired("run")
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
