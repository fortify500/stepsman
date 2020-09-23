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

var describeRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.RangeArgs(1, 2),
	Short: "Describe a run steps",
	Long: `Enumerate the steps of a run in a verbose and friendly way. 
Use run <run id>.
You can also describe a single step by adding --step <step id>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandDescribeRun
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
		stepId, err := parseStepId(run, Parameters.Step)
		if err != nil {
			Parameters.Err = err
			return
		}
		steps, err := bl.ListSteps(run.Id)
		if err != nil {
			msg := "failed to describe steps"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		mainT := table.NewWriter()
		mainT.SetStyle(NoBordersStyle)
		mainT.SetOutputMirror(os.Stdout)
		if stepId == -1 {
			mainT.AppendRow(table.Row{"Description"})

			{
				runStatus, err := bl.TranslateRunStatus(run.Status)
				if err != nil {
					msg := "failed to describe steps"
					Parameters.Err = &Error{
						Technical: fmt.Errorf(msg+": %w", err),
						Friendly:  msg,
					}
					return
				}
				t := table.NewWriter()
				//t.SetOutputMirror(os.Stdout)
				//t.SetTitle(cursor + checked + " " + text.WrapText(stepRecord.Name, 70))
				t.SetStyle(NoBordersStyle)
				t.AppendRows([]table.Row{
					//{cursor, checked, stepRecord.StepId, stepRecord.UUID, stepRecord.Name, status, heartBeat},
					{"Id:", run.Id},
					{"Cursor:", run.Cursor},
					{"Title:", strings.TrimSpace(text.WrapText(run.Title, 70))},
					{"UUID:", run.UUID},
					{"Status:", runStatus},
				})
				mainT.AppendRow(table.Row{t.Render()})
			}
			mainT.AppendRow(table.Row{""})
			mainT.AppendRow(table.Row{"Steps"})
		} else {
			mainT.AppendRow(table.Row{"Step"})
		}

		for i, stepRecord := range steps {
			if stepId > 0 && stepId != int64(i)+1 {
				continue
			}
			status, err := bl.TranslateStepStatus(stepRecord.Status)
			if err != nil {
				msg := "failed to describe steps"
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
			cursor := ""
			checked := "[ ]"
			heartBeat := "N/A"
			switch stepRecord.Status {
			case bl.StepDone:
				checked = "True"
			case bl.StepSkipped:
				checked = "True"
			}
			if stepRecord.StepId == run.Cursor {
				cursor = "True"
			} else {
				cursor = "False"
			}
			if stepRecord.Status == bl.StepInProgress {
				heartBeat = fmt.Sprintf("%d", stepRecord.HeartBeat)
			}
			step, err := stepRecord.ToStep()
			if err != nil {
				msg := "failed to describe steps"
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
			{
				t := table.NewWriter()
				//t.SetOutputMirror(os.Stdout)
				//t.SetTitle(cursor + checked + " " + text.WrapText(stepRecord.Name, 70))
				t.SetStyle(NoBordersStyle)
				t.AppendRows([]table.Row{
					//{cursor, checked, stepRecord.StepId, stepRecord.UUID, stepRecord.Name, status, heartBeat},
					{"Id:", stepRecord.StepId},
					{"Cursor:", cursor},
					{"Name:", strings.TrimSpace(text.WrapText(stepRecord.Name, TableWrapLen))},
					{"UUID:", stepRecord.UUID},
					{"Status:", status},
					{"Heartbeat:", heartBeat},
					{"Done:", checked},
					{"Description:", strings.TrimSpace(text.WrapText(step.Description, TableWrapLen))},
				})
				if step.Do != nil {
					do, ok := step.Do.(bl.DO)
					if ok {
						t.AppendRow(table.Row{"Do:", strings.TrimSpace(text.WrapText(do.Describe(), 70))})
					}
				}
				t.AppendRow(table.Row{"", ""})
				mainT.AppendRow(table.Row{t.Render()})
			}
		}

		mainT.Render()
	},
}

func init() {
	describeCmd.AddCommand(describeRunCmd)
	initFlags := func() {
		describeRunCmd.ResetFlags()
		describeRunCmd.Flags().StringVarP(&Parameters.Step, "step", "s", "", "Step Id")
		describeRunCmd.Flags().Lookup("step").NoOptDefVal = "cursor"
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
