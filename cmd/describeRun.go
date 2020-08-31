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
	Args:  cobra.RangeArgs(1,2),
	Short: "Describe a run steps",
	Long: `Enumerate the steps of a run in a verbose and friendly way. 
Use run <run id>.
You can also describe a single step by adding --step <step id>.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		runId, err := parseRunId(args[0])
		if err != nil {
			return err
		}
		run, err := getRun(runId)
		if err != nil {
			return err
		}
		steps, err := bl.ListSteps(run.Id)
		if err != nil {
			msg := "failed to describe steps"
			return &CMDError{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
		}
		for i, stepRecord := range steps {
			if Parameters.Step>0 && Parameters.Step!=int64(i)+1 {
				continue
			}
			status, err := bl.TranslateStepStatus(stepRecord.Status)
			if err != nil {
				msg := "failed to describe steps"
				return &CMDError{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
			}
			cursor := ""
			checked := "[ ]"
			heartBeat := ""
			switch stepRecord.Status {
			case bl.StepDone:
				checked = "True"
			case bl.StepSkipped:
				checked = "True"
			}
			if stepRecord.StepId == run.Cursor {
				cursor = "True"
			}
			if stepRecord.Status == bl.StepInProgress {
				heartBeat = string(stepRecord.HeartBeat)
			}
			step, err := stepRecord.ToStep()
			if err != nil {
				msg := "failed to describe steps"
				return &CMDError{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
			}
			{
				description := step.Description
				description = strings.TrimSuffix(description, "\r")
				t := table.NewWriter()
				t.SetOutputMirror(os.Stdout)
				//t.SetTitle(cursor + checked + " " + text.WrapText(stepRecord.Name, 70))
				t.SetStyle(NoBordersStyle)
				t.AppendRows([]table.Row{
					//{cursor, checked, stepRecord.StepId, stepRecord.UUID, stepRecord.Name, status, heartBeat},
					{"Id:", stepRecord.StepId},
					{"Cursor:", cursor},
					{"Name:", stepRecord.Name},
					{"UUID:", stepRecord.UUID},
					{"Status:", status},
					{"Heartbeat:", heartBeat},
					{"Done:", checked},
					{"Description:", text.WrapText(step.Description, 70)},
				})
				t.Render()
			}
		}
		return nil
	},
}

func init() {
	describeCmd.AddCommand(describeRunCmd)
	describeRunCmd.Flags().Int64VarP(&Parameters.Step, "step", "s", -1, "Step Id")
}
