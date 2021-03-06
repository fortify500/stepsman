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
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"
	"os"
	"strings"
	"time"
)

var describeRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.RangeArgs(1, 2),
	Short: "Describe a run steps",
	Long: `Enumerate the steps of a run in a verbose and friendly way. 
Use run <run id>.
You can also describe a single step by adding --step <Index>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandDescribeRun
		defer recoverAndLog("failed to describe run")
		syncDescribeRunParams()
		runId, err := parseRunId(args[0])
		if err != nil {
			Parameters.Err = err
			return
		}
		run, err := getRun(api.Options{GroupId: Parameters.GroupId}, runId)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to describe run: %w", err)
			return
		}
		Parameters.CurrentRunId = run.Id
		expressions := []api.Expression{{
			AttributeName: dao.RunId,
			Operator:      "=",
			Value:         runId,
		}}
		index, err := parseIndex(Parameters.Step)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to describe run: %w", err)
			return
		}
		if index > 0 {
			expressions = append(expressions, api.Expression{
				AttributeName: dao.Index,
				Operator:      "=",
				Value:         Parameters.Step,
			})
		}
		stepRecords, _, err := BL.ListSteps(&api.ListQuery{
			Sort: api.Sort{
				Fields: []string{dao.Index},
				Order:  "asc",
			},
			Filters:          expressions,
			ReturnAttributes: nil,
			Options:          api.Options{GroupId: Parameters.GroupId},
		})
		if err != nil {
			msg := "failed to describe steps"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		script := bl.Template{}
		err = script.LoadFromBytes(BL, run.Id, false, []byte(run.Template))
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
		if index == -1 {
			mainT.AppendRow(table.Row{"Description"})

			{
				runStatus := run.Status.TranslateRunStatus()
				t := table.NewWriter()
				//t.SetOutputMirror(os.Stdout)
				//t.SetTitle( checked + " " + text.WrapText(stepRecord.Name, 120))
				t.SetStyle(NoBordersStyle)
				completeBy := ""
				if run.CompleteBy != nil {
					completeBy = time.Time(*run.CompleteBy).Format(time.RFC3339)
				}
				t.AppendRows([]table.Row{
					//{ checked, stepRecord.index, stepRecord.UUID, stepRecord.Name, status, heartBeat},
					{"Id:", run.Id},
					{"Key:", run.Key},
					{"Template Title:", strings.TrimSpace(text.WrapText(run.TemplateTitle, 120))},
					{"Status:", runStatus},
					{"Complete By:", completeBy},
					{"Created At:", time.Time(run.CreatedAt).Format(time.RFC3339)},
				})
				mainT.AppendRow(table.Row{t.Render()})
			}
			mainT.AppendRow(table.Row{""})
			mainT.AppendRow(table.Row{"Steps"})
		} else {
			mainT.AppendRow(table.Row{"Step"})
		}

		var t table.Writer
		for i, stepRecord := range stepRecords {
			if index > 0 && index != int64(i)+1 {
				continue
			}
			t, err = RenderStep(&stepRecord, &script)

			if err != nil {
				msg := "failed to describe steps"
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
			mainT.AppendRow(table.Row{t.Render()})
		}

		mainT.Render()
	},
}

func RenderStep(stepRecord *api.StepRecord, script *bl.Template) (table.Writer, error) {
	status := stepRecord.Status.TranslateStepStatus()
	checked := "[ ]"
	heartBeat := "N/A"
	completeBy := "N/A"
	createdAt := fmt.Sprintf("%s", time.Time(stepRecord.CreatedAt).Format(time.RFC3339))
	switch stepRecord.Status {
	case api.StepDone:
		checked = "True"
	}
	if stepRecord.CompleteBy != nil {
		completeBy = fmt.Sprintf("%s", time.Time(*stepRecord.CompleteBy).Format(time.RFC3339))
	}
	if stepRecord.Status == api.StepInProgress {
		heartBeat = fmt.Sprintf("%s", time.Time(stepRecord.Heartbeat).Format(time.RFC3339))
	}
	step := script.Steps[stepRecord.Index-1]
	t := table.NewWriter()
	t.SetStyle(NoBordersStyle)
	t.AppendRows([]table.Row{
		//{ checked, stepRecord.index, stepRecord.UUID, stepRecord.Name, status, heartBeat},
		{"Index:", stepRecord.Index},
		{"Name:", strings.TrimSpace(text.WrapText(stepRecord.Name, TableWrapLen))},
		{"Label:", strings.TrimSpace(text.WrapText(stepRecord.Label, TableWrapLen))},
		{"UUID:", stepRecord.UUID},
		{"Status:", status},
		{"Status Owner:", stepRecord.StatusOwner},
		{"Heartbeat:", heartBeat},
		{"Complete By:", completeBy},
		{"Created At:", createdAt},
		{"Done:", checked},
		{"Description:", strings.TrimSpace(text.WrapText(step.Description, TableWrapLen))},
	})
	if step.Do != nil {
		do, ok := step.Do.(bl.DO)
		if ok {
			var describe string
			describe = do.Describe()
			t.AppendRow(table.Row{"Do:", strings.TrimSpace(text.WrapText(describe, 120))})
		}
	}

	jsonState := stepRecord.PrettyJSONState()
	t.AppendRow(table.Row{"State:", strings.TrimSpace(text.WrapText(jsonState, 120))})
	jsonContext, err := json.Marshal(stepRecord.Context)
	if err != nil {
		panic(err)
	}
	t.AppendRow(table.Row{"Context:", strings.TrimSpace(text.WrapText(string(jsonContext), 120))})
	t.AppendRow(table.Row{"", ""})
	return t, nil
}

var describeRunParams AllParameters

func syncDescribeRunParams() {
	Parameters.Step = describeRunParams.Step
}
func init() {
	describeCmd.AddCommand(describeRunCmd)
	initFlags := func() error {
		describeRunCmd.ResetFlags()
		describeRunCmd.Flags().StringVarP(&describeRunParams.Step, "step", "s", "", "Step Index")
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
