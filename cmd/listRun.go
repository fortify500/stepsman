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

// listRunCmd represents the listRun command
var listRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.MinimumNArgs(1),
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		t := table.NewWriter()
		t.SetStyle(NoBordersStyle)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"", "", "#", "UUID", "Title", "Status", "HeartBeat"})
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
			msg := "failed to list steps"
			return &CMDError{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
		}
		for _, step := range steps {
			status, err := bl.TranslateStepStatus(step.Status)
			if err != nil {
				msg := "failed to list steps"
				return &CMDError{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
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
				heartBeat = string(step.HeartBeat)
			}
			t.AppendRows([]table.Row{
				{cursor, checked, step.StepId, step.UUID, step.Name, status, heartBeat},
			})
		}
		t.Render()
		return nil
	},
}

func init() {
	listCmd.AddCommand(listRunCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// listRunCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// listRunCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
