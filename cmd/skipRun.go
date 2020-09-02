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
	"github.com/spf13/cobra"
)

var skipRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "Skip a step of a run.",
	Long: `Skip a step of a run. The step is the one of the cursor. After skipping the cursor will advance.
Use run <run id>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandSkipRun
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
		Parameters.CurrentRun = run
		if run.Status == bl.RunDone {
			msg := "run is already done"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg),
				Friendly:  msg,
			}
			return
		}
		stepRecord, err := getCursorStep(run)
		if err != nil {
			Parameters.Err = err
			return
		}
		err = stepRecord.UpdateStatus(bl.StepSkipped, false)
		if err != nil {
			msg := "failed to update step status"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		Parameters.CurrentRun, err = getRun(runId)
		if err != nil {
			Parameters.Err = err
			return
		}
	},
}

func init() {
	skipCmd.AddCommand(skipRunCmd)
}
