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
	"github.com/fortify500/stepsman/api"
	"github.com/spf13/cobra"
)

var updateRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "A brief description of your command",
	Long: `Changes the status of a run.
Use run <run id>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandUpdateRun
		defer recoverAndLog("failed to update run")
		syncUpdateRunParams()
		runId, err := parseRunId(args[0])
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update run: %w", err)
			return
		}
		status, err := api.TranslateToRunStatus(Parameters.Status)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update run: %w", err)
			return
		}
		run, err := getRun(api.Options{GroupId: Parameters.GroupId}, runId)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update run: %w", err)
			return
		}
		err = BL.UpdateRunStatus(api.Options{GroupId: Parameters.GroupId}, runId, status)
		if err != nil {
			msg := "failed to update run status"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		Parameters.CurrentRunId = run.Id
	},
}

var updateRunParams AllParameters

func syncUpdateRunParams() {
	Parameters.Status = updateRunParams.Status
}

func init() {
	updateCmd.AddCommand(updateRunCmd)
	initFlags := func() error {
		updateRunCmd.ResetFlags()
		updateRunCmd.Flags().StringVarP(&updateRunParams.Status, "status", "s", "", fmt.Sprintf("Status - %s,%s,%s,%s,%s", api.StepIdle.TranslateStepStatus(), api.StepPending.TranslateStepStatus(), api.StepInProgress.TranslateStepStatus(), api.StepFailed.TranslateStepStatus(), api.StepDone.TranslateStepStatus()))
		err := updateRunCmd.MarkFlagRequired("status")
		return err
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
