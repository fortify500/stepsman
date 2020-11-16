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
	"github.com/fortify500/stepsman/bl"
	"github.com/spf13/cobra"
)

var updateStepCmd = &cobra.Command{
	Use:   "step",
	Args:  cobra.ExactArgs(1),
	Short: "update a step of a run.",
	Long: `update a step of a run.
Use run <run id>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandUpdateStep
		defer recoverAndLog("failed to update step")
		stepUUID, err := parseStepUUID(args[0])
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update step: %w", err)
			return
		}
		changes := make(map[string]interface{})
		updateQuery := &api.UpdateQuery{
			Id:      stepUUID,
			Force:   Parameters.Force,
			Changes: changes,
		}

		if Parameters.Status != "" {
			var status api.StepStatusType
			status, err = api.TranslateToStepStatus(Parameters.Status)
			if err != nil {
				Parameters.Err = err
				return
			}
			changes["status"] = status.TranslateStepStatus()
		} else if Parameters.StatusUUID != "" {
			var statusUUID string
			statusUUID, err = parseStepUUID(Parameters.StatusUUID)
			if err != nil {
				Parameters.Err = fmt.Errorf("failed to update step: %w", err)
				return
			}
			changes["heartbeat"] = statusUUID
		} else {
			Parameters.Err = fmt.Errorf("failed to update step no argument provided")
			return
		}
		err = bl.UpdateStep(updateQuery)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update step: %w", err)
			return
		}
		stepRecords, err := bl.GetSteps(&api.GetStepsQuery{
			UUIDs: []string{stepUUID},
		})
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update step: %w", err)
			return
		}
		if len(stepRecords) != 1 {
			Parameters.Err = fmt.Errorf("failed to locate step record for uuid [%s]", stepUUID)
			return
		}
		Parameters.CurrentStepIndex = fmt.Sprintf("%d", stepRecords[0].Index)
	},
}

func init() {
	updateCmd.AddCommand(updateStepCmd)
	initFlags := func() error {
		updateStepCmd.ResetFlags()
		updateStepCmd.Flags().StringVarP(&Parameters.Status, "status", "s", "", fmt.Sprintf("Status - %s,%s,%s,%s,%s", api.StepIdle.TranslateStepStatus(), api.StepPending.TranslateStepStatus(), api.StepInProgress.TranslateStepStatus(), api.StepFailed.TranslateStepStatus(), api.StepDone.TranslateStepStatus()))
		updateStepCmd.Flags().BoolVarP(&Parameters.Force, "force", "f", false, fmt.Sprintf("force change status - ignores heartbeat"))
		updateStepCmd.Flags().StringVarP(&Parameters.StatusUUID, "heartbeat", "b", "", "Will update the heartbeat. The status UUID must be supplied.")
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
