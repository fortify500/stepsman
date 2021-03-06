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
	"github.com/google/uuid"
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
		syncUpdateStepParams()
		stepUUID, err := parseStepUUID(args[0])
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update step: %w", err)
			return
		}
		changes := make(map[string]interface{})
		updateQuery := &api.UpdateQueryByUUID{
			UUID:        stepUUID,
			StatusOwner: Parameters.StatusOwner,
			Force:       Parameters.Force,
			Changes:     changes,
			Options:     api.Options{GroupId: Parameters.GroupId},
		}

		if Parameters.Status != "" {
			var status api.StepStatusType
			status, err = api.TranslateToStepStatus(Parameters.Status)
			if err != nil {
				Parameters.Err = err
				return
			}
			changes["status"] = status.TranslateStepStatus()
		} else if Parameters.StatusOwner != "" {
		} else {
			Parameters.Err = fmt.Errorf("failed to update step no argument provided")
			return
		}
		if Parameters.State != "" {
			changes["state"] = Parameters.State
		}
		err = BL.UpdateStepByUUID(updateQuery)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update step: %w", err)
			return
		}
		stepRecords, err := BL.GetSteps(&api.GetStepsQuery{
			UUIDs:   []uuid.UUID{stepUUID},
			Options: api.Options{GroupId: Parameters.GroupId},
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

var updateStepParams AllParameters

func syncUpdateStepParams() {
	Parameters.Status = updateStepParams.Status
	Parameters.Force = updateStepParams.Force
	Parameters.StatusOwner = updateStepParams.StatusOwner
	Parameters.State = updateStepParams.State
}

func init() {
	updateCmd.AddCommand(updateStepCmd)
	initFlags := func() error {
		updateStepCmd.ResetFlags()
		updateStepCmd.Flags().StringVarP(&updateStepParams.Status, "status", "s", "", fmt.Sprintf("Status - %s,%s,%s,%s,%s", api.StepIdle.TranslateStepStatus(), api.StepPending.TranslateStepStatus(), api.StepInProgress.TranslateStepStatus(), api.StepFailed.TranslateStepStatus(), api.StepDone.TranslateStepStatus()))
		updateStepCmd.Flags().StringVarP(&updateStepParams.State, "state", "t", "", "will update the state, but status must also be specified")
		updateStepCmd.Flags().BoolVarP(&updateStepParams.Force, "force", "f", false, fmt.Sprintf("force change status - ignores heartbeat"))
		updateStepCmd.Flags().StringVarP(&updateStepParams.StatusOwner, "status-owner", "o", "", "will only update the heartbeat if no other options supplied")
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
