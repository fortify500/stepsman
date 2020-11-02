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
	"github.com/fortify500/stepsman/dao"
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
		stepRecords, err := bl.GetSteps(&api.GetStepsQuery{
			UUIDs: []string{stepUUID},
		})
		if len(stepRecords) != 1 {
			msg := fmt.Sprintf("failed to locate step uuid [%s]", stepUUID)
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
		}
		stepRecord := stepRecords[0]
		run, err := getRun(stepRecord.RunId)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to update step: %w", err)
			return
		}
		template := bl.Template{}
		err = template.LoadFromBytes(false, []byte(run.Template))
		if err != nil {
			msg := "failed to convert step record to step"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		step := template.Steps[stepRecord.Index-1]
		if Parameters.Status != "" {
			var status api.StepStatusType
			status, err = api.TranslateToStepStatus(Parameters.Status)
			if err != nil {
				Parameters.Err = err
				return
			}
			_, err = step.UpdateStateAndStatus(&stepRecord, status, nil, false)
			if err != nil {
				msg := "failed to update step status"
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
		}
		if Parameters.StatusUUID != "" {
			err = dao.UpdateHeartBeat(&stepRecord, Parameters.StatusUUID)
			if err != nil {
				msg := "failed to update heartbeat"
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
		}
		Parameters.CurrentStepIndex = fmt.Sprintf("%d", stepRecord.Index)
	},
}

func init() {
	updateCmd.AddCommand(updateStepCmd)
	initFlags := func() error {
		updateStepCmd.ResetFlags()
		updateStepCmd.Flags().StringVarP(&Parameters.Status, "status", "s", "", fmt.Sprintf("Status - %s,%s,%s,%s", api.StepIdle.TranslateStepStatus(), api.StepInProgress.TranslateStepStatus(), api.StepFailed.TranslateStepStatus(), api.StepDone.TranslateStepStatus()))
		updateStepCmd.Flags().StringVarP(&Parameters.StatusUUID, "heartbeat", "b", "", "Will update the heartbeat. The status UUID must be supplied.")
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}