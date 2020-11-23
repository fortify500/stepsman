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
	"github.com/spf13/cobra"
)

var doRunCmd = &cobra.Command{
	Use:   "step",
	Args:  cobra.ExactArgs(1),
	Short: "Do can execute a step do.",
	Long: `Do can execute a step do.
Use do step <step uuid>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandDoStep
		defer recoverAndLog("failed to do step")
		syncDoStepParams()
		stepUUID, err := parseStepUUID(args[0])
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to do step: %w", err)
			return
		}
		statusOwner := ""
		if Parameters.StatusOwner != "" {
			statusOwner = Parameters.StatusOwner
		}
		var stepContext api.Context
		err = json.Unmarshal([]byte(Parameters.Context), &stepContext)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to do step: %w", err)
			return
		}
		var doStepResult *api.DoStepResult
		doStepResult, err = bl.DoStep(&api.DoStepParams{
			UUID:        stepUUID,
			Context:     stepContext,
			StatusOwner: statusOwner,
		}, true)
		if err != nil {
			msg := "failed to do step"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		fmt.Printf("returned status owner: %s\n", doStepResult.StatusOwner)
		stepRecords, err := bl.GetSteps(&api.GetStepsQuery{
			UUIDs: []string{stepUUID},
		})
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to do step: %w", err)
			return
		}
		if len(stepRecords) != 1 {
			Parameters.Err = fmt.Errorf("failed to locate step uuid [%s]", stepUUID)
			return
		}
		stepRecord := stepRecords[0]
		Parameters.CurrentRun, err = getRun(stepRecord.RunId)
		if err != nil {
			Parameters.Err = err
			return
		}
		Parameters.CurrentStepIndex = fmt.Sprintf("%d", stepRecord.Index)
	},
}

var doStepParams AllParameters

func syncDoStepParams() {
	Parameters.StatusOwner = doStepParams.Step
	Parameters.Context = doStepParams.Context
}
func init() {
	doCmd.AddCommand(doRunCmd)
	initFlags := func() error {
		doRunCmd.ResetFlags()
		doRunCmd.Flags().StringVarP(&doStepParams.StatusOwner, "step-owner", "s", "", "Step Owner - to prevent do step contentions and duplicate calls, a step owner will allow duplicate calls and behave as the first call")
		doRunCmd.Flags().StringVarP(&doStepParams.Context, "context", "e", "{}", "step do context which will be available in {% %} or rego code")
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
