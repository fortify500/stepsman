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

var doRunCmd = &cobra.Command{
	Use:   "step",
	Args:  cobra.ExactArgs(1),
	Short: "Do can execute a step do.",
	Long: `Do can execute a step do.
Use do step <step uuid>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandDoStep
		defer recoverAndLog("failed to do step")
		stepUUID, err := parseStepUUID(args[0])
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to do step: %w", err)
			return
		}
		_, err = bl.DoStep(&api.DoStepParams{UUID: stepUUID}, true, false)
		if err != nil {
			msg := "failed to do step"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
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

func init() {
	doCmd.AddCommand(doRunCmd)
}
