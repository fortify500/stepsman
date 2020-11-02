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
	"os"
)

var getStepCmd = &cobra.Command{
	Use:   "step",
	Args:  cobra.ExactArgs(1),
	Short: "step summary.",
	Long:  `Get step summary.`,
	Run: func(cmd *cobra.Command, args []string) {
		defer recoverAndLog("failed to get step")
		stepUUID, err := parseStepUUID(args[0])
		if err != nil {
			Parameters.Err = err
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
		run, err := getRun(stepRecords[0].RunId)
		if err != nil {
			Parameters.Err = err
			return
		}
		script := bl.Template{}
		err = script.LoadFromBytes(false, []byte(run.Template))
		if err != nil {
			msg := "failed to load template while getting a step"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		t, err := RenderStep(&stepRecords[0], &script)
		if err != nil {
			return
		}
		t.SetOutputMirror(os.Stdout)
		t.Render()
	},
}

func init() {
	getCmd.AddCommand(getStepCmd)
}
