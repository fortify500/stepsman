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
	"strings"
)

var doRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "Do can execute a command of a run step.",
	Long: `Do can execute a command of a run step, currently only shell execute commands are supported.
Use run <run id>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandDoRun
		defer recoverAndLog("failed to do run")
		runId, err := parseRunId(args[0])
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to do run: %w", err)
			return
		}
		_, err = parseIndex(Parameters.Step)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to do run: %w", err)
			return
		}
		run, err := getRun(runId)
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to do run: %w", err)
			return
		}
		Parameters.CurrentRunId = run.Id
		Parameters.CurrentRun = run
		if run.Status == api.RunDone {
			msg := "run is already done"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg),
				Friendly:  msg,
			}
			return
		}
		expressions := []api.Expression{{
			AttributeName: dao.RunId,
			Operator:      "=",
			Value:         args[0],
		}, {
			AttributeName: dao.Index,
			Operator:      "=",
			Value:         Parameters.Step,
		}}
		stepRecords, _, err := bl.ListSteps(&api.ListQuery{
			Sort: api.Sort{
				Fields: []string{dao.Index},
				Order:  "asc",
			},
			Filters:          expressions,
			ReturnAttributes: nil,
		})
		if err != nil {
			msg := "failed to perform do run"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		if len(stepRecords) != 1 {
			msg := "failed to perform do run"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg + ": list steps query should have returned only 1 step record"),
				Friendly:  msg,
			}
			return
		}
		stepRecord := stepRecords[0]
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
		err = step.StartDo(&stepRecord)
		if err != nil {
			msg := "failed to start do"
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
		Parameters.CurrentStepIndex = strings.TrimSpace(Parameters.Step)
	},
}

func init() {
	doCmd.AddCommand(doRunCmd)
	initFlags := func() error {
		doRunCmd.ResetFlags()
		doRunCmd.Flags().StringVarP(&Parameters.Step, "step", "s", "", "Step Index")
		err := doRunCmd.MarkFlagRequired("step")
		return err
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
