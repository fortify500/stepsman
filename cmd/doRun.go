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

var doRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "Do can execute a command of a run step.",
	Long: `Do can execute a command of a run step, currently only shell execute commands are supported.
Use run <run id>.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		runId, err := parseRunId(args[0])
		if err != nil {
			return err
		}
		run, err := getRun(runId)
		if err != nil {
			return err
		}
		if run.Status==bl.RunDone{
			msg := "run is already done"
			return &CMDError{
				Technical: fmt.Errorf(msg),
				Friendly:  msg,
			}
		}
		stepRecord, err := getCursorStep(run)
		if err != nil {
			return err
		}
		step, err := stepRecord.ToStep()
		if err != nil {
			msg := "failed to convert step record to step"
			return &CMDError{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
		}
		_, err = step.StartDo()
		if err != nil {
			msg := "failed to start do"
			return &CMDError{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
		}
		return nil
	},
}

func init() {
	doCmd.AddCommand(doRunCmd)
}
