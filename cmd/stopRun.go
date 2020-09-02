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

var stopRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "A brief description of your command",
	Long: `Changes the status of a run to Stopped.
Use run <run id>.`,
	Run: func(cmd *cobra.Command, args []string) {
		Parameters.CurrentCommand = CommandStopRun
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
		err = run.UpdateStatus(bl.RunStopped)
		if err != nil {
			msg := "failed to update run status"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
	},
}

func init() {
	stopCmd.AddCommand(stopRunCmd)
}
