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
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var createRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.MaximumNArgs(0),
	Short: "Create a run and move the cursor to the first step",
	Long:  `Create a run and move the cursor to the first step. You must specify a file.`,
	Run: func(cmd *cobra.Command, args []string) {
		var t bl.Script
		Parameters.CurrentCommand = CommandCreateRun
		if len(Parameters.CreateFileName) == 0 {
			msg := "you must specify a file name"
			Parameters.Err = &CMDError{
				Technical: fmt.Errorf(msg),
				Friendly:  msg,
			}
			return
		}
		runRow, err := t.Start(Parameters.CreateFileName)
		if err == bl.ErrActiveRunsWithSameTitleExists {
			msg := "you must stop runs with the same title before creating a new run"
			//"you must either stop runs with the same title or force an additional run (see --force-run)"
			Parameters.Err = &CMDError{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		} else if err != nil {
			msg := "failed to create run"
			Parameters.Err = &CMDError{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		msg := fmt.Sprintf("run created with id: %d", runRow.Id)
		fmt.Println(msg)
		log.Info(msg)
		Parameters.CurrentRunId = runRow.Id
	},
}

func init() {
	createCmd.AddCommand(createRunCmd)
	initFlags := func() {
		createRunCmd.ResetFlags()
		createRunCmd.Flags().StringVarP(&Parameters.CreateFileName, "file", "f", "", "Template file (yaml) to create run")
		createRunCmd.MarkFlagRequired("file")
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
