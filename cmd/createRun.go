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
	"github.com/fortify500/stepsman/bl"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"strings"
)

var createRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.MaximumNArgs(0),
	Short: "Create a run",
	Long:  `Create a run. You must specify a file.`,
	Run: func(cmd *cobra.Command, args []string) {
		var t bl.Template
		Parameters.CurrentCommand = CommandCreateRun
		Parameters.CreateFileName = strings.TrimSpace(Parameters.CreateFileName)
		strings.TrimPrefix(Parameters.CreateFileName, "\"")
		strings.TrimSuffix(Parameters.CreateFileName, "\"")
		if len(Parameters.CreateFileName) == 0 {
			msg := "you must specify a file name"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg),
				Friendly:  msg,
			}
			return
		}
		runRow, err := t.Start(Parameters.RunKey, Parameters.CreateFileName)
		if err != nil {
			msg := "failed to create run"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		msg := fmt.Sprintf("run created with id: %s", runRow.Id)
		fmt.Println(msg)
		log.Info(msg)
		Parameters.CurrentRunId = runRow.Id
	},
}

func init() {
	createCmd.AddCommand(createRunCmd)
	initFlags := func() error {
		createRunCmd.ResetFlags()
		random, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		createRunCmd.Flags().StringVarP(&Parameters.RunKey, "key", "k", random.String(), "Run unique key. If omitted a random uuid will be used")
		createRunCmd.Flags().StringVarP(&Parameters.CreateFileName, "file", "f", "", "Template file (yaml) to create run")
		err = createRunCmd.MarkFlagRequired("file")
		return err
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
