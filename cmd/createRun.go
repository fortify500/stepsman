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
		defer recoverAndLog("failed to create run")
		syncCreateRunParams()
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
		runId, err := t.LoadAndCreateRun(
			BL,
			api.Options{GroupId: Parameters.GroupId},
			Parameters.RunKey,
			Parameters.CreateFileName,
			Parameters.FileType)
		if err != nil {
			msg := "failed to create run"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		msg := fmt.Sprintf("run created with id: %s", runId)
		fmt.Println(msg)
		log.Info(msg)
		Parameters.CurrentRunId = runId
	},
}
var createRunParams AllParameters

func syncCreateRunParams() {
	Parameters.RunKey = createRunParams.RunKey
	Parameters.CreateFileName = createRunParams.CreateFileName
	Parameters.FileType = createRunParams.FileType
}
func init() {
	createCmd.AddCommand(createRunCmd)
	initFlags := func() error {
		createRunCmd.ResetFlags()
		random, err := uuid.NewRandom()
		if err != nil {
			panic(err)
		}
		createRunCmd.Flags().StringVarP(&createRunParams.RunKey, "key", "k", random.String(), "Run unique key. If omitted a random uuid will be used")
		createRunCmd.Flags().StringVarP(&createRunParams.FileType, "type", "t", random.String(), "File type. Supports: JSON, YAML.")
		createRunCmd.Flags().StringVarP(&createRunParams.CreateFileName, "file", "f", "", "Template file (yaml) to create run")
		err = createRunCmd.MarkFlagRequired("file")
		return err
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
