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
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var deleteRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "delete a run",
	Long:  `delete a run.`,
	Run: func(cmd *cobra.Command, args []string) {
		defer recoverAndLog("failed to delete run")
		syncDeleteRunParams()
		Parameters.CurrentCommand = CommandDeleteRun
		runId, err := parseRunId(args[0])
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to delete run: %w", err)
			return
		}
		err = BL.DeleteRuns(&api.DeleteQuery{
			Ids:     []uuid.UUID{runId},
			Force:   Parameters.Force,
			Options: api.Options{GroupId: Parameters.GroupId},
		})
		if err != nil {
			Parameters.Err = fmt.Errorf("failed to delete run: %w", err)
			return
		}
		Parameters.CurrentRunId = runId
	},
}
var deleteRunParams AllParameters

func syncDeleteRunParams() {
	Parameters.Force = deleteRunParams.Force
}

func init() {
	deleteCmd.AddCommand(deleteRunCmd)
	initFlags := func() error {
		deleteRunCmd.ResetFlags()
		deleteRunCmd.Flags().BoolVarP(&deleteRunParams.Force, "force", "f", false, fmt.Sprintf("force delete (required for in progress run status)"))
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
