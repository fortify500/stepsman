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
	"github.com/spf13/cobra"
)

var getRunCmd = &cobra.Command{
	Use:   "run",
	Args:  cobra.ExactArgs(1),
	Short: "Run summary.",
	Long:  `Get run summary.`,
	Run: func(cmd *cobra.Command, args []string) {
		defer recoverAndLog("failed to get runs")
		runId, err := parseRunId(args[0])
		if err != nil {
			Parameters.Err = err
			return
		}
		Parameters.CurrentCommand = CommandGetRun
		if Parameters.OnlyTemplateType != "" {
			switch Parameters.OnlyTemplateType {
			case "yaml":
			case "json":
			default:
				msg := fmt.Sprintf("invalid-only-template-type value: %s", Parameters.OnlyTemplateType)
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
			var run *api.RunRecord
			run, err = getRun(runId)
			if err != nil {
				Parameters.Err = err
				return
			}
			switch Parameters.OnlyTemplateType {
			case "yaml":
				fmt.Print(run.PrettyYamlTemplate())
			case "json":
				fmt.Print(run.PrettyJSONTemplate())
			default:
				msg := fmt.Sprintf("invalid-only-template-type value: %s", Parameters.OnlyTemplateType)
				Parameters.Err = &Error{
					Technical: fmt.Errorf(msg+": %w", err),
					Friendly:  msg,
				}
				return
			}
		} else {
			listRunsInternal(runId)
		}
	},
}

func init() {
	getCmd.AddCommand(getRunCmd)
	initFlags := func() error {
		getRunCmd.ResetFlags()
		getRunCmd.Flags().StringVar(&Parameters.OnlyTemplateType, "only-template-type", "", "will output a pretty template. possible values [json,yaml]")
		return nil
	}
	Parameters.FlagsReInit = append(Parameters.FlagsReInit, initFlags)
}
