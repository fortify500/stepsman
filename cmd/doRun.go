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
	"github.com/spf13/cobra"
)

// doRunCmd represents the doRun command
var doRunCmd = &cobra.Command{
	Use:   "run",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	RunE: func(cmd *cobra.Command, args []string) error{
		runId, err := parseRunId(args[0])
		if err != nil {
			return err
		}
		run, err := getRun(runId)
		if err != nil {
			return err
		}
		stepRecord, err := getStep(run)
		if err != nil {
			return err
		}
		step, err := stepRecord.ToStep()
		if err != nil {
			msg := "failed to convert step record to step"
			fmt.Println(msg + SeeLogMsg)
			return fmt.Errorf(msg+": %w", err)
		}
		err = step.StartDo()
		if err != nil {
			msg := "failed to start do"
			fmt.Println(msg + SeeLogMsg)
			return fmt.Errorf(msg+": %w", err)
		}
		return nil
	},
}

func init() {
	doCmd.AddCommand(doRunCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// doRunCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only doRun when this command
	// is called directly, e.g.:
	// doRunCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
