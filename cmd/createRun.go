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

// createRunCmd represents the createRun command
var createRunCmd = &cobra.Command{
	Use:   "run",
	Args: cobra.MaximumNArgs(0),
	Short: "Create a run and move the cursor to the first step",
	Long: `Create a run and move the cursor to the first step.
You can specify either a file or stdin (stdin not implemented yet)`,
	RunE: func(cmd *cobra.Command, args []string) error{
		var t bl.Script
		if len(Parameters.CreateFileName) == 0 {
			msg := "you must specify a file name"
			fmt.Println(msg)
			return fmt.Errorf(msg)
		}
		runRow, err := t.Start(Parameters.CreateFileName)
		if err == bl.ErrActiveRunsWithSameTitleExists {
			msg := "you must stop runs with the same title before creating a new run"
			//"you must either stop runs with the same title or force an additional run (see --force-run)"
			fmt.Println(msg + SeeLogMsg)
			return fmt.Errorf(msg+": %w", err)
		} else if err != nil {
			msg:= "failed to create run"
			fmt.Println(msg + SeeLogMsg)
			return fmt.Errorf(msg + ": %w", err)
		}
		msg:=fmt.Sprintf("run created with id: %d", runRow.Id)
		fmt.Println(msg)
		log.Info(msg)
		log.Debug("run: %#v", t)
		return nil
	},
}

func init() {
	createCmd.AddCommand(createRunCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// createRunCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	createRunCmd.Flags().StringVarP(&Parameters.CreateFileName, "file", "f", "", "File to create run")
	createRunCmd.MarkFlagRequired("file")
}
