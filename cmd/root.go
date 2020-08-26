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
	"log"
	"strconv"
)
const SeeLogMsg = " (see stepsman.log file for more details - usually resides in ~/.stepsman/stepsman.log)"

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "stepsman",
	Short: "Manage scripts step by step",
	Long: `Stepsman is a command line utility to manage processes such as:
* Installations
* Upgrades
* Migrations
* Tests
* Anything that looks like a checklist`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Script: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

/*
create run -f
create run -f --set status=not-started
delete run 3
stop run 3
skip run 3
move-cursor run 3 --step 4
do run 3
list
list run 3
vet -f // will make all the pre create steps without creating the run
*/

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.stepsman.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	//rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	err := bl.InitBL(cfgFile)
	if err!=nil{
		fmt.Println(err)
		log.Fatal(err)
	}
}

func getStep(run *bl.RunRecord) (*bl.StepRecord, error) {
	step, err := run.GetStep()
	if err != nil {
		msg := fmt.Sprintf("failed to get step with [run id,step id]: [%d,%d]", run.Id, run.Cursor)
		fmt.Println(msg + SeeLogMsg)
		return nil, fmt.Errorf(msg+": %w", err)
	}
	return step, nil
}

func getRun(runId int64) (*bl.RunRecord, error) {
	run, err := bl.GetRun(runId)
	if err != nil {
		msg := fmt.Sprintf("failed to get run with id: %d", runId)
		fmt.Println(msg + SeeLogMsg)
		return nil, fmt.Errorf(msg+": %w", err)
	}
	return run, nil
}

func parseRunId(idStr string) (int64, error) {
	runId, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		msg := "failed to parse run id"
		fmt.Println(msg + SeeLogMsg)
		return -1, fmt.Errorf(msg+": %w", err)
	}
	return runId, nil
}

