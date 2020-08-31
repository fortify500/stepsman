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
	"github.com/jedib0t/go-pretty/table"
	log "github.com/sirupsen/logrus"
	"strconv"
)
const SeeLogMsg = " (see stepsman.log file for more details ! \"tail ~/.stepsman/stepsman.log\")"

type AllParameters struct{
	CfgFile string
	InPromptMode bool
	CreateFileName string
	Step int64
}

var Parameters = AllParameters{
	CfgFile: "",
	InPromptMode: false,
	CreateFileName: "",
	Step: -1,
}

var NoBordersStyle = table.Style{
	Name:    "StyleDefault",
	Box:     table.StyleBoxDefault,
	Color:   table.ColorOptionsDefault,
	Format:  table.FormatOptionsDefault,
	HTML:    table.DefaultHTMLOptions,
	Options: table.OptionsNoBordersAndSeparators,
	Title:   table.TitleOptionsDefault,
}
var BordersStyle = table.Style{
	Name:    "StyleDefault",
	Box:     table.StyleBoxDefault,
	Color:   table.ColorOptionsDefault,
	Format:  table.FormatOptionsDefault,
	HTML:    table.DefaultHTMLOptions,
	Options: table.OptionsDefault,
	Title:   table.TitleOptionsDefault,
}


// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the RootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		log.Error(err)
	}
}



/*
create run -f
//create run -f --set status=not-started
* delete run 3
stop run 3
skip run 3
* set run 3 cursor=4
do run 3
describe run 3 [--step 4]
* status run 3 [--step 4]
list runs
list run 35475
//vet -f // will make all the pre create steps without creating the run
*/

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	err := bl.InitBL(Parameters.CfgFile)
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
}

func getCursorStep(run *bl.RunRecord) (*bl.StepRecord, error) {
	step, err := run.GetCursorStep()
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

func init(){
	initConfig()
	// use this later on
	log.SetOutput(bl.Luberjack)

	//mw := io.MultiWriter(os.Stdout, &lumberjack.Logger{
	//	Filename:   path.Join(StoreDir, "stepsman.log"),
	//	MaxSize:    10, // megabytes
	//	MaxBackups: 2,
	//	MaxAge:     1, // days
	//	Compress:   true,
	//})
	//log.SetOutput(mw)
	log.SetLevel(log.TraceLevel)

}
