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
	"errors"
	"fmt"
	"github.com/fortify500/stepsman/bl"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
)

const SeeLogMsg = " (use with \"--help\" or see stepsman.log file for more details ! \"tail ~/.stepsman/stepsman.log\")"
const TableWrapLen = 70

type CommandType int

const (
	CommandUndetermined CommandType = iota
	CommandBang
	CommandCreateRun
	CommandDescribeRun
	CommandDoRun
	CommandListRun
	CommandListRuns
	CommandSkipRun
	CommandStopRun
)

type AllParameters struct {
	// Flags
	CfgFile            string
	CreateFileName     string
	Step               string
	Run                string
	DisableSuggestions bool
	// Others
	InitialInput   string
	CurrentCommand CommandType
	CurrentRunId   int64
	FlagsReInit    []func()
	Err            error
}

var Parameters = AllParameters{
	CfgFile:            "",
	CreateFileName:     "",
	Step:               "",
	Run:                "",
	DisableSuggestions: true,
	InitialInput:       "",
	CurrentCommand:     CommandUndetermined,
	CurrentRunId:       -1,
	FlagsReInit:        []func(){},
}

type CMDError struct {
	Technical error
	Friendly  string
}

func (ce *CMDError) Error() string {
	return ce.Friendly + SeeLogMsg
}
func (ce *CMDError) TechnicalError() error {
	return ce.Technical
}

// Returns true on error
func Execute() bool {
	var cmdError *CMDError
	RootCmd.Execute()
	if Parameters.Err != nil {
		fmt.Println(Parameters.Err)
		if errors.As(Parameters.Err, &cmdError) {
			log.Error(cmdError.TechnicalError())
		} else {
			log.Error(Parameters.Err)
		}
		return true
	}
	return false
}

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
		return nil, &CMDError{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return step, nil
}

func getRun(runId int64) (*bl.RunRecord, error) {
	run, err := bl.GetRun(runId)
	if err != nil {
		msg := fmt.Sprintf("failed to get run with id: %d", runId)
		return nil, &CMDError{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return run, nil
}

func parseRunId(idStr string) (int64, error) {
	runId, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		msg := "failed to parse run id"
		return -1, &CMDError{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return runId, nil
}

func parseStepId(runRecord *bl.RunRecord, idStr string) (int64, error) {
	idStr = strings.TrimSpace(idStr)
	if idStr == "" {
		return -1, nil
	}
	if strings.EqualFold(idStr, "cursor") {
		return runRecord.Cursor, nil
	}
	stepId, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		msg := "failed to parse step id"
		return -1, &CMDError{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return stepId, nil
}

func init() {
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
