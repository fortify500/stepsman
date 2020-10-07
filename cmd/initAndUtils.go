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
	"errors"
	"flag"
	"fmt"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/table"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
	"path"
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
	CfgFile          string
	DatabaseVendor   string
	DataSourceName   string
	DatabaseHost     string
	DatabasePort     int64
	DatabaseName     string
	DatabaseSSLMode  bool
	DatabaseUserName string
	DatabasePassword string
	CreateFileName   string
	RunKey           string
	ServerPort       int64
	Step             string
	Run              string
	//Query
	RangeStart       int64
	RangeEnd         int64
	RangeReturnTotal bool
	SortFields       []string
	SortOrder        string
	Filters          []string
	// Others
	InitialInput   string
	CurrentCommand CommandType
	CurrentRunId   string
	CurrentRun     *dao.RunRecord
	FlagsReInit    []func() error
	Err            error
}

var Parameters = AllParameters{
	CfgFile:        "",
	RunKey:         "",
	CreateFileName: "",
	Step:           "",
	Run:            "",
	InitialInput:   "",
	CurrentCommand: CommandUndetermined,
	CurrentRunId:   "",
	FlagsReInit:    []func() error{},
}

var StoreDir string
var LumberJack *lumberjack.Logger

type Error struct {
	Technical error
	Friendly  string
}

func (ce *Error) Error() string {
	return ce.Friendly + SeeLogMsg
}
func (ce *Error) TechnicalError() error {
	return ce.Technical
}

// Returns true on error
func Execute() bool {
	var cmdError *Error
	_ = RootCmd.Execute()
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

func InitConfig() {
	flag.Parse()
	dir, err := homedir.Dir()
	if err != nil {
		err = fmt.Errorf("failed to detect home directory: %w", err)
		fmt.Println(err)
		log.Fatal(err)
	}
	StoreDir = path.Join(dir, ".stepsman")

	if Parameters.CfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(Parameters.CfgFile)
	} else {
		viper.AddConfigPath(StoreDir)
		viper.SetConfigName(".stepsman")
	}

	viper.AutomaticEnv() // read in environment variables that match
	LumberJack = &lumberjack.Logger{
		Filename:   path.Join(StoreDir, "stepsman.log"),
		MaxSize:    100, // megabytes
		MaxBackups: 2,
		MaxAge:     1, // days
		Compress:   true,
	}
	log.SetFormatter(&log.JSONFormatter{})
	// use this later on
	log.SetOutput(LumberJack)

	//mw := io.MultiWriter(os.Stdout, &lumberjack.Logger{
	//	Filename:   path.Join(StoreDir, "stepsman.log"),
	//	MaxSize:    10, // megabytes
	//	MaxBackups: 2,
	//	MaxAge:     1, // days
	//	Compress:   true,
	//})
	//log.SetOutput(mw)
	log.SetLevel(log.TraceLevel)

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	}

	if viper.IsSet("db-vendor") {
		Parameters.DatabaseVendor = viper.GetString("db-vendor")
	}
	if viper.IsSet("db-file-name") {
		Parameters.DataSourceName = viper.GetString("db-file-name")
	}
	if viper.IsSet("db-host") {
		Parameters.DatabaseHost = viper.GetString("db-host")
	}
	if viper.IsSet("db-port") {
		Parameters.DatabasePort = viper.GetInt64("db-port")
	}
	if viper.IsSet("db-name") {
		Parameters.DatabaseName = viper.GetString("db-name")
	}
	if viper.IsSet("db-user-name") {
		Parameters.DatabaseUserName = viper.GetString("db-user-name")
	}
	if viper.IsSet("db-password") {
		Parameters.DatabasePassword = viper.GetString("db-password")
	}
	if viper.IsSet("db-enable-ssl") {
		Parameters.DatabaseSSLMode = viper.GetBool("db-enable-ssl")
	}

	_, err = os.Stat(StoreDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(StoreDir, 0700)
		if err != nil {
			err = fmt.Errorf("failed to create the .stepsman diretory: %w", err)
			fmt.Println(err)
			log.Fatal(err)
		}
	} else if err != nil {
		err = fmt.Errorf("failed to determine existance of .stepsman directory: %w", err)
		fmt.Println(err)
		log.Fatal(err)
	}
}

func GetNotDoneAndNotSkippedStep(run *dao.RunRecord) (*dao.StepRecord, error) {
	step, err := bl.GetNotDoneAndNotSkippedStep(run)
	if err != nil {
		msg := fmt.Sprintf("failed to get step with [run id]: [%s]", run.Id)
		return nil, &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return step, nil
}

func getRun(id string) (*dao.RunRecord, error) {
	run, err := bl.GetRun(id)
	if err != nil {
		msg := fmt.Sprintf("failed to get run with id: %s", id)
		return nil, &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return run, nil
}

func parseRunId(idStr string) (string, error) {
	uuid4, err := uuid.Parse(idStr)
	if err != nil {
		msg := "failed to parse run id"
		return "", &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return strings.ToLower(uuid4.String()), nil
}

func parseIndex(idStr string) (int64, error) {
	idStr = strings.TrimSpace(idStr)
	if idStr == "" {
		return -1, nil
	}
	index, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		msg := "failed to parse Index"
		return -1, &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return index, nil
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
