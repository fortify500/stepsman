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
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/dao"
	"github.com/fortify500/stepsman/serve"
	"github.com/google/uuid"
	"github.com/jedib0t/go-pretty/v6/table"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
	"io"
	"os"
	"path"
	"runtime/debug"
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
	CommandDoStep
	CommandListSteps
	CommandListRuns
	CommandGetRun
	CommandUpdateRun
	CommandUpdateStep
	CommandDeleteRun
)

type AllParameters struct {
	// Flags
	GroupId             uuid.UUID
	GroupIdStr          string
	CfgFile             string
	DatabaseVendor      string
	DataSourceName      string
	DatabaseHost        string
	DatabasePort        int64
	DatabaseName        string
	DatabaseSSLMode     bool
	DatabaseAutoMigrate bool
	DatabaseUserName    string
	DatabasePassword    string
	DatabaseSchema      string
	CreateFileName      string
	FileType            string
	RunKey              string
	ServerPort          int64
	ServerAddress       string
	ServerHealthPort    int64
	Step                string
	OnlyTemplateType    string
	Run                 string
	Status              string
	Force               bool
	StatusOwner         string
	Context             string
	//ListQuery
	RangeStart       int64
	RangeEnd         int64
	RangeReturnTotal bool
	SortFields       []string
	SortOrder        string
	Filters          []string
	// Others
	InPromptMode     bool
	InitialInput     string
	CurrentCommand   CommandType
	CurrentStepIndex string
	CurrentRunId     uuid.UUID
	CurrentRun       *api.RunRecord
	FlagsReInit      []func() error
	Err              error
	Label            string
	State            string
}

var Parameters = AllParameters{
	CfgFile:        "",
	RunKey:         "",
	CreateFileName: "",
	Step:           "",
	Run:            "",
	InitialInput:   "",
	CurrentCommand: CommandUndetermined,
	CurrentRunId:   uuid.UUID{},
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
		if errors.As(Parameters.Err, &cmdError) {
			log.Error(cmdError.TechnicalError())
		} else {
			log.Error(Parameters.Err)
		}
		return true
	}
	return false
}
func recoverAndLog(msg string) {
	if p := recover(); p != nil {
		var err error
		var ok bool
		if err, ok = p.(error); ok {
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
		} else {
			err = fmt.Errorf(msg+": %+v", p)
			Parameters.Err = &Error{
				Technical: err,
				Friendly:  msg,
			}
		}
		log.WithField("stack", string(debug.Stack())).Error(err)
	}
}

func InitConfig() {
	var err error
	flag.Parse()
	viper.SetEnvPrefix("STEPSMAN")
	viper.AutomaticEnv() // read in environment variables that match

	logLevel := "error"
	if viper.IsSet("STORE_DIR") {
		StoreDir = viper.GetString("STORE_DIR")
		if strings.Contains(StoreDir, "~") {
			fmt.Println(`{"level":"warn", "msg":"~ are not supported and are treated as a file or directory part"}`)
		}
	}
	if viper.IsSet("LOG_LEVEL") {
		logLevel = strings.ToLower(viper.GetString("LOG_LEVEL"))
	}

	if Parameters.CfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(Parameters.CfgFile)
	} else {
		viper.AddConfigPath(StoreDir)
		viper.SetConfigName(".stepsman")
	}
	// If a config file is found, read it in.
	if err = viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	}
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		panic(fmt.Errorf("failed to parse log level: %s", logLevel))
	}
	if StoreDir != "" {
		_, err = os.Stat(StoreDir)
		if os.IsNotExist(err) {
			err = os.MkdirAll(StoreDir, 0700)
			if err != nil {
				log.Fatal(api.NewLocalizedError("failed to create the .stepsman directory: %w", err))
			}
		} else if err != nil {
			log.Fatal(api.NewLocalizedError("failed to determine existence of .stepsman directory: %w", err))
		}
		LumberJack = &lumberjack.Logger{
			Filename:   path.Join(StoreDir, "stepsman.log"),
			MaxSize:    100, // megabytes
			MaxBackups: 2,
			MaxAge:     1, // days
			Compress:   true,
		}
	}
	if StoreDir == "" {
		InitLogrusALL(os.Stdout, level)
	} else {
		InitLogrusALL(LumberJack, level)
	}

	if viper.IsSet("DB_VENDOR") {
		Parameters.DatabaseVendor = viper.GetString("DB_VENDOR")
	}
	if viper.IsSet("DB_FILE_NAME") {
		Parameters.DataSourceName = viper.GetString("DB_FILE_NAME")
	}
	if viper.IsSet("DB_HOST") {
		Parameters.DatabaseHost = viper.GetString("DB_HOST")
	}
	if viper.IsSet("DB_PORT") {
		Parameters.DatabasePort = viper.GetInt64("DB_PORT")
	}
	if viper.IsSet("DB_NAME") {
		Parameters.DatabaseName = viper.GetString("DB_NAME")
	}
	if viper.IsSet("DB_USER_NAME") {
		Parameters.DatabaseUserName = viper.GetString("DB_USER_NAME")
	}
	if viper.IsSet("DB_SCHEMA") {
		Parameters.DatabaseSchema = viper.GetString("DB_SCHEMA")
	}
	if viper.IsSet("DB_PASSWORD") {
		Parameters.DatabasePassword = viper.GetString("DB_PASSWORD")
	}
	if viper.IsSet("DB_ENABLE_SSL") {
		Parameters.DatabaseSSLMode = viper.GetBool("DB_ENABLE_SSL")
	}
	if viper.IsSet("DB_AUTO_MIGRATE") {
		Parameters.DatabaseAutoMigrate = viper.GetBool("DB_AUTO_MIGRATE")
	}

}

func getRun(options api.Options, id uuid.UUID) (*api.RunRecord, error) {
	run, err := BL.GetRun(options, id)
	if err != nil {
		msg := fmt.Sprintf("failed to get run with id: %s", id)
		return nil, &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return run, nil
}

func detectStartsWithGTLTEquals(trimPrefix string, filter string) string {
	if strings.HasPrefix(trimPrefix, "<=") {
		return "<="
	} else if strings.HasPrefix(trimPrefix, ">=") {
		return ">="
	} else if strings.HasPrefix(trimPrefix, "<>") {
		return "<>"
	} else if strings.HasPrefix(trimPrefix, ">") {
		return ">"
	} else if strings.HasPrefix(trimPrefix, "<") {
		return "<"
	} else if strings.HasPrefix(trimPrefix, "=") {
		return "="
	} else {
		msg := "failed to parse filter"
		Parameters.Err = &Error{
			Technical: fmt.Errorf(msg+" %s", filter),
			Friendly:  msg,
		}
		return ""
	}
}

func parseStepUUID(idStr string) (uuid.UUID, error) {
	uuid4, err := uuid.Parse(idStr)
	if err != nil {
		msg := "failed to parse step uuid"
		return uuid.UUID{}, &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return uuid4, nil
}
func parseRunId(idStr string) (uuid.UUID, error) {
	uuid4, err := uuid.Parse(idStr)
	if err != nil {
		msg := "failed to parse run id"
		return uuid.UUID{}, &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}
	return uuid4, nil
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

func InitLogrusALL(out io.Writer, level log.Level) {
	InitLogrus(out, level)
	api.InitLogrus(out, level)
	dao.InitLogrus(out, level)
	serve.InitLogrus(out, level)
	client.InitLogrus(out, level)
	bl.InitLogrus(out, level)
}
func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
}
