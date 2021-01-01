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

package api

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/osamingo/jsonrpc"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

var TestMode = false

const (
	RPCListRuns  = "listRuns"
	RPCGetRuns   = "getRuns"
	RPCUpdateRun = "updateRun"
	RPCCreateRun = "createRun"
	RPCDeleteRun = "deleteRun"

	RPCListSteps         = "listSteps"
	RPCGetSteps          = "getSteps"
	RPCUpdateStepByUUID  = "updateStepByUUID"
	RPCUpdateStepByLabel = "updateStepByLabel"
	RPCDoStepByUUID      = "doStepByUUID"
	RPCDoStepByLabel     = "doStepByLabel"
)

const CurrentTimeStamp = "2006-01-02 15:04:05"

type RangeResult struct {
	Range
	Total int64 `json:"total,omitempty"`
}

type RangeQuery struct {
	Range
	ReturnTotal bool `json:"return-total"`
}

type Sort struct {
	Fields []string // ordered left to right
	Order  string   // Either asc/desc
}

type Range struct {
	Start int64
	End   int64
}

type Expression struct {
	AttributeName string `json:"attribute-name"`
	Operator      string // =,>=,>,<=,<,starts-with,ends-with,contains
	Value         interface{}
}

type ListQuery struct {
	Range            RangeQuery   `json:"range,omitempty"`
	Sort             Sort         `json:"sort,omitempty"`
	Filters          []Expression `json:"filters,omitempty"`
	ReturnAttributes []string     `json:"return-attributes,omitempty"`
	Options          Options      `json:"options,omitempty"`
}
type ListParams ListQuery
type ListRunsResult struct {
	Range RangeResult `json:"range,omitempty"`
	Data  []RunRecord `json:"data,omitempty"`
}

type GetRunsQuery struct {
	Ids              []uuid.UUID `json:"ids,omitempty"`
	ReturnAttributes []string    `json:"return-attributes,omitempty"`
	Options          Options     `json:"options,omitempty"`
}
type GetRunsResult []RunRecord
type GetRunsParams GetRunsQuery

type UpdateQueryById struct {
	Id      uuid.UUID              `json:"id,omitempty"`
	Force   bool                   `json:"force,omitempty"`
	Changes map[string]interface{} `json:"changes,omitempty"`
	Options Options                `json:"options,omitempty"`
}

type UpdateQueryByUUID struct {
	UUID        uuid.UUID              `json:"uuid,omitempty"`
	StatusOwner string                 `json:"status-owner,omitempty"`
	Force       bool                   `json:"force,omitempty"`
	Changes     map[string]interface{} `json:"changes,omitempty"`
	Options     Options                `json:"options,omitempty"`
}

type UpdateQueryByLabel struct {
	RunId       uuid.UUID              `json:"run-id,omitempty"`
	StatusOwner string                 `json:"status-owner,omitempty"`
	Label       string                 `json:"label,omitempty"`
	Force       bool                   `json:"force,omitempty"`
	Changes     map[string]interface{} `json:"changes,omitempty"`
	Options     Options                `json:"options,omitempty"`
}

type UpdateRunParams UpdateQueryById
type UpdateRunResult struct{}

type DeleteRunsParams DeleteQuery
type DeleteRunsResult struct{}

type CreateRunsResult RunRecord
type TemplateContents string
type CreateRunParams struct {
	Key          string           `json:"key,omitempty"`
	Template     TemplateContents `json:"template,omitempty"`
	TemplateType string           `json:"template-type,omitempty"`
	Options      Options          `json:"options,omitempty"`
}
type Options struct {
	GroupId uuid.UUID `json:"group-id,omitempty" validate:"required"`
}
type ListStepsResult struct {
	Range RangeResult  `json:"range,omitempty"`
	Data  []StepRecord `json:"data,omitempty"`
}

type GetStepsQuery struct {
	UUIDs            []uuid.UUID `json:"uuids,omitempty"`
	ReturnAttributes []string    `json:"return-attributes,omitempty"`
	Options          Options     `json:"options,omitempty"`
}
type GetStepsResult []StepRecord
type GetStepsParams GetStepsQuery

type UpdateStepByUUIDParams UpdateQueryByUUID
type UpdateStepByUUIDResult struct{}

type UpdateStepByLabelParams UpdateQueryByLabel
type UpdateStepByLabelResult struct{}

type DoStepByUUIDParams struct {
	UUID        uuid.UUID `json:"uuid,omitempty"`
	Context     Context   `json:"context,omitempty"`
	StatusOwner string    `json:"status-owner,omitempty"`
	Options     Options   `json:"options,omitempty"`
}
type DoStepByUUIDResult struct {
	StatusOwner string `json:"status-owner,omitempty"`
}

type DoStepByLabelParams struct {
	RunId       uuid.UUID `json:"run-id,omitempty"`
	Label       string    `json:"label,omitempty"`
	Context     Context   `json:"context,omitempty"`
	StatusOwner string    `json:"status-owner,omitempty"`
	Options     Options   `json:"options,omitempty"`
}
type DoStepByLabelResult struct {
	UUID        uuid.UUID `json:"uuid,omitempty"`
	StatusOwner string    `json:"status-owner,omitempty"`
}

type DeleteQuery struct {
	Ids     []uuid.UUID `json:"id,omitempty"`
	Force   bool        `json:"force,omitempty"`
	Options Options     `json:"options,omitempty"`
}

type RunRecord struct {
	GroupId         uuid.UUID     `db:"group_id" json:"group-id,omitempty"`
	Id              uuid.UUID     `json:"id,omitempty"`
	Key             string        `json:"key,omitempty"`
	Tags            Tags          `db:"tags" json:"tags,omitempty"`
	CreatedAt       AnyTime       `db:"created_at" json:"created-at,omitempty"`
	CompleteBy      *AnyTime      `db:"complete_by" json:"complete-by,omitempty"`
	Now             AnyTime       `db:"now" json:"now,omitempty"`
	TemplateVersion int           `db:"template_version" json:"template-version,omitempty"`
	TemplateTitle   string        `db:"template_title" json:"template-title,omitempty"`
	Status          RunStatusType `json:"status,omitempty"`
	Template        string        `json:"template,omitempty"`
}

type RunStatusType int

func (r RunStatusType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(r.TranslateRunStatus())
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}
func (r *RunStatusType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	status, err := TranslateToRunStatus(s)
	if err != nil {
		return err
	}
	*r = status
	return nil
}
func (t *TemplateContents) UnmarshalJSON(data []byte) error {
	s := string(data)
	if strings.HasPrefix(s, "\"") {
		var err error
		s, err = strconv.Unquote(s)
		if err != nil {
			return err
		}
	}
	*t = TemplateContents(s)
	return nil
}
func (r RunStatusType) MarshalYAML() (interface{}, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(r.TranslateRunStatus())
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}
func (r *RunStatusType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	var err error
	var status RunStatusType
	err = unmarshal(&s)
	if err != nil {
		return err
	}
	status, err = TranslateToRunStatus(s)
	if err != nil {
		return err
	}
	*r = status
	return nil
}

const (
	RunIdle       RunStatusType = 100
	RunInProgress RunStatusType = 200
	RunDone       RunStatusType = 300
)

func (r *RunRecord) PrettyJSONTemplate() string {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(r.Template)))
	decoder.DisallowUnknownFields()
	var tmp interface{}
	err := decoder.Decode(&tmp)
	if err != nil {
		panic(err)
	}
	prettyBytes, err := json.MarshalIndent(&tmp, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(prettyBytes)
}
func (r *RunRecord) PrettyYamlTemplate() string {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(r.Template)))
	decoder.DisallowUnknownFields()
	var tmp interface{}
	err := decoder.Decode(&tmp)
	if err != nil {
		panic(err)
	}
	prettyBytes, err := yaml.Marshal(&tmp)
	if err != nil {
		panic(err)
	}
	return string(prettyBytes)
}
func (r RunStatusType) TranslateRunStatus() string {
	switch r {
	case RunIdle:
		return "Stopped"
	case RunInProgress:
		return "In Progress"
	case RunDone:
		return "Done"
	default:
		panic(fmt.Errorf("failed to translate run status: %d", r))
	}
}

func TranslateToRunStatus(status string) (RunStatusType, error) {
	switch status {
	case "Stopped":
		return RunIdle, nil
	case "In Progress":
		return RunInProgress, nil
	case "Done":
		return RunDone, nil
	default:
		return RunIdle, NewError(ErrInvalidParams, "failed to translate run status: %s", status)
	}
}

type StepStatusType int

func (s StepStatusType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(s.TranslateStepStatus())
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}
func (s *StepStatusType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	status, err := TranslateToStepStatus(str)
	if err != nil {
		return err
	}
	*s = status
	return nil
}

func (a AnyTime) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(time.Time(a).Format(time.RFC3339))
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}
func (a *AnyTime) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	parsedTime, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return err
	}
	*a = AnyTime(parsedTime)
	return nil
}

func (t Tags) Value() (driver.Value, error) {
	if marshal, err := json.Marshal(t); err != nil {
		return nil, err
	} else {
		return marshal, nil
	}
}
func (t *Tags) Scan(src interface{}) error {
	if err := json.Unmarshal(src.([]byte), t); err != nil {
		return err
	}
	return nil
}

func (c Context) Value() (driver.Value, error) {
	if marshal, err := json.Marshal(c); err != nil {
		return nil, err
	} else {
		return marshal, nil
	}
}
func (c *Context) Scan(src interface{}) error {
	if err := json.Unmarshal(src.([]byte), c); err != nil {
		return err
	}
	return nil
}

func (s State) Value() (driver.Value, error) {
	if marshal, err := json.Marshal(s); err != nil {
		return nil, err
	} else {
		return marshal, nil
	}
}
func (s *State) Scan(src interface{}) error {
	if err := json.Unmarshal(src.([]byte), s); err != nil {
		return err
	}
	return nil
}

func (a *AnyTime) Scan(src interface{}) error {
	var err error
	var result time.Time
	switch v := src.(type) {
	case time.Time:
		result = src.(time.Time)
	case string:
		result, err = time.Parse(CurrentTimeStamp, v)
	case []byte:
		result, err = time.Parse(CurrentTimeStamp, string(src.([]byte)))
	default:
		err = fmt.Errorf("invalid type for current_timestamp")
	}
	if err != nil {
		return err
	}
	*a = AnyTime(result)
	return nil
}

const (
	StepIdle       StepStatusType = 10
	StepPending    StepStatusType = 20
	StepInProgress StepStatusType = 30
	StepFailed     StepStatusType = 40
	StepDone       StepStatusType = 50
)

type AnyTime time.Time
type Parameters map[string]interface{}
type Context map[string]interface{}
type Input map[string]interface{}
type State struct {
	Result interface{} `json:"result,omitempty" mapstructure:"result" yaml:"result"`
	Error  string      `json:"error,omitempty" mapstructure:"error" yaml:"error,omitempty"`
}
type Tags []string
type StepRecord struct {
	GroupId     uuid.UUID      `db:"group_id" json:"group-id,omitempty"`
	CreatedAt   AnyTime        `db:"created_at" json:"created-at,omitempty"`
	RunId       uuid.UUID      `db:"run_id" json:"run-id,omitempty"`
	Index       int            `db:"index" json:"index,omitempty"`
	Tags        Tags           `db:"tags" json:"tags,omitempty"`
	Label       string         `json:"label,omitempty"`
	UUID        uuid.UUID      `json:"uuid,omitempty"`
	Name        string         `json:"name,omitempty"`
	Status      StepStatusType `json:"status,omitempty"`
	StatusOwner string         `db:"status_owner" json:"status-owner,omitempty"`
	Now         AnyTime        `db:"now" json:"now,omitempty"`
	Heartbeat   AnyTime        `json:"heartbeat,omitempty"`
	CompleteBy  *AnyTime       `db:"complete_by" json:"complete-by,omitempty"`
	Context     Context        `db:"context" json:"context,omitempty"`
	RetriesLeft int            `db:"retries_left" json:"retries-left,omitempty"`
	State       State          `json:"state,omitempty"`
}

func (s *StepRecord) PrettyJSONState() string {
	prettyBytes, err := json.MarshalIndent(s.State, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(prettyBytes)
}
func (s *StepRecord) PrettyYamlState() string {
	prettyBytes, err := yaml.Marshal(s.State)
	if err != nil {
		panic(err)
	}
	return string(prettyBytes)
}

func (s StepStatusType) TranslateStepStatus() string {
	switch s {
	case StepIdle:
		return "Idle"
	case StepPending:
		return "Pending"
	case StepInProgress:
		return "In Progress"
	case StepFailed:
		return "Failed"
	case StepDone:
		return "Done"
	default:
		panic(fmt.Errorf("failed to translate step status: %d", s))
	}
}
func TranslateToStepStatus(status string) (StepStatusType, error) {
	switch status {
	case "Idle":
		return StepIdle, nil
	case "Pending":
		return StepPending, nil
	case "In Progress":
		return StepInProgress, nil
	case "Failed":
		return StepFailed, nil
	case "Done":
		return StepDone, nil
	default:
		return StepIdle, NewError(ErrInvalidParams, "failed to translate to step status: %s", status)
	}
}
func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
}

type ErrorCode struct {
	Code    int
	Message string
}

var ErrStatusNotChanged = &ErrorCode{
	Code:    1000,
	Message: "status did not change",
}
var ErrInvalidParams = &ErrorCode{
	Code:    int(jsonrpc.ErrInvalidParams().Code),
	Message: jsonrpc.ErrInvalidParams().Message,
}
var ErrRecordNotFound = &ErrorCode{
	Code:    1001,
	Message: "failed to locate record",
}
var ErrRecordNotAffected = &ErrorCode{
	Code:    1002,
	Message: "failed to affect record",
}
var ErrExternal = &ErrorCode{
	Code:    1003,
	Message: "failed to interact with an external resource",
}
var ErrStepAlreadyInProgress = &ErrorCode{
	Code:    1004,
	Message: "step is already in progress",
}
var ErrRunIsAlreadyDone = &ErrorCode{
	Code:    1005,
	Message: "run is already done, no change is possible",
}
var ErrShuttingDown = &ErrorCode{
	Code:    1006,
	Message: "shutting down server",
}
var ErrPrevStepStatusDoesNotMatch = &ErrorCode{
	Code:    1007,
	Message: "prev step status does not match an in transaction status",
}
var ErrJobQueueUnavailable = &ErrorCode{
	Code:    1008,
	Message: "job queue may be full or unresponsive",
}
var ErrStepNoRetriesLeft = &ErrorCode{
	Code:    1009,
	Message: "step status cannot be changed to in progress because no retries are left",
}
var ErrTemplateEvaluationFailed = &ErrorCode{
	Code:    1010,
	Message: "failed to evaluate a template expression",
}
var ErrStepDoneCannotBeChanged = &ErrorCode{
	Code:    1011,
	Message: "step is already done and we rely on it to not be changed",
}
var ErrCannotDeleteRunIsInProgress = &ErrorCode{
	Code:    1012,
	Message: "a run cannot be deleted if in progress, unless force is specified",
}
var ErrCustomTemplateErrorThrown = &ErrorCode{
	Code:    1013,
	Message: "an intentional logical error was thrown by the specified template",
}

var ErrorCodes = map[int64]*ErrorCode{
	int64(ErrStatusNotChanged.Code):            ErrStatusNotChanged,
	int64(ErrInvalidParams.Code):               ErrInvalidParams,
	int64(ErrRecordNotFound.Code):              ErrRecordNotFound,
	int64(ErrRecordNotAffected.Code):           ErrRecordNotAffected,
	int64(ErrExternal.Code):                    ErrExternal,
	int64(ErrStepAlreadyInProgress.Code):       ErrStepAlreadyInProgress,
	int64(ErrRunIsAlreadyDone.Code):            ErrRunIsAlreadyDone,
	int64(ErrShuttingDown.Code):                ErrShuttingDown,
	int64(ErrPrevStepStatusDoesNotMatch.Code):  ErrPrevStepStatusDoesNotMatch,
	int64(ErrJobQueueUnavailable.Code):         ErrJobQueueUnavailable,
	int64(ErrStepNoRetriesLeft.Code):           ErrStepNoRetriesLeft,
	int64(ErrTemplateEvaluationFailed.Code):    ErrTemplateEvaluationFailed,
	int64(ErrStepDoneCannotBeChanged.Code):     ErrStepDoneCannotBeChanged,
	int64(ErrCannotDeleteRunIsInProgress.Code): ErrCannotDeleteRunIsInProgress,
	int64(ErrCustomTemplateErrorThrown.Code):   ErrCustomTemplateErrorThrown,
}

type ErrorCaller struct {
	File string
	Line int
}
type Error struct {
	msg    string
	code   *ErrorCode
	err    error
	data   interface{}
	input  Input
	caller *ErrorCaller
	stack  []byte //only available if debug is enabled.
}

func NewLocalizedError(msg string, args ...interface{}) error {
	_, file, line, ok := runtime.Caller(1)
	if ok {
		return fmt.Errorf(fmt.Sprintf("[%s:%d]: %s", file, line, msg), args...)
	}
	return fmt.Errorf(msg, args...)
}
func NewError(code *ErrorCode, msg string, args ...interface{}) *Error {
	return NewWrapErrorInternal(code, nil, nil, nil, msg, args...)
}
func NewErrorWithInput(code *ErrorCode, input Input, msg string, args ...interface{}) *Error {
	return NewWrapErrorInternal(code, nil, nil, input, msg, args...)
}
func NewErrorWithData(code *ErrorCode, data interface{}, msg string, args ...interface{}) *Error {
	return NewWrapErrorInternal(code, nil, data, nil, msg, args...)
}
func NewWrapErrorWithInput(code *ErrorCode, wrapErr error, input Input, msg string, args ...interface{}) *Error {
	return NewWrapErrorInternal(code, wrapErr, nil, input, msg, args...)
}
func NewWrapError(code *ErrorCode, wrapErr error, msg string, args ...interface{}) *Error {
	return NewWrapErrorInternal(code, wrapErr, nil, nil, msg, args...)
}
func NewWrapErrorInternal(code *ErrorCode, wrapErr error, data interface{}, input Input, msg string, args ...interface{}) *Error {
	newErr := &Error{
		msg:  fmt.Errorf(msg, args...).Error(),
		code: code,
		err:  wrapErr,
		data: data,
	}
	if log.IsLevelEnabled(log.DebugLevel) {
		newErr.stack = debug.Stack()
		newErr.input = input
	}
	if log.IsLevelEnabled(log.ErrorLevel) {
		_, file, line, ok := runtime.Caller(2)
		index := strings.Index(file, "github.com")
		if index > 0 {
			file = file[index:]
		} else {
			file = ""
		}
		if ok {
			newErr.caller = &ErrorCaller{
				File: file,
				Line: line,
			}
		}
	}
	return newErr
}

func (e *Error) Error() string {
	return e.msg
}
func (e *Error) Caller() *ErrorCaller {
	return e.caller
}
func (e *Error) Stack() []byte {
	return e.stack
}
func (e *Error) Data() interface{} {
	return e.data
}
func (e *Error) Input() Input {
	return e.input
}
func (e *Error) Code() *ErrorCode {
	return e.code
}
func (e *Error) Unwrap() error {
	return e.err
}

func ResolveErrorAndLog(err error, propagate bool) *Error {
	var apiErr *Error
	if errors.As(err, &apiErr) {
		var dataJson string
		var inputJson string
		if apiErr.Data() != nil {
			var mErr error
			var data []byte
			data, mErr = json.Marshal(apiErr.Data())
			if mErr != nil {
				panic(fmt.Errorf("marshal for data in error should always succeed: %w", err))
			}
			dataJson = string(data)
		}
		if apiErr.Input() != nil {
			var mErr error
			var input []byte
			input, mErr = json.Marshal(apiErr.Input())
			if mErr != nil {
				panic(fmt.Errorf("marshal for input in error should always succeed: %w", err))
			}
			inputJson = string(input)
		}
		if stack := apiErr.Stack(); stack != nil && len(stack) > 0 {
			caller := apiErr.Caller()
			defer log.
				WithField("input", inputJson).
				WithField("propagate", propagate).
				WithField("code", apiErr.Code().Code).
				WithField("code-msg", apiErr.Code().Message).
				WithField("data", dataJson).
				WithField("file", caller.File).
				WithField("line", caller.Line).
				WithField("stack", string(stack)).
				Error(err)
		} else if caller := apiErr.Caller(); caller != nil {
			log.
				WithField("input", inputJson).
				WithField("propagate", propagate).
				WithField("code", apiErr.Code().Code).
				WithField("code-msg", apiErr.Code().Message).
				WithField("data", dataJson).
				WithField("file", caller.File).
				WithField("line", caller.Line).
				Error(err)
		} else {
			log.Error(err)
		}
	}
	return apiErr
}
