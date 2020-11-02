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
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io"
	"time"
)

const (
	RPCListRuns  = "listRuns"
	RPCGetRuns   = "getRuns"
	RPCUpdateRun = "updateRun"
	RPCCreateRun = "createRun"

	RPCListSteps = "listSteps"
	RPCGetSteps  = "getSteps"
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
	Value         string
}

type ListQuery struct {
	Range            RangeQuery   `json:"range,omitempty"`
	Sort             Sort         `json:"sort,omitempty"`
	Filters          []Expression `json:"filters,omitempty"`
	ReturnAttributes []string     `json:"return-attributes,omitempty"`
}
type ListParams ListQuery
type ListRunsResult struct {
	Range RangeResult `json:"range,omitempty"`
	Data  []RunRecord `json:"data,omitempty"`
}

type GetRunsQuery struct {
	Ids              []string `json:"ids,omitempty"`
	ReturnAttributes []string `json:"return-attributes,omitempty"`
}
type GetRunsResult []RunRecord
type GetRunsParams GetRunsQuery

type UpdateQuery struct {
	Id      string                 `json:"id,omitempty"`
	Changes map[string]interface{} `json:"changes,omitempty"`
}
type UpdateRunParams UpdateQuery
type UpdateRunsResult struct{}

type CreateRunsResult RunRecord
type CreateRunParams struct {
	Key      string      `json:"key,omitempty"`
	Template interface{} `json:"template,omitempty"`
}

type ListStepsResult struct {
	Range RangeResult  `json:"range,omitempty"`
	Data  []StepRecord `json:"data,omitempty"`
}

type GetStepsQuery struct {
	UUIDs            []string `json:"uuids,omitempty"`
	ReturnAttributes []string `json:"return-attributes,omitempty"`
}
type GetStepsResult []StepRecord
type GetStepsParams GetStepsQuery

type RunRecord struct {
	Id              string        `json:"id,omitempty"`
	Key             string        `json:"key,omitempty"`
	TemplateVersion int64         `db:"template_version" json:"template-version,omitempty"`
	TemplateTitle   string        `db:"template_title" json:"template-title,omitempty"`
	Status          RunStatusType `json:"status,omitempty"`
	Template        string        `json:"template,omitempty"`
}

type RunStatusType int64

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

const (
	RunIdle       RunStatusType = 10
	RunInProgress RunStatusType = 12
	RunDone       RunStatusType = 15
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
		return RunIdle, fmt.Errorf("failed to translate run status: %s", status)
	}
}

type StepStatusType int64

func (s StepStatusType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(s.TranslateStepStatus())
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}
func (s StepStatusType) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	s, err := TranslateToStepStatus(str)
	if err != nil {
		return err
	}
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

func (a *AnyTime) Scan(src interface{}) error {
	var err error
	var result time.Time
	switch v := src.(type) {
	case time.Time:
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
	StepIdle       StepStatusType = 0
	StepInProgress StepStatusType = 2
	StepFailed     StepStatusType = 4
	StepDone       StepStatusType = 5
)

type AnyTime time.Time
type StepRecord struct {
	RunId      string         `db:"run_id" json:"run-id,omitempty"`
	Index      int64          `db:"index" json:"index,omitempty"`
	Label      string         `json:"label,omitempty"`
	UUID       string         `json:"uuid,omitempty"`
	Name       string         `json:"name,omitempty"`
	Status     StepStatusType `json:"status,omitempty"`
	StatusUUID string         `db:"status_uuid" json:"status-uuid,omitempty"`
	Now        AnyTime        `json:"now,omitempty"`
	Heartbeat  AnyTime        `json:"heartbeat,omitempty"`
	State      string         `json:"state,omitempty"`
}

func (s *StepRecord) PrettyJSONState() string {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(s.State)))
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
func (s *StepRecord) PrettyYamlState() string {
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(s.State)))
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

func (s StepStatusType) TranslateStepStatus() string {
	switch s {
	case StepIdle:
		return "Idle"
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
	case "In Progress":
		return StepInProgress, nil
	case "Failed":
		return StepFailed, nil
	case "Done":
		return StepDone, nil
	default:
		return StepIdle, fmt.Errorf("failed to translate statys to step status")
	}
}
func InitLogrus(out io.Writer) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.TraceLevel)
	log.SetOutput(out)
}
