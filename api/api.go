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

const (
	RPCListRuns  = "listRuns"
	RPCGetRuns   = "getRuns"
	RPCUpdateRun = "updateRun"
	RPCCreateRun = "createRun"

	RPCListSteps = "listSteps"
)

type (
	RunAPIRecord struct {
		Id              string `json:"id,omitempty"`
		Key             string `json:"key,omitempty"`
		TemplateVersion int64  `json:"template-version,omitempty"`
		TemplateTitle   string `json:"template-title,omitempty"`
		Status          string `json:"status,omitempty"`
		Template        string `json:"template,omitempty"`
	}

	StepAPIRecord struct {
		RunId      string `json:"run-id,omitempty"`
		Index      int64  `json:"index,omitempty"`
		Label      string
		UUID       string
		Name       string
		Status     string `json:"status,omitempty"`
		StatusUUID string `json:"status-uuid,omitempty"`
		HeartBeat  string `json:"heartbeat,omitempty"`
		Now        string
		State      string
	}
)

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
	Range RangeResult    `json:"range,omitempty"`
	Data  []RunAPIRecord `json:"data,omitempty"`
}

type GetQuery struct {
	Ids              []string `json:"ids,omitempty"`
	ReturnAttributes []string `json:"return-attributes,omitempty"`
}
type GetRunsResult []RunAPIRecord
type GetParams GetQuery

type UpdateQuery struct {
	Id      string                 `json:"id,omitempty"`
	Changes map[string]interface{} `json:"changes,omitempty"`
}
type UpdateRunParams UpdateQuery
type UpdateRunsResult struct{}

type CreateRunsResult RunAPIRecord
type CreateRunParams struct {
	Key      string      `json:"key,omitempty"`
	Template interface{} `json:"template,omitempty"`
}

type ListStepsResult struct {
	Range RangeResult     `json:"range,omitempty"`
	Data  []StepAPIRecord `json:"data,omitempty"`
}
