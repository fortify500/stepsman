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
package dao

type (
	RunAPIRecord struct {
		Id     int64
		UUID   string
		Title  string
		Cursor int64
		Status string
		Script string
	}
)

const (
	LIST_RUNS = "listRuns"
)

type Range struct {
	Start int64
	End   int64
}

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
type Expression struct {
	AttributeName string `json:"attribute-name"`
	Operator      string // =,>=,>,<=,<,starts-with,ends-with,contains
	Value         string
}

type Query struct {
	Range   RangeQuery
	Sort    Sort
	Filters []Expression
}

type ListRunsResult struct {
	Range RangeResult    `json:"range,omitempty"`
	Data  []RunAPIRecord `json:"data,omitempty"`
}

type ListRunsParams struct {
	Query Query
}
