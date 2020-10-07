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
package dao

import (
	"encoding/json"
	"io"
)

type ListRunsResponse struct {
	Version string         `json:"jsonrpc"`
	Result  ListRunsResult `json:"result,omitempty"`
	Error   JSONRPCError   `json:"error,omitempty"`
	ID      string         `json:"id,omitempty"`
}

func RemoteListRuns(query *Query) ([]*RunRecord, *RangeResult, error) {
	result := make([]*RunRecord, 0)
	params := ListParams{}
	if query != nil {
		params.Query = *query
	}
	request, err := NewMarshaledJSONRPCRequest("1", LIST_RUNS, &params)
	if err != nil {
		return nil, nil, err
	}
	var rangeResult *RangeResult
	err = remoteJRPCCall(request, func(body *io.ReadCloser) error {
		var jsonRPCResult ListRunsResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return err
		}
		if jsonRPCResult.Result.Data != nil &&
			jsonRPCResult.Result.Range.End >= jsonRPCResult.Result.Range.Start &&
			jsonRPCResult.Result.Range.Start > 0 {
			for _, record := range jsonRPCResult.Result.Data {
				status, err := TranslateToRunStatus(record.Status)
				if err != nil {
					return err
				}
				result = append(result, &RunRecord{
					Id:              record.Id,
					Key:             record.Key,
					TemplateVersion: record.TemplateVersion,
					TemplateTitle:   record.TemplateTitle,
					Status:          status,
					Template:        record.Template,
					State:           record.State,
				})
			}
		}
		rangeResult = &jsonRPCResult.Result.Range
		return err
	})
	return result, rangeResult, err
}

type GetRunsResponse struct {
	Version string        `json:"jsonrpc"`
	Result  GetRunsResult `json:"result,omitempty"`
	Error   JSONRPCError  `json:"error,omitempty"`
	ID      string        `json:"id,omitempty"`
}

func RemoteGetRuns(ids []string) ([]*RunRecord, error) {
	result := make([]*RunRecord, 0)
	request, err := NewMarshaledJSONRPCRequest("1", GET_RUNS, &ids)
	if err != nil {
		return nil, err
	}
	err = remoteJRPCCall(request, func(body *io.ReadCloser) error {
		var jsonRPCResult GetRunsResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return err
		}
		if jsonRPCResult.Result != nil {
			for _, record := range jsonRPCResult.Result {
				status, err := TranslateToRunStatus(record.Status)
				if err != nil {
					return err
				}
				result = append(result, &RunRecord{
					Id:              record.Id,
					Key:             record.Key,
					TemplateVersion: record.TemplateVersion,
					TemplateTitle:   record.TemplateTitle,
					Status:          status,
					Template:        record.Template,
					State:           record.State,
				})
			}
		}
		return err
	})
	return result, err
}
