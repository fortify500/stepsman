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

package client

import (
	"encoding/json"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/dao"
	"io"
)

type ListRunsResponse struct {
	Version string             `json:"jsonrpc"`
	Result  api.ListRunsResult `json:"result,omitempty"`
	Error   JSONRPCError       `json:"error,omitempty"`
	ID      string             `json:"id,omitempty"`
}

func RemoteListRuns(query *api.ListQuery) ([]*dao.RunRecord, *api.RangeResult, error) {
	result := make([]*dao.RunRecord, 0)
	params := api.ListParams{}
	if query != nil {
		params = api.ListParams(*query)
	}
	request, err := NewMarshaledJSONRPCRequest("1", api.RPCListRuns, &params)
	if err != nil {
		return nil, nil, err
	}
	var rangeResult *api.RangeResult
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
				status, err := dao.TranslateToRunStatus(record.Status)
				if err != nil {
					return err
				}
				result = append(result, &dao.RunRecord{
					Id:              record.Id,
					Key:             record.Key,
					TemplateVersion: record.TemplateVersion,
					TemplateTitle:   record.TemplateTitle,
					Status:          status,
					Template:        record.Template,
				})
			}
		}
		rangeResult = &jsonRPCResult.Result.Range
		return err
	})
	return result, rangeResult, err
}

type GetRunsResponse struct {
	Version string            `json:"jsonrpc"`
	Result  api.GetRunsResult `json:"result,omitempty"`
	Error   JSONRPCError      `json:"error,omitempty"`
	ID      string            `json:"id,omitempty"`
}

func RemoteGetRuns(query *api.GetQuery) ([]*dao.RunRecord, error) {
	result := make([]*dao.RunRecord, 0)
	request, err := NewMarshaledJSONRPCRequest("1", api.RPCGetRuns, query)
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
				status, err := dao.TranslateToRunStatus(record.Status)
				if err != nil {
					return err
				}
				result = append(result, &dao.RunRecord{
					Id:              record.Id,
					Key:             record.Key,
					TemplateVersion: record.TemplateVersion,
					TemplateTitle:   record.TemplateTitle,
					Status:          status,
					Template:        record.Template,
				})
			}
		}
		return err
	})
	return result, err
}

type UpdateRunResponse struct {
	Version string               `json:"jsonrpc"`
	Result  api.UpdateRunsResult `json:"result,omitempty"`
	Error   JSONRPCError         `json:"error,omitempty"`
	ID      string               `json:"id,omitempty"`
}

func RemoteUpdateRun(query *api.UpdateQuery) error {
	request, err := NewMarshaledJSONRPCRequest("1", api.RPCUpdateRun, query)
	if err != nil {
		return err
	}
	return remoteJRPCCall(request, func(body *io.ReadCloser) error {
		var jsonRPCResult UpdateRunResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return err
		}
		return nil
	})
}

type CreateRunResponse struct {
	Version string               `json:"jsonrpc"`
	Result  api.CreateRunsResult `json:"result,omitempty"`
	Error   JSONRPCError         `json:"error,omitempty"`
	ID      string               `json:"id,omitempty"`
}

func RemoteCreateRun(params *api.CreateRunParams) (string, string, dao.RunStatusType, error) {
	var runId string
	key := params.Key
	var status dao.RunStatusType
	request, err := NewMarshaledJSONRPCRequest("1", api.RPCCreateRun, params)
	if err != nil {
		return "", "", dao.RunIdle, err
	}
	err = remoteJRPCCall(request, func(body *io.ReadCloser) error {
		var jsonRPCResult CreateRunResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return err
		}
		status, err = dao.TranslateToRunStatus(jsonRPCResult.Result.Status)
		if err != nil {
			return err
		}
		runId = jsonRPCResult.Result.Id
		if jsonRPCResult.Result.Key != "" {
			key = jsonRPCResult.Result.Key
		}
		return nil
	})
	if err != nil {
		return "", "", dao.RunIdle, err
	}
	return runId, key, status, nil
}
