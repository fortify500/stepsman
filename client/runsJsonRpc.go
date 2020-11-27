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
	"fmt"
	"github.com/fortify500/stepsman/api"
	"io"
)

type ListRunsResponse struct {
	Version string             `json:"jsonrpc"`
	Result  api.ListRunsResult `json:"result,omitempty"`
	Error   JSONRPCError       `json:"error,omitempty"`
	ID      string             `json:"id,omitempty"`
}

func (c *CLI) RemoteListRuns(query *api.ListQuery) ([]api.RunRecord, *api.RangeResult, error) {
	var result []api.RunRecord
	params := api.ListParams{}
	if query != nil {
		params = api.ListParams(*query)
	}
	request := NewMarshaledJSONRPCRequest("1", api.RPCListRuns, &params)
	var rangeResult *api.RangeResult
	err := c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult ListRunsResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote list runs: %w", err)
		}
		if jsonRPCResult.Result.Data != nil &&
			jsonRPCResult.Result.Range.End >= jsonRPCResult.Result.Range.Start &&
			jsonRPCResult.Result.Range.Start > 0 {
			rangeResult = &jsonRPCResult.Result.Range
			result = jsonRPCResult.Result.Data
		}

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

func (c *CLI) RemoteGetRuns(query *api.GetRunsQuery) ([]api.RunRecord, error) {
	var result []api.RunRecord
	request := NewMarshaledJSONRPCRequest("1", api.RPCGetRuns, query)
	err := c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult GetRunsResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote get runs: %w", err)
		}
		if jsonRPCResult.Result != nil {
			result = jsonRPCResult.Result
		}
		return err
	})
	return result, err
}

type UpdateRunResponse struct {
	Version string              `json:"jsonrpc"`
	Result  api.UpdateRunResult `json:"result,omitempty"`
	Error   JSONRPCError        `json:"error,omitempty"`
	ID      string              `json:"id,omitempty"`
}

func (c *CLI) RemoteUpdateRun(query *api.UpdateQuery) error {
	request := NewMarshaledJSONRPCRequest("1", api.RPCUpdateRun, query)
	return c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult UpdateRunResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote update run: %w", err)
		}
		return nil
	})
}

type DeleteRunsResponse struct {
	Version string               `json:"jsonrpc"`
	Result  api.DeleteRunsResult `json:"result,omitempty"`
	Error   JSONRPCError         `json:"error,omitempty"`
	ID      string               `json:"id,omitempty"`
}

func (c *CLI) RemoteDeleteRuns(query *api.DeleteQuery) error {
	request := NewMarshaledJSONRPCRequest("1", api.RPCDeleteRun, query)
	return c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult DeleteRunsResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote delete runs: %w", err)
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

func (c *CLI) RemoteCreateRun(params *api.CreateRunParams) (string, string, api.RunStatusType, error) {
	var runId string
	key := params.Key
	var status api.RunStatusType
	request := NewMarshaledJSONRPCRequest("1", api.RPCCreateRun, params)
	err := c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult CreateRunResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote create run:%w", err)
		}
		status = jsonRPCResult.Result.Status
		runId = jsonRPCResult.Result.Id
		key = jsonRPCResult.Result.Key
		return nil
	})
	if err != nil {
		return "", "", api.RunIdle, err
	}
	return runId, key, status, nil
}
