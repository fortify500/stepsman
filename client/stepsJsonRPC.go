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

type ListStepsResponse struct {
	Version string              `json:"jsonrpc"`
	Result  api.ListStepsResult `json:"result,omitempty"`
	Error   JSONRPCError        `json:"error,omitempty"`
	ID      string              `json:"id,omitempty"`
}

func (c *CLI) RemoteListSteps(query *api.ListQuery) ([]api.StepRecord, *api.RangeResult, error) {
	var result []api.StepRecord
	params := api.ListParams{}
	if query != nil {
		params = api.ListParams(*query)
	}
	request := NewMarshaledJSONRPCRequest("1", api.RPCListSteps, &params)
	var rangeResult *api.RangeResult
	err := c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult ListStepsResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote list steps: %w", err)
		}
		if jsonRPCResult.Result.Data != nil &&
			jsonRPCResult.Result.Range.End >= jsonRPCResult.Result.Range.Start &&
			jsonRPCResult.Result.Range.Start > 0 {
			result = jsonRPCResult.Result.Data
			rangeResult = &jsonRPCResult.Result.Range
		}

		return err
	})
	return result, rangeResult, err
}

type GetStepsResponse struct {
	Version string             `json:"jsonrpc"`
	Result  api.GetStepsResult `json:"result,omitempty"`
	Error   JSONRPCError       `json:"error,omitempty"`
	ID      string             `json:"id,omitempty"`
}

func (c *CLI) RemoteGetSteps(query *api.GetStepsQuery) ([]api.StepRecord, error) {
	var result []api.StepRecord
	request := NewMarshaledJSONRPCRequest("1", api.RPCGetSteps, query)
	err := c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult GetStepsResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote get steps: %w", err)
		}
		if jsonRPCResult.Result != nil {
			result = jsonRPCResult.Result
		}
		return err
	})
	return result, err
}

type UpdateStepByUUIDResponse struct {
	Version string                     `json:"jsonrpc"`
	Result  api.UpdateStepByUUIDResult `json:"result,omitempty"`
	Error   JSONRPCError               `json:"error,omitempty"`
	ID      string                     `json:"id,omitempty"`
}

func (c *CLI) RemoteUpdateStepByUUID(query *api.UpdateQueryByUUID) error {
	request := NewMarshaledJSONRPCRequest("1", api.RPCUpdateStepByUUID, query)
	return c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult UpdateStepByUUIDResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote update step by uuid: %w", err)
		}
		return nil
	})
}

type UpdateStepByLabelResponse struct {
	Version string                      `json:"jsonrpc"`
	Result  api.UpdateStepByLabelResult `json:"result,omitempty"`
	Error   JSONRPCError                `json:"error,omitempty"`
	ID      string                      `json:"id,omitempty"`
}

func (c *CLI) RemoteUpdateStepByLabel(query *api.UpdateQueryByLabel) error {
	request := NewMarshaledJSONRPCRequest("1", api.RPCUpdateStepByLabel, query)
	return c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult UpdateStepByLabelResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote update step by label: %w", err)
		}
		return nil
	})
}

type DoStepByUUIDResponse struct {
	Version string                 `json:"jsonrpc"`
	Result  api.DoStepByUUIDResult `json:"result,omitempty"`
	Error   JSONRPCError           `json:"error,omitempty"`
	ID      string                 `json:"id,omitempty"`
}

func (c *CLI) RemoteDoStepByUUID(params *api.DoStepByUUIDParams) (api.DoStepByUUIDResult, error) {
	var result api.DoStepByUUIDResult
	request := NewMarshaledJSONRPCRequest("1", api.RPCDoStepByUUID, params)
	err := c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult DoStepByUUIDResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote do step: %w", err)
		}
		result = jsonRPCResult.Result
		return nil
	})
	return result, err
}

type DoStepByLabelResponse struct {
	Version string                  `json:"jsonrpc"`
	Result  api.DoStepByLabelResult `json:"result,omitempty"`
	Error   JSONRPCError            `json:"error,omitempty"`
	ID      string                  `json:"id,omitempty"`
}

func (c *CLI) RemoteDoStepByLabel(params *api.DoStepByLabelParams) (api.DoStepByLabelResult, error) {
	var result api.DoStepByLabelResult
	request := NewMarshaledJSONRPCRequest("1", api.RPCDoStepByLabel, params)
	err := c.remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult DoStepByLabelResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote do step: %w", err)
		}
		result = jsonRPCResult.Result
		return nil
	})
	return result, err
}
