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

func RemoteListSteps(query *api.ListQuery) ([]api.StepRecord, *api.RangeResult, error) {
	var result []api.StepRecord
	params := api.ListParams{}
	if query != nil {
		params = api.ListParams(*query)
	}
	request := NewMarshaledJSONRPCRequest("1", api.RPCListSteps, &params)
	var rangeResult *api.RangeResult
	err := remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
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

func RemoteGetSteps(query *api.GetStepsQuery) ([]api.StepRecord, error) {
	var result []api.StepRecord
	request := NewMarshaledJSONRPCRequest("1", api.RPCGetSteps, query)
	err := remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
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

type UpdateStepResponse struct {
	Version string               `json:"jsonrpc"`
	Result  api.UpdateStepResult `json:"result,omitempty"`
	Error   JSONRPCError         `json:"error,omitempty"`
	ID      string               `json:"id,omitempty"`
}

func RemoteUpdateStep(query *api.UpdateQuery) error {
	request := NewMarshaledJSONRPCRequest("1", api.RPCUpdateStep, query)
	return remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult UpdateStepResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote update step: %w", err)
		}
		return nil
	})
}

type DoStepResponse struct {
	Version string           `json:"jsonrpc"`
	Result  api.DoStepResult `json:"result,omitempty"`
	Error   JSONRPCError     `json:"error,omitempty"`
	ID      string           `json:"id,omitempty"`
}

func RemoteDoStep(uuid string) (*api.DoStepResult, error) {
	var result *api.DoStepResult
	params := &api.DoStepParams{
		UUID: uuid,
	}
	request := NewMarshaledJSONRPCRequest("1", api.RPCDoStep, params)
	err := remoteJRPCCall(request, func(body *io.ReadCloser) (err error) {
		var jsonRPCResult DoStepResponse
		decoder := json.NewDecoder(*body)
		decoder.DisallowUnknownFields()
		if err = decoder.Decode(&jsonRPCResult); err != nil {
			return err
		}
		err = getJSONRPCError(&jsonRPCResult.Error)
		if err != nil {
			return fmt.Errorf("failed to remote do step: %w", err)
		}
		result = &jsonRPCResult.Result
		return nil
	})
	return result, err
}
