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
	"time"
)

type ListStepsResponse struct {
	Version string              `json:"jsonrpc"`
	Result  api.ListStepsResult `json:"result,omitempty"`
	Error   JSONRPCError        `json:"error,omitempty"`
	ID      string              `json:"id,omitempty"`
}

func RemoteListSteps(query *api.ListQuery) ([]*dao.StepRecord, *api.RangeResult, error) {
	result := make([]*dao.StepRecord, 0)
	params := api.ListParams{}
	if query != nil {
		params = api.ListParams(*query)
	}
	request, err := NewMarshaledJSONRPCRequest("1", api.RPCListSteps, &params)
	if err != nil {
		return nil, nil, err
	}
	var rangeResult *api.RangeResult
	err = remoteJRPCCall(request, func(body *io.ReadCloser) error {
		var jsonRPCResult ListStepsResponse
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
				err = appendStepToResult(record, &result)
				if err != nil {
					return err
				}
			}
		}
		rangeResult = &jsonRPCResult.Result.Range
		return err
	})
	return result, rangeResult, err
}

func appendStepToResult(record api.StepAPIRecord, result *[]*dao.StepRecord) error {
	status, err := dao.TranslateToStepStatus(record.Status)
	if err != nil {
		return err
	}
	var now time.Time
	var heartbeat time.Time
	if record.Now != "" {
		now, err = time.Parse(time.RFC3339, record.Now)
		if err != nil {
			return err
		}
	}
	if record.HeartBeat != "" {
		heartbeat, err = time.Parse(time.RFC3339, record.HeartBeat)
		if err != nil {
			return err
		}
	}
	*result = append(*result, &dao.StepRecord{
		RunId:      record.RunId,
		Index:      record.Index,
		Label:      record.Label,
		UUID:       record.UUID,
		Name:       record.Name,
		Status:     status,
		StatusUUID: record.StatusUUID,
		Now:        now,
		HeartBeat:  heartbeat,
		State:      record.State,
	})
	return nil
}

type GetStepsResponse struct {
	Version string             `json:"jsonrpc"`
	Result  api.GetStepsResult `json:"result,omitempty"`
	Error   JSONRPCError       `json:"error,omitempty"`
	ID      string             `json:"id,omitempty"`
}

func RemoteGetSteps(query *api.GetStepsQuery) ([]*dao.StepRecord, error) {
	result := make([]*dao.StepRecord, 0)
	request, err := NewMarshaledJSONRPCRequest("1", api.RPCGetSteps, query)
	if err != nil {
		return nil, err
	}
	err = remoteJRPCCall(request, func(body *io.ReadCloser) error {
		var jsonRPCResult GetStepsResponse
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
				err = appendStepToResult(record, &result)
				if err != nil {
					return err
				}
			}
		}
		return err
	})
	return result, err
}
