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

package serve

import (
	"context"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/go-chi/valve"
	"github.com/intel-go/fastjson"
	"github.com/osamingo/jsonrpc"
	"time"
)

type (
	ListStepsHandler struct{}
)

func StepRecordToStepRPCRecord(stepsRecords []*dao.StepRecord, translateStatus bool) ([]api.StepAPIRecord, error) {
	var stepRpcRecords []api.StepAPIRecord
	for _, stepRecord := range stepsRecords {
		var status string
		var err error
		if translateStatus {
			status, err = stepRecord.Status.TranslateStepStatus()
			if err != nil {
				return nil, err
			}
		}
		stepRpcRecords = append(stepRpcRecords, api.StepAPIRecord{
			RunId:      stepRecord.RunId,
			Index:      stepRecord.Index,
			Label:      stepRecord.Label,
			UUID:       stepRecord.UUID,
			Name:       stepRecord.Name,
			Status:     status,
			StatusUUID: stepRecord.StatusUUID,
			HeartBeat:  stepRecord.HeartBeat.(time.Time).Format(time.RFC3339),
			Now:        stepRecord.Now.(time.Time).Format(time.RFC3339),
			State:      stepRecord.State,
		})
	}
	return stepRpcRecords, nil
}
func (h ListStepsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	err := valve.Lever(c).Open()
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var result interface{}
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.ListParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		query := api.ListQuery(p)
		steps, stepsRange, err := bl.ListSteps(&query)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		translateStatus := false
		if query.ReturnAttributes != nil && len(query.ReturnAttributes) > 0 {
			for _, attribute := range query.ReturnAttributes {
				if attribute == dao.Status {
					translateStatus = true
				}
			}
		} else {
			translateStatus = true
		}
		runRpcRecords, err := StepRecordToStepRPCRecord(steps, translateStatus)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		result = api.ListStepsResult{
			Range: *stepsRange,
			Data:  runRpcRecords,
		}
		return nil
	})
	return result, jsonRPCErr
}

type (
	GetStepsHandler struct{}
)

func (h GetStepsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	err := valve.Lever(c).Open()
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var result interface{}
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.GetStepsParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		vetErr := VetIds(p.UUIDs)
		if vetErr != nil {
			return vetErr
		}
		query := api.GetStepsQuery(p)
		steps, err := bl.GetSteps(&query)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		result, err = StepRecordToStepRPCRecord(steps, true)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		return nil
	})

	return result, jsonRPCErr
}
