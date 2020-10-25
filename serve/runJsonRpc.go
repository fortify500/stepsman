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
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/go-chi/valve"
	"github.com/google/uuid"
	"github.com/intel-go/fastjson"
	"github.com/osamingo/jsonrpc"
)

type (
	ListRunsHandler struct{}
)

func (h ListRunsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	err := valve.Lever(c).Open()
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var p dao.ListParams
	if params != nil {
		if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
			return nil, errResult
		}
	}
	query := dao.ListQuery(p)
	runs, runsRange, err := bl.ListRuns(&query)
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	translateStatus := false
	if query.ReturnAttributes != nil && len(query.ReturnAttributes) > 0 {
		for _, attribute := range query.ReturnAttributes {
			if attribute == "status" {
				translateStatus = true
			}
		}
	} else {
		translateStatus = true
	}
	runRpcRecords, err := RunRecordToRunRPCRecord(runs, translateStatus)
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	return dao.ListRunsResult{
		Range: *runsRange,
		Data:  runRpcRecords,
	}, nil
}

type (
	GetRunsHandler struct{}
)

func (h GetRunsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	err := valve.Lever(c).Open()
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var p dao.GetParams
	if params != nil {
		if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
			return nil, errResult
		}
	}
	vetErr := VetIds(p.Ids)
	if vetErr != nil {
		return nil, vetErr
	}
	query := dao.GetQuery(p)
	runs, err := bl.GetRuns(&query)
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	runRpcRecords, err := RunRecordToRunRPCRecord(runs, true)
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	return runRpcRecords, nil
}

func VetIds(ids []string) *jsonrpc.Error {
	if ids != nil {
		for _, id := range ids {
			_, err := uuid.Parse(id)
			if err != nil {
				return jsonrpc.ErrInvalidParams()
			}
		}
	}
	return nil
}
func RunRecordToRunRPCRecord(runRecords []*dao.RunRecord, translateStatus bool) ([]dao.RunAPIRecord, error) {
	var runRpcRecords []dao.RunAPIRecord
	for _, runRecord := range runRecords {
		var status string
		var err error
		if translateStatus {
			status, err = runRecord.Status.TranslateRunStatus()
			if err != nil {
				return nil, err
			}
		}
		runRpcRecords = append(runRpcRecords, dao.RunAPIRecord{
			Id:              runRecord.Id,
			Key:             runRecord.Key,
			TemplateVersion: runRecord.TemplateVersion,
			TemplateTitle:   runRecord.TemplateTitle,
			Status:          status,
			Template:        runRecord.Template,
		})
	}
	return runRpcRecords, nil
}

type (
	UpdateRunHandler struct{}
)

func (h UpdateRunHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	err := valve.Lever(c).Open()
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var p dao.UpdateRunParams
	if params != nil {
		if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
			return nil, errResult
		}
	}
	vetErr := VetIds([]string{p.Id})
	if vetErr != nil {
		return nil, vetErr
	}
	if len(p.Changes) > 0 {
		if len(p.Changes) != 1 {
			return nil, jsonrpc.ErrInvalidParams()
		}
		val, ok := p.Changes["status"]
		if !ok {
			return nil, jsonrpc.ErrInvalidParams()
		}
		statusStr, ok := val.(string)
		if !ok {
			return nil, jsonrpc.ErrInvalidParams()
		}
		newStatus, err := dao.TranslateToRunStatus(statusStr)
		if err != nil {
			return nil, &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInvalidParams,
				Message: err.Error(),
			}
		}
		err = bl.UpdateRunStatus(p.Id, newStatus)
		if err != nil {
			return nil, &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
	}

	return &dao.UpdateRunsResult{}, nil
}
