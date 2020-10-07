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
	runs, runsRange, err := bl.ListRuns(&p.Query)
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	runRpcRecords, err := RunRecordToRunRPCRecord(runs)
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
	runs, err := bl.GetRuns(p)
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	runRpcRecords, err := RunRecordToRunRPCRecord(runs)
	if err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	return runRpcRecords, nil
}

func RunRecordToRunRPCRecord(runRecords []*dao.RunRecord) ([]dao.RunAPIRecord, error) {
	var runRpcRecords []dao.RunAPIRecord
	for _, runRecord := range runRecords {
		status, err := runRecord.Status.TranslateRunStatus()
		if err != nil {
			return nil, err
		}
		runRpcRecords = append(runRpcRecords, dao.RunAPIRecord{
			Id:              runRecord.Id,
			Key:             runRecord.Key,
			TemplateVersion: runRecord.TemplateVersion,
			TemplateTitle:   runRecord.TemplateTitle,
			Status:          status,
			Template:        runRecord.Template,
			State:           runRecord.State,
		})
	}
	return runRpcRecords, nil
}
