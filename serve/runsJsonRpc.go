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
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/go-chi/valve"
	"github.com/google/uuid"
	"github.com/intel-go/fastjson"
	"github.com/mitchellh/mapstructure"
	"github.com/osamingo/jsonrpc"
	"strings"
)

type (
	ListRunsHandler  struct{}
	GetRunsHandler   struct{}
	CreateRunHandler struct{}
	UpdateRunHandler struct{}
)

func (h ListRunsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	if err := valve.Lever(c).Open(); err != nil {
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
		runs, runsRange, err := bl.ListRuns(&query)
		if err != nil {
			return &jsonrpc.Error{
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
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		result = api.ListRunsResult{
			Range: *runsRange,
			Data:  runRpcRecords,
		}
		return nil
	})
	return result, jsonRPCErr
}

func (h GetRunsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	if err := valve.Lever(c).Open(); err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var result interface{}
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.GetRunsParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		vetErr := VetIds(p.Ids)
		if vetErr != nil {
			return vetErr
		}
		query := api.GetRunsQuery(p)
		runs, err := bl.GetRuns(&query)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		runRpcRecords, err := RunRecordToRunRPCRecord(runs, true)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		result = runRpcRecords
		return nil
	})
	return result, jsonRPCErr
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
func RunRecordToRunRPCRecord(runRecords []*dao.RunRecord, translateStatus bool) ([]api.RunAPIRecord, error) {
	var runRpcRecords []api.RunAPIRecord
	for _, runRecord := range runRecords {
		var status string
		var err error
		if translateStatus {
			status, err = runRecord.Status.TranslateRunStatus()
			if err != nil {
				return nil, err
			}
		}
		runRpcRecords = append(runRpcRecords, api.RunAPIRecord{
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

func (h UpdateRunHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	if err := valve.Lever(c).Open(); err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var result interface{}
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.UpdateRunParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		vetErr := VetIds([]string{p.Id})
		if vetErr != nil {
			return vetErr
		}
		if len(p.Changes) > 0 {
			if len(p.Changes) != 1 {
				return jsonrpc.ErrInvalidParams()
			}
			val, ok := p.Changes["status"]
			if !ok {
				return jsonrpc.ErrInvalidParams()
			}
			statusStr, ok := val.(string)
			if !ok {
				return jsonrpc.ErrInvalidParams()
			}
			newStatus, err := dao.TranslateToRunStatus(statusStr)
			if err != nil {
				return &jsonrpc.Error{
					Code:    jsonrpc.ErrorCodeInvalidParams,
					Message: err.Error(),
				}
			}
			err = bl.UpdateRunStatus(p.Id, newStatus)
			if err != nil {
				return &jsonrpc.Error{
					Code:    jsonrpc.ErrorCodeInternal,
					Message: err.Error(),
				}
			}
		}

		result = &api.UpdateRunsResult{}
		return nil
	})
	return result, jsonRPCErr
}

func (h CreateRunHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	if err := valve.Lever(c).Open(); err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var p api.CreateRunParams
	var result interface{}
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		key := p.Key
		if p.Key == "" {
			random, err := uuid.NewRandom()
			if err != nil {
				return &jsonrpc.Error{
					Code:    jsonrpc.ErrorCodeInternal,
					Message: err.Error(),
				}
			}
			key = random.String()
		}
		var md mapstructure.Metadata
		var template bl.Template
		decoder, err := mapstructure.NewDecoder(
			&mapstructure.DecoderConfig{
				Metadata: &md,
				Result:   &template,
			})
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		err = decoder.Decode(p.Template)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInvalidParams,
				Message: err.Error(),
			}
		}
		if len(md.Unused) > 0 {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInvalidParams,
				Message: fmt.Sprintf("unsupported attributes provided in do options: %s", strings.Join(md.Unused, ",")),
			}
		}
		run, err := template.CreateRun(key)
		if err != nil {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: err.Error(),
			}
		}
		if p.Key == "" {
			key = run.Key
		} else {
			key = ""
		}
		result = &api.CreateRunsResult{
			Id:     run.Id,
			Status: run.Status.MustTranslateRunStatus(),
			Key:    key,
		}
		return nil
	})
	return result, jsonRPCErr
}
