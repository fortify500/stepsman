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
	"github.com/google/uuid"
	"github.com/intel-go/fastjson"
	"github.com/osamingo/jsonrpc"
	"strings"
)

type (
	ListRunsHandler   struct{}
	GetRunsHandler    struct{}
	CreateRunHandler  struct{}
	UpdateRunHandler  struct{}
	DeleteRunsHandler struct{}
)

func (h ListRunsHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.ListParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		query := api.ListQuery(p)
		runs, runsRange, err := BL.ListRuns(&query)
		if err != nil {
			return resolveError(err)
		}
		result = api.ListRunsResult{
			Range: *runsRange,
			Data:  runs,
		}
		return nil
	})
	return result, jsonRPCErr
}

func (h GetRunsHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.GetRunsParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		query := api.GetRunsQuery(p)
		runs, err := BL.GetRuns(&query)
		if err != nil {
			return resolveError(err)
		}
		result = runs
		return nil
	})
	return result, jsonRPCErr
}

func (h UpdateRunHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.UpdateRunParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		if len(p.Changes) > 0 {
			if len(p.Changes) != 1 {
				return resolveError(api.NewError(api.ErrInvalidParams, "failed to update run, changes length!=1"))
			}
			val, ok := p.Changes["status"]
			if !ok {
				return resolveError(api.NewError(api.ErrInvalidParams, "failed to update run, changes do not contain status"))
			}
			var statusStr string
			statusStr, ok = val.(string)
			if !ok {
				return resolveError(api.NewError(api.ErrInvalidParams, "failed to update run, status is not of type string"))
			}
			newStatus, err := api.TranslateToRunStatus(statusStr)
			if err != nil {
				return &jsonrpc.Error{
					Code:    jsonrpc.ErrorCodeInvalidParams,
					Message: err.Error(),
				}
			}
			err = BL.UpdateRunStatus(p.Options, p.Id, newStatus)
			if err != nil {
				return resolveError(err)
			}
		}

		result = &api.UpdateRunResult{}
		return nil
	})
	return result, jsonRPCErr
}

func (h DeleteRunsHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.DeleteRunsParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		err := BL.DeleteRuns((*api.DeleteQuery)(&p))
		if err != nil {
			return resolveError(err)
		}

		result = &api.DeleteRunsResult{}
		return nil
	})
	return result, jsonRPCErr
}

func (h CreateRunHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var p api.CreateRunParams
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
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
		var template bl.Template
		//var isString bool
		isYaml := false
		if strings.EqualFold(p.TemplateType, "yaml") {
			isYaml = true
		} else if p.TemplateType != "" && !strings.EqualFold(p.TemplateType, "json") {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInvalidParams,
				Message: "template-type must be either empty (=json), json or yaml",
			}
		}

		err := template.LoadFromBytes(BL, uuid.UUID{}, isYaml, []byte(p.Template))
		if err != nil {
			return resolveError(err)
		}
		run, err := template.CreateRun(BL, p.Options, key)
		if err != nil {
			return resolveError(err)
		}
		if p.Key == "" {
			key = run.Key
		} else {
			key = ""
		}
		result = &api.CreateRunsResult{
			Id:     run.Id,
			Status: run.Status,
			Key:    key,
		}
		return nil
	})
	return result, jsonRPCErr
}
