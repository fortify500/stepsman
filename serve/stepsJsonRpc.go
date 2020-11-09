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
)

type (
	ListStepsHandler  struct{}
	GetStepsHandler   struct{}
	UpdateStepHandler struct{}
	DoStepHandler     struct{}
)

func (h ListStepsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
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
		steps, stepsRange, err := bl.ListSteps(&query)
		if err != nil {
			return resolveError(err)
		}

		result = api.ListStepsResult{
			Range: *stepsRange,
			Data:  steps,
		}
		return nil
	})
	return result, jsonRPCErr
}

func (h GetStepsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	if err := valve.Lever(c).Open(); err != nil {
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
		if err := dao.VetIds(p.UUIDs); err != nil {
			return resolveError(err)
		}
		query := api.GetStepsQuery(p)
		steps, err := bl.GetSteps(&query)
		if err != nil {
			return resolveError(err)
		}
		result = steps
		return nil
	})

	return result, jsonRPCErr
}

func (h UpdateStepHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	if err := valve.Lever(c).Open(); err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var result interface{}
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.UpdateStepParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		query := api.UpdateQuery(p)
		err := bl.UpdateStep(&query)
		if err != nil {
			return resolveError(err)
		}
		result = &api.UpdateStepResult{}
		return nil
	})
	return result, jsonRPCErr
}

func (h DoStepHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	if err := valve.Lever(c).Open(); err != nil {
		return nil, &jsonrpc.Error{
			Code:    jsonrpc.ErrorCodeInternal,
			Message: err.Error(),
		}
	}
	defer valve.Lever(c).Close()
	var result interface{}
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.DoStepParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		if err := dao.VetIds([]string{p.UUID}); err != nil {
			return resolveError(err)
		}
		doResult, err := bl.DoStep(&p, false)
		if err != nil {
			return resolveError(err)
		}
		result = doResult
		return nil
	})
	return result, jsonRPCErr
}
