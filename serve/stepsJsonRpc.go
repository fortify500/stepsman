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
	"github.com/intel-go/fastjson"
	"github.com/osamingo/jsonrpc"
)

type (
	ListStepsHandler        struct{}
	GetStepsHandler         struct{}
	UpdateStepByUUIDHandler struct{}
	DoStepByUUIDHandler     struct{}
	DoStepByLabelHandler    struct{}
)

func (h ListStepsHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
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
		steps, stepsRange, err := BL.ListSteps(&query)
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

func (h GetStepsHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
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
		steps, err := BL.GetSteps(&query)
		if err != nil {
			return resolveError(err)
		}
		result = steps
		return nil
	})

	return result, jsonRPCErr
}

func (h UpdateStepByUUIDHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.UpdateStepByUUIDParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		query := api.UpdateQueryById(p)
		err := BL.UpdateStepByUUID(&query)
		if err != nil {
			return resolveError(err)
		}
		result = &api.UpdateStepByUUIDResult{}
		return nil
	})
	return result, jsonRPCErr
}

func (h DoStepByUUIDHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.DoStepByUUIDParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		if err := dao.VetIds([]string{p.UUID}); err != nil {
			return resolveError(err)
		}
		doResult, err := BL.DoStepByUUID(&p, false)
		if err != nil {
			return resolveError(err)
		}
		result = doResult
		return nil
	})
	return result, jsonRPCErr
}

func (h DoStepByLabelHandler) ServeJSONRPC(ctx context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	var result interface{}
	BL := ctx.Value("BL").(*bl.BL)
	jsonRPCErr := recoverable(func() *jsonrpc.Error {
		var p api.DoStepByLabelParams
		if params != nil {
			if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
				return errResult
			}
		}
		if err := dao.VetIds([]string{p.RunId}); err != nil {
			return resolveError(err)
		}
		if p.Label == "" {
			return &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInvalidParams,
				Message: "label string must not be empty",
			}
		}
		doResult, err := BL.DoStepByLabel(&p, false)
		if err != nil {
			return resolveError(err)
		}
		result = doResult
		return nil
	})
	return result, jsonRPCErr
}
