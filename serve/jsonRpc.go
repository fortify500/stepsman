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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/go-playground/validator/v10"
	"github.com/osamingo/jsonrpc"
	log "github.com/sirupsen/logrus"
	"runtime/debug"
)

var validate = validator.New()

func recoverable(recoverableFunction func() *jsonrpc.Error) (err *jsonrpc.Error) {
	defer func() {
		if p := recover(); p != nil {
			var msg string
			if _, ok := p.(error); ok {
				defer log.WithField("stack", string(debug.Stack())).Error(fmt.Errorf("failed to serve: %w", p.(error)))
				msg = p.(error).Error()
			} else {
				defer log.WithField("stack", string(debug.Stack())).Error(fmt.Errorf("failed to serve: %v", p))
				msg = fmt.Sprintf("%v", p)
			}
			err = &jsonrpc.Error{
				Code:    jsonrpc.ErrorCodeInternal,
				Message: msg,
			}
		} else if err != nil {
			defer log.Debug(fmt.Errorf("failed to serve: %w", err))
		}
	}()
	err = recoverableFunction()
	return err
}

func resolveError(err error) *jsonrpc.Error {
	var apiErr *api.Error
	apiErr = api.ResolveErrorAndLog(err, true)
	if apiErr != nil {
		return &jsonrpc.Error{
			Code:    jsonrpc.ErrorCode(apiErr.Code().Code),
			Message: apiErr.Error(),
			Data:    apiErr.Data(),
		}
	} else {
		panic(err) // there shouldn't be any error of another type.
	}
}

func JSONRPCUnmarshal(params []byte, dst interface{}) *jsonrpc.Error {
	if params == nil {
		return resolveError(api.NewError(api.ErrInvalidParams, "failed to unmarshal json-rpc"))
	}
	decoder := json.NewDecoder(bytes.NewReader(params))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return resolveError(api.NewWrapError(api.ErrInvalidParams, err, "failed to unmarshal json-rpc: %w", err))
	}
	if api.TestMode {
		err := validate.Struct(dst)
		if err != nil {
			return resolveError(api.NewWrapError(api.ErrInvalidParams, err, "failed to unmarshal json-rpc: %w", err))
		}
	}
	return nil
}

func GetJsonRpcHandler() *jsonrpc.MethodRepository {

	mr := jsonrpc.NewMethodRepository()

	if err := mr.RegisterMethod(api.RPCListRuns, ListRunsHandler{}, api.ListParams{}, api.ListRunsResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}
	if err := mr.RegisterMethod(api.RPCGetRuns, GetRunsHandler{}, api.GetRunsParams{}, api.GetRunsResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}

	if err := mr.RegisterMethod(api.RPCUpdateRun, UpdateRunHandler{}, api.UpdateRunParams{}, api.UpdateRunResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}

	if err := mr.RegisterMethod(api.RPCCreateRun, CreateRunHandler{}, api.CreateRunParams{}, api.CreateRunsResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}
	if err := mr.RegisterMethod(api.RPCDeleteRun, DeleteRunsHandler{}, api.DeleteRunsParams{}, api.DeleteRunsResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}

	if err := mr.RegisterMethod(api.RPCListSteps, ListStepsHandler{}, api.ListParams{}, api.ListStepsResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}

	if err := mr.RegisterMethod(api.RPCGetSteps, GetStepsHandler{}, api.GetStepsParams{}, api.GetStepsResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}

	if err := mr.RegisterMethod(api.RPCUpdateStepByUUID, UpdateStepByUUIDHandler{}, api.UpdateStepByUUIDParams{}, api.UpdateStepByUUIDResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}

	if err := mr.RegisterMethod(api.RPCUpdateStepByLabel, UpdateStepByLabelHandler{}, api.UpdateStepByLabelParams{}, api.UpdateStepByLabelResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}

	if err := mr.RegisterMethod(api.RPCDoStepByUUID, DoStepByUUIDHandler{}, api.DoStepByUUIDParams{}, api.DoStepByUUIDResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}
	if err := mr.RegisterMethod(api.RPCDoStepByLabel, DoStepByLabelHandler{}, api.DoStepByLabelParams{}, api.DoStepByLabelResult{}); err != nil {
		log.Fatal(api.NewLocalizedError("failed to start serving json rpc: %w", err))
	}
	return mr
}
