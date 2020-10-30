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
	"github.com/fortify500/stepsman/api"
	"github.com/osamingo/jsonrpc"
	log "github.com/sirupsen/logrus"
)

func JSONRPCUnmarshal(params []byte, dst interface{}) *jsonrpc.Error {
	if params == nil {
		return jsonrpc.ErrInvalidParams()
	}
	decoder := json.NewDecoder(bytes.NewReader(params))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return jsonrpc.ErrInvalidParams()
	}
	return nil
}

func GetJsonRpcHandler() *jsonrpc.MethodRepository {

	mr := jsonrpc.NewMethodRepository()

	if err := mr.RegisterMethod(api.RPCListRuns, ListRunsHandler{}, api.ListParams{}, api.ListRunsResult{}); err != nil {
		log.Fatal(err)
	}
	if err := mr.RegisterMethod(api.RPCGetRuns, GetRunsHandler{}, api.GetRunsParams{}, api.GetRunsResult{}); err != nil {
		log.Fatal(err)
	}

	if err := mr.RegisterMethod(api.RPCUpdateRun, UpdateRunHandler{}, api.UpdateRunParams{}, api.UpdateRunsResult{}); err != nil {
		log.Fatal(err)
	}

	if err := mr.RegisterMethod(api.RPCCreateRun, CreateRunHandler{}, api.CreateRunParams{}, api.CreateRunsResult{}); err != nil {
		log.Fatal(err)
	}

	if err := mr.RegisterMethod(api.RPCListSteps, ListStepsHandler{}, api.ListParams{}, api.ListStepsResult{}); err != nil {
		log.Fatal(err)
	}

	if err := mr.RegisterMethod(api.RPCGetSteps, GetStepsHandler{}, api.GetStepsParams{}, api.GetStepsResult{}); err != nil {
		log.Fatal(err)
	}
	return mr
}
