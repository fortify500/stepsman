/*
Copyright © 2020 stepsman authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/fortify500/stepsman/bl"
	"github.com/go-chi/valve"
	log "github.com/sirupsen/logrus"

	"github.com/intel-go/fastjson"
	"github.com/osamingo/jsonrpc"
)

type (
	ListRunsHandler struct{}
)

// TODO: need to change this to a response with data and range result{} start, end (start+returned size) and total (hardest to calculate)
func (h ListRunsHandler) ServeJSONRPC(c context.Context, params *fastjson.RawMessage) (interface{}, *jsonrpc.Error) {
	valve.Lever(c).Open()
	defer valve.Lever(c).Close()
	var p bl.ListRunsParams
	if params != nil {
		if errResult := JSONRPCUnmarshal(*params, &p); errResult != nil {
			return nil, errResult
		}
	}
	runs, err := bl.ListRuns()
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

func RunRecordToRunRPCRecord(runs []*bl.RunRecord) ([]bl.RunRPCRecord, error) {
	var runRpcRecords []bl.RunRPCRecord
	for _, run := range runs {
		status, err := bl.TranslateRunStatus(run.Status)
		if err != nil {
			return nil, err
		}
		runRpcRecords = append(runRpcRecords, bl.RunRPCRecord{
			Id:     run.Id,
			UUID:   run.UUID,
			Title:  run.Title,
			Cursor: run.Cursor,
			Status: status,
			Script: run.Script,
		})
	}
	return runRpcRecords, nil
}

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

	if err := mr.RegisterMethod(bl.LIST_RUNS, ListRunsHandler{}, bl.ListRunsParams{}, bl.ListRunsResult{}); err != nil {
		log.Error(err)
	}
	return mr
}
