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
package dao

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
)

type (
	ErrorCode int64

	JSONRPCError struct {
		Code    ErrorCode   `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data,omitempty"`
	}
)
type ListRunsResponse struct {
	Version string         `json:"jsonrpc"`
	Result  []RunRPCRecord `json:"result,omitempty"`
	Error   JSONRPCError   `json:"error,omitempty"`
	ID      string         `json:"id,omitempty"`
}

func RemoteListRuns() ([]*RunRecord, error) {
	result := make([]*RunRecord, 0)
	request, err := NewMarshaledJSONRPCRequest("1", LIST_RUNS, &ListRunsParams{})
	if err != nil {
		return nil, err
	}
	newRequest, err := http.NewRequest("POST", "http://localhost:3333/v0/json-rpc", bytes.NewBuffer(request))
	if err != nil {
		return nil, err
	}
	newRequest.Header.Set("Content-type", "application/json")
	response, err := Client.Do(newRequest)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	defer io.Copy(ioutil.Discard, response.Body)
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to reach remote server, got: %d", response.StatusCode)
	}
	var jsonRPCResult ListRunsResponse
	decoder := json.NewDecoder(response.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&jsonRPCResult); err != nil {
		return nil, err
	}
	if jsonRPCResult.Error.Code != 0 {
		return nil, fmt.Errorf("failed to perform operation, remote server responded with code: %d, and message: %s", jsonRPCResult.Error.Code, jsonRPCResult.Error.Message)
	}
	if jsonRPCResult.Result != nil {
		for _, record := range jsonRPCResult.Result {
			status, err := TranslateToRunStatus(record.Status)
			if err != nil {
				return nil, err
			}
			result = append(result, &RunRecord{
				Id:     record.Id,
				UUID:   record.UUID,
				Title:  record.Title,
				Cursor: record.Cursor,
				Status: status,
				Script: record.Script,
			})
		}
	}
	return result, nil
}
