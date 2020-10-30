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

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"
)

//goland:noinspection GoDeprecation,SpellCheckingInspection
var Transport = http.Transport{
	Proxy: nil,
	DialContext: (&net.Dialer{
		Timeout:   60 * time.Second,
		KeepAlive: 60 * time.Second,
		DualStack: true,
	}).DialContext,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          100,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   30 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
	//MaxIdleConnsPerHost:    0,
	//MaxConnsPerHost:        0,
	ResponseHeaderTimeout:  60 * time.Second,
	MaxResponseHeaderBytes: 10 * 1024 * 1024,
}
var Client = &http.Client{
	//Jar:           nil,
	Timeout:   time.Second * 60,
	Transport: &Transport,
}

type JSONRPCRequest struct {
	Version string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      string      `json:"id,omitempty"`
}

func NewMarshaledJSONRPCRequest(id string, method string, params interface{}) ([]byte, error) {
	request := JSONRPCRequest{
		Version: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}
	return json.Marshal(request)
}

var jsonRpcUrl string

func InitClient(ssl bool, host string, port int64) {
	protocol := "http"
	if ssl {
		protocol += "s"
	}
	jsonRpcUrl = fmt.Sprintf("%s://%s:%d/v0/json-rpc",
		protocol,
		host,
		port)
}

//goland:noinspection GoUnhandledErrorResult
func remoteJRPCCall(request []byte, decodeResponse func(body *io.ReadCloser) error) error {
	newRequest, err := http.NewRequest("POST", jsonRpcUrl, bytes.NewBuffer(request))
	if err != nil {
		return err
	}
	newRequest.Header.Set("Content-type", "application/json")
	response, err := Client.Do(newRequest)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	defer io.Copy(ioutil.Discard, response.Body)
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to reach remote server, got: %d", response.StatusCode)
	}
	err = decodeResponse(&response.Body)
	return err
}

type (
	ErrorCode int64

	JSONRPCError struct {
		Code    ErrorCode   `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data,omitempty"`
	}
)

func getJSONRPCError(jsonRpcError *JSONRPCError) error {
	if jsonRpcError.Code != 0 {
		return fmt.Errorf("failed to perform operation, remote server responded with code: %d, and message: %s", jsonRpcError.Code, jsonRpcError.Message)
	}
	return nil
}

func InitLogrus(out io.Writer) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(log.TraceLevel)
	log.SetOutput(out)
}
