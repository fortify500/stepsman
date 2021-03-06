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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

type CLI struct {
	Client     *http.Client
	JsonRpcUrl string
}

type JSONRPCRequest struct {
	Version string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
	ID      string      `json:"id,omitempty"`
}

func NewMarshaledJSONRPCRequest(id string, method string, params interface{}) []byte {
	request := JSONRPCRequest{
		Version: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}
	marshal, err := json.Marshal(request)
	if err != nil {
		panic(err)
	}
	return marshal
}

func New(ssl bool, host string, port int64, tlsConfig *tls.Config) *CLI {
	protocol := "http"
	if ssl {
		protocol += "s"
	}
	//goland:noinspection GoDeprecation,SpellCheckingInspection
	var transport = http.Transport{
		MaxResponseHeaderBytes: 128 * 1024,
		IdleConnTimeout:        10 * time.Minute,
	}
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}
	var c = &http.Client{
		Timeout:   time.Second * 60,
		Transport: &transport,
	}
	cli := &CLI{
		Client: c,
		JsonRpcUrl: fmt.Sprintf("%s://%s:%d/v0/json-rpc",
			protocol,
			host,
			port),
	}
	return cli
}

//goland:noinspection GoUnhandledErrorResult
func (c *CLI) remoteJRPCCall(request []byte, decodeResponse func(body *io.ReadCloser) error) error {
	newRequest, err := http.NewRequest("POST", c.JsonRpcUrl, bytes.NewBuffer(request))
	if err != nil {
		return fmt.Errorf("failed to form rest request: %w", err)
	}
	newRequest.Header.Set("Content-type", "application/json")
	response, err := c.Client.Do(newRequest)
	if err != nil {
		return fmt.Errorf("failed to reach remote server: %w", err)
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
	ErrorCode int

	JSONRPCError struct {
		Code    ErrorCode   `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data,omitempty"`
	}
)

func getJSONRPCError(jsonRpcError *JSONRPCError) error {
	if jsonRpcError.Code != 0 {
		code, ok := api.ErrorCodes[int64(jsonRpcError.Code)]
		if ok {
			return api.NewErrorWithData(code, jsonRpcError.Data, jsonRpcError.Message)
		}
		return fmt.Errorf("failed to perform operation, remote server responded with code: %d, and message: %s", jsonRpcError.Code, jsonRpcError.Message)
	}
	return nil
}

func InitLogrus(out io.Writer, level log.Level) {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetLevel(level)
	log.SetOutput(out)
}
