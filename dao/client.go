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
	"encoding/json"
	"net"
	"net/http"
	"time"
)

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
func initClient() {
	// we will populate this later for changing the timeout values.
}
