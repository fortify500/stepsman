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

package bl

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/dao"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"
)

//var ErrNoRunsDirectory = fmt.Errorf("no runs directory detected and make directory flag is false")
const DefaultDoRestTimeout = 60

type StepStateRest struct {
	Body         interface{}         `json:"body,omitempty" mapstructure:"body" yaml:"body,omitempty"`
	ContentType  string              `json:"content-type,omitempty" mapstructure:"content-type" yaml:"content-type,omitempty"`
	StatusCode   int                 `json:"status-code,omitempty" mapstructure:"status-code" yaml:"status-code,omitempty"`
	StatusPhrase string              `json:"status-phrase,omitempty" mapstructure:"status-phrase" yaml:"status-phrase,omitempty"`
	Header       map[string][]string `json:"header,omitempty" mapstructure:"header" yaml:"header,omitempty"`
}

var emptyMap = make(map[string]string)

func do(doType DoType, doInterface interface{}, prevState *dao.StepState) (*dao.StepState, error) {
	var newState dao.StepState
	newState = *prevState
	newState.Error = ""
	newState.Result = emptyMap
	if doInterface != nil {
		switch doType {
		case DoTypeREST:
			doRest := doInterface.(StepDoREST)
			var response *http.Response
			{
				var timeout = DefaultDoRestTimeout * time.Second
				if doRest.Options.Timeout > 0 {
					timeout = time.Duration(doRest.Options.Timeout) * time.Second
				}
				var maxResponseHeaderBytes int64 = 256 * 1024
				if doRest.Options.MaxResponseHeaderBytes > 0 {
					maxResponseHeaderBytes = doRest.Options.MaxResponseHeaderBytes
				}
				var body io.ReadCloser = nil
				if len(doRest.Options.Body) > 0 {
					body = ioutil.NopCloser(strings.NewReader(doRest.Options.Body))
				}
				var netTransport = &http.Transport{
					DialContext: (&net.Dialer{
						Timeout: timeout,
					}).DialContext,
					TLSHandshakeTimeout:    timeout,
					ResponseHeaderTimeout:  timeout * time.Second,
					MaxResponseHeaderBytes: maxResponseHeaderBytes,
				}
				var netClient = &http.Client{
					Transport: netTransport,
					Timeout:   timeout,
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				request, err := http.NewRequestWithContext(ctx, doRest.Options.Method, doRest.Options.Url, body)
				if err != nil {
					newState.Error = err.Error()
					return &newState, api.NewWrapError(api.ErrInvalidParams, err, "failed to form a request due to: %w", err)
				}
				for k, v := range doRest.Options.Headers {
					request.Header[k] = v
				}
				response, err = netClient.Do(request)
				if err != nil {
					newState.Error = err.Error()
					return &newState, api.NewWrapError(api.ErrExternal, err, "failed to connect to a rest api due to: %w", err)
				}
			}
			result := StepStateRest{}
			defer response.Body.Close()
			bodyBytes, err := ioutil.ReadAll(response.Body)
			if err != nil {
				newState.Error = err.Error()
				return &newState, api.NewWrapError(api.ErrExternal, err, "failed to read from a rest api due to: %w", err)
			}
			result.ContentType = "text/plain"
			result.ContentType, _, _ = mime.ParseMediaType(response.Header.Get("Content-Type"))
			switch result.ContentType {
			case "application/json":
				err = json.Unmarshal(bodyBytes, &result.Body)
				if err != nil {
					newState.Error = err.Error()
					return &newState, api.NewWrapError(api.ErrExternal, err, "failed to decode a rest api body into a json due to: %w", err)
				}
			default:
				result.Body = string(bodyBytes)
			}
			result.StatusCode = response.StatusCode
			result.StatusPhrase = response.Status
			result.Header = response.Header
			newState.Result = result
			log.Debug(fmt.Sprintf("response status:%d, body:%s", response.StatusCode, result.Body))
		default:
			err := api.NewError(api.ErrInvalidParams, "unsupported do type: %s", doType)
			newState.Error = err.Error()
			return &newState, err
		}
	}

	return &newState, nil
}
