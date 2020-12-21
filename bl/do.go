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
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"strings"
	"time"
)

const (
	DefaultDoRestTimeout          = 60
	DefaultMaxResponseHeaderBytes = 64 * 1024
	DefaultMaxResponseBodyBytes   = 256 * 1024
)

type DoubleCurlyToken struct {
	Start int
	End   int
}

type StepStateRest struct {
	Body         interface{}         `json:"body,omitempty" mapstructure:"body" yaml:"body,omitempty"`
	ContentType  string              `json:"content-type,omitempty" mapstructure:"content-type" yaml:"content-type,omitempty"`
	StatusCode   int                 `json:"status-code,omitempty" mapstructure:"status-code" yaml:"status-code,omitempty"`
	StatusPhrase string              `json:"status-phrase,omitempty" mapstructure:"status-phrase" yaml:"status-phrase,omitempty"`
	Header       map[string][]string `json:"header,omitempty" mapstructure:"header" yaml:"header,omitempty"`
}

var emptyMap = make(map[string]string)

func (b *BL) initDO(maxResponseHeaderByte int64) {
	b.netTransport = &http.Transport{
		MaxResponseHeaderBytes: DefaultMaxResponseHeaderBytes,
		IdleConnTimeout:        10 * time.Minute, //avoid overwhelming the infrastructure
	}
	if maxResponseHeaderByte > 0 {
		b.netTransport.MaxResponseHeaderBytes = maxResponseHeaderByte
	}
}
func (b *BL) do(t *Template, step *Step, stepRecord *api.StepRecord, doType DoType, doInterface interface{}, prevState *api.State, currentContext api.Context) (api.State, bool, error) {
	var newState api.State
	var err error
	var async bool
	newState = *prevState
	newState.Error = ""
	newState.Result = emptyMap
	if doInterface != nil {
		switch doType {
		case DoTypeREST:
			newState, async, err = b.doRest(t, step, stepRecord, doInterface, currentContext, newState)
			if err != nil {
				return newState, false, err
			}
		case DoTypeEVALUATE:
			doEvaluate := doInterface.(StepDoEvaluate)
			var resolvedResult string
			resolvedResult, err = t.EvaluateCurlyPercent(b, step, doEvaluate.Options.Result, currentContext, nil, nil)
			if err != nil {
				err = fmt.Errorf("failed to evaluate: %s: %w", doEvaluate.Options.Result, err)
				newState.Error = err.Error()
				return newState, false, err
			}
			err = json.Unmarshal([]byte(resolvedResult), &(newState.Result))
			if err != nil {
				err = fmt.Errorf("failed to evaluate: %s: %w", doEvaluate.Options.Result, err)
				newState.Result = emptyMap
				newState.Error = err.Error()
				return newState, false, err
			}
		default:
			err = api.NewError(api.ErrInvalidParams, "unsupported do type: %s", doType)
			newState.Error = err.Error()
			return newState, false, err
		}
	}

	return newState, async, err
}

func (b *BL) doRest(t *Template, step *Step, stepRecord *api.StepRecord, doInterface interface{}, currentContext api.Context, newState api.State) (api.State, bool, error) {
	doRest := doInterface.(StepDoREST)
	var response *http.Response

	var timeout = DefaultDoRestTimeout * time.Second
	var maxResponseBodyBytes int64 = DefaultMaxResponseBodyBytes

	if doRest.Options.Timeout > 0 {
		timeout = time.Duration(doRest.Options.Timeout) * time.Second
	}

	if doRest.Options.MaxResponseBodyBytes > 0 {
		maxResponseBodyBytes = doRest.Options.MaxResponseBodyBytes
	}
	{
		var err error
		var body io.ReadCloser = nil
		if len(doRest.Options.Body) > 0 {
			body = ioutil.NopCloser(strings.NewReader(doRest.Options.Body))
		}
		var netClient = &http.Client{
			Transport: b.netTransport,
			Timeout:   timeout,
		}
		var resolvedUrl string
		resolvedUrl, err = t.EvaluateCurlyPercent(b, step, doRest.Options.Url, currentContext, nil, nil)
		if err != nil {
			err = fmt.Errorf("failed to evaluate url: %s: %w", doRest.Options.Url, err)
			newState.Error = err.Error()
			return newState, false, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		var request *http.Request
		request, err = http.NewRequestWithContext(ctx, doRest.Options.Method, resolvedUrl, body)
		if err != nil {
			err = api.NewWrapError(api.ErrInvalidParams, err, "failed to form a request due to: %w", err)
			newState.Error = err.Error()
			return newState, false, err
		}
		for k, v := range doRest.Options.Headers {
			var header []string
			for _, h := range v {
				var resolvedHeaderPart string
				resolvedHeaderPart, err = t.EvaluateCurlyPercent(b, step, h, currentContext, nil, nil)
				if err != nil {
					err = fmt.Errorf("failed to evaluate header part: %s: %w", h, err)
					newState.Error = err.Error()
					return newState, false, err
				}
				header = append(header, resolvedHeaderPart)
			}
			request.Header[k] = header
		}
		request.Header["stepsman-run-id"] = []string{stepRecord.RunId}
		request.Header["stepsman-label"] = []string{step.Label}
		request.Header["stepsman-status-owner"] = []string{stepRecord.StatusOwner}
		response, err = netClient.Do(request)
		if err != nil {
			err = api.NewWrapError(api.ErrExternal, err, "failed to connect to a rest api due to: %w", err)
			newState.Error = err.Error()
			return newState, false, err
		}
	}
	result := StepStateRest{}
	defer response.Body.Close()
	bodyBytes, err := ioutil.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes))
	if err != nil {
		err = api.NewWrapError(api.ErrExternal, err, "failed to read from a rest api due to: %w", err)
		newState.Error = err.Error()
		return newState, false, err
	}
	result.ContentType = "text/plain"
	result.ContentType, _, _ = mime.ParseMediaType(response.Header.Get("Content-Type"))
	switch result.ContentType {
	case "application/json":
		err = json.Unmarshal(bodyBytes, &result.Body)
		if err != nil {
			err = api.NewWrapError(api.ErrExternal, err, "failed to decode a rest api body into a json due to: %w", err)
			newState.Error = err.Error()
			return newState, false, err
		}
	default:
		result.Body = string(bodyBytes)
	}
	result.StatusCode = response.StatusCode
	result.StatusPhrase = response.Status
	result.Header = response.Header
	newState.Result = result
	async := false
	if result.Header != nil {
		for k, v := range result.Header {
			if strings.EqualFold(k, "stepsman-async") {
				if v != nil && len(v) > 0 {
					if strings.EqualFold(v[0], "true") {
						async = true
					}
				}
				break
			}
		}
	}
	log.Debug(fmt.Sprintf("response status:%d, body:%s", response.StatusCode, result.Body))
	return newState, async, nil
}

func TokenizeCurlyPercent(originalString string) (result []DoubleCurlyToken, sanitizedString string) {
	s := strings.ReplaceAll(originalString, "\\{\\%", "xx")
	s = strings.ReplaceAll(s, "\\%\\}", "xx")
	tagPart := false
	start := -1
	end := -1
	for i := 0; i < len(s); i++ {
		if start == -1 {
			if tagPart && s[i] == '%' {
				tagPart = false
				start = i + 1
				if start >= len(s) {
					break
				}
			} else if s[i] == '{' {
				tagPart = true
				continue
			}
			tagPart = false
			continue
		} else {
			if tagPart && s[i] == '}' {
				tagPart = false
				end = i - 1
				result = append(result, DoubleCurlyToken{
					Start: start,
					End:   end,
				})
				start = -1
				end = -1
				continue
			} else if s[i] == '%' {
				tagPart = true
				continue
			}
			tagPart = false
		}
	}
	sanitizedString = strings.ReplaceAll(originalString, "\\{\\%", "{%")
	sanitizedString = strings.ReplaceAll(sanitizedString, "\\%\\}", "%}")
	return
}
