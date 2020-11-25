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
func (b *BL) do(doType DoType, doInterface interface{}, prevState *dao.StepState) (dao.StepState, error) {
	var newState dao.StepState
	newState = *prevState
	newState.Error = ""
	newState.Result = emptyMap
	if doInterface != nil {
		switch doType {
		case DoTypeREST:
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
				var body io.ReadCloser = nil
				if len(doRest.Options.Body) > 0 {
					body = ioutil.NopCloser(strings.NewReader(doRest.Options.Body))
				}
				var netClient = &http.Client{
					Transport: b.netTransport,
					Timeout:   timeout,
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				request, err := http.NewRequestWithContext(ctx, doRest.Options.Method, doRest.Options.Url, body)
				if err != nil {
					newState.Error = err.Error()
					return newState, api.NewWrapError(api.ErrInvalidParams, err, "failed to form a request due to: %w", err)
				}
				for k, v := range doRest.Options.Headers {
					request.Header[k] = v
				}
				response, err = netClient.Do(request)
				if err != nil {
					newState.Error = err.Error()
					return newState, api.NewWrapError(api.ErrExternal, err, "failed to connect to a rest api due to: %w", err)
				}
			}
			result := StepStateRest{}
			defer response.Body.Close()
			bodyBytes, err := ioutil.ReadAll(io.LimitReader(response.Body, maxResponseBodyBytes))
			if err != nil {
				newState.Error = err.Error()
				return newState, api.NewWrapError(api.ErrExternal, err, "failed to read from a rest api due to: %w", err)
			}
			result.ContentType = "text/plain"
			result.ContentType, _, _ = mime.ParseMediaType(response.Header.Get("Content-Type"))
			switch result.ContentType {
			case "application/json":
				err = json.Unmarshal(bodyBytes, &result.Body)
				if err != nil {
					newState.Error = err.Error()
					return newState, api.NewWrapError(api.ErrExternal, err, "failed to decode a rest api body into a json due to: %w", err)
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
			return newState, err
		}
	}

	return newState, nil
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
