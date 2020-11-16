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
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/dao"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"strings"
)

type DoType string

const (
	DoTypeREST DoType = "REST"
)

type Template struct {
	Title           string `json:"title"`
	Version         int64  `json:"version"`
	Steps           []Step `json:"steps"`
	labelsToIndices map[string]int64
}

type ThenDo struct {
	Label   string `json:"label,omitempty"`
	Context string `json:"context,omitempty"`
}
type Then struct {
	Do []ThenDo `json:"do,omitempty"`
}
type Rule struct {
	Then *Then `json:"then,omitempty"`
}
type Event struct {
	Rules []Rule `json:"rules,omitempty"`
}
type On struct {
	PreDone *Event `json:"pre-done,omitempty" mapstructure:"pre-done" yaml:"pre-done,omitempty"`
}
type Step struct {
	Name        string      `json:"name,omitempty"`
	Label       string      `json:"label,omitempty"`
	Description string      `json:"description,omitempty"`
	Do          interface{} `json:"do,omitempty"`
	On          On          `json:"on,omitempty"`
	Retries     int         `json:"retries,omitempty"`
	stepDo      StepDo
	doType      DoType
	template    *Template
}

type StepDo struct {
	Type             DoType `json:"type,omitempty" mapstructure:"type" yaml:"type,omitempty"`
	HeartBeatTimeout int64  `json:"heartbeat-timeout,omitempty" mapstructure:"heartbeat-timeout" yaml:"heartbeat-timeout,omitempty"`
	Retries          int64  `json:"retries,omitempty" mapstructure:"retries" yaml:"retries,omitempty"`
	CompleteBy       int64  `json:"complete-by,omitempty" mapstructure:"complete-by" yaml:"complete-by,omitempty"`
}

type DO interface {
	Describe() string
}
type StepDoREST struct {
	StepDo  `yaml:",inline" mapstructure:",squash"`
	Options StepDoRESTOptions `json:"options,omitempty"`
}
type StepDoRESTOptions struct {
	Timeout              int64       `json:"timeout,omitempty"`
	Method               string      `json:"method,omitempty"`
	Url                  string      `json:"url,omitempty"`
	Headers              http.Header `json:"headers,omitempty"`
	MaxResponseBodyBytes int64       `json:"max-response-body-bytes,omitempty" mapstructure:"max-response-body-bytes" yaml:"max-response-body-bytes,omitempty"`
	Body                 string      `json:"body,omitempty"`
}

func (do StepDoREST) Describe() string {
	doStr, err := yaml.Marshal(do)
	if err != nil {
		panic(err)
	}
	return string(doStr)
}

func (t *Template) LoadFromBytes(isYaml bool, yamlDocument []byte) error {
	var err error
	if isYaml {
		decoder := yaml.NewDecoder(bytes.NewBuffer(yamlDocument))
		decoder.SetStrict(true)
		err = decoder.Decode(t)
	} else {
		decoder := json.NewDecoder(bytes.NewBuffer(yamlDocument))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(t)
	}
	if err != nil {
		return api.NewWrapError(api.ErrInvalidParams, err, "failed to load from bytes: %w", err)
	}
	t.labelsToIndices = make(map[string]int64)
	for i := range t.Steps {
		err = (&t.Steps[i]).AdjustUnmarshalStep(t, int64(i)+1)
		if err != nil {
			return fmt.Errorf("failed to load from bytes: %w", err)
		}
	}
	return nil
}

func (t *Template) Start(key string, fileName string) (string, error) {
	yamlDocument, err := ioutil.ReadFile(fileName)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", fileName, err)
	}
	err = t.LoadFromBytes(true, yamlDocument)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal file %s: %w", fileName, err)
	}
	if dao.IsRemote {
		var runId string
		runId, _, _, err = client.RemoteCreateRun(&api.CreateRunParams{
			Key:      key,
			Template: t,
		})
		if err != nil {
			return "", fmt.Errorf("failed to create run remotely for file %s: %w", fileName, err)
		}
		return runId, err
	} else {
		var runRow *api.RunRecord
		runRow, err = t.CreateRun(key)
		if err != nil {
			return "", fmt.Errorf("failed to start: %w", err)
		}
		return runRow.Id, err
	}

}

func (s *Step) AdjustUnmarshalStep(t *Template, index int64) error {
	if s.Label == "" {
		random, err := uuid.NewRandom()
		if err != nil {
			panic(err)
		}
		s.Label = random.String()
	}
	s.template = t
	s.template.labelsToIndices[s.Label] = index
	if s.Do == nil {
		return nil
	} else {
		var doType DoType
		switch s.Do.(type) {
		case map[interface{}]interface{}:
			doMap := s.Do.(map[interface{}]interface{})
			switch doMap["type"].(type) {
			case string:
			default:
				return api.NewError(api.ErrInvalidParams, "failed to adjust step do options, invalid do type - type value")
			}
			doType = DoType(doMap["type"].(string))
		case map[string]interface{}:
			doMap := s.Do.(map[string]interface{})
			switch doMap["type"].(type) {
			case string:
			default:
				return api.NewError(api.ErrInvalidParams, "failed to adjust step do options, invalid do type - string type value")
			}
			doType = DoType(doMap["type"].(string))
		case map[string]string:
			doMap := s.Do.(map[string]string)
			doType = DoType(doMap["type"])
		default:
			return api.NewError(api.ErrInvalidParams, "failed to adjust step do options, invalid do type")
		}

		switch doType {
		case DoTypeREST:
			doRest := StepDoREST{}
			var md mapstructure.Metadata
			decoder, err := mapstructure.NewDecoder(
				&mapstructure.DecoderConfig{
					Metadata: &md,
					Result:   &doRest,
				})
			if err != nil {
				return api.NewWrapError(api.ErrInvalidParams, err, "failed to adjust step do rest: %w", err)
			}
			err = decoder.Decode(s.Do)
			if err != nil {
				return api.NewWrapError(api.ErrInvalidParams, err, "failed to adjust step do rest: %w", err)
			}
			if len(md.Unused) > 0 {
				return api.NewError(api.ErrInvalidParams, "unsupported attributes provided in do options: %s", strings.Join(md.Unused, ","))
			}
			s.stepDo = doRest.StepDo
			s.Do = doRest
		default:
			return api.NewError(api.ErrInvalidParams, "unsupported do type: %s", doType)
		}
		s.doType = doType
	}
	return nil
}
