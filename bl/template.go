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
	Title   string `json:"title"`
	Version int64  `json:"version"`
	Steps   []Step `json:"steps"`
}

type Step struct {
	Name        string      `json:"name"`
	Label       string      `json:"label"`
	Description string      `json:"description"`
	Do          interface{} `json:"do,omitempty"`
	stepDo      StepDo
	doType      DoType
}

type StepDo struct {
	Type             DoType `json:"type" mapstructure:"type"`
	HeartBeatTimeout int64  `json:"heartbeat-timeout" mapstructure:"heartbeat-timeout" yaml:"heartbeat-timeout"`
}

type DO interface {
	Describe() (string, error)
}
type StepDoREST struct {
	StepDo  `yaml:",inline" mapstructure:",squash"`
	Options StepDoRESTOptions `json:"options"`
}
type StepDoRESTOptions struct {
	Timeout                int64       `json:"timeout"`
	Method                 string      `json:"method"`
	Url                    string      `json:"url"`
	Headers                http.Header `json:"headers"`
	MaxResponseHeaderBytes int64       `json:"max-response-header-bytes" mapstructure:"max-response-header-bytes" yaml:"max-response-header-bytes"`
	Body                   string
}

func (do StepDoREST) Describe() (string, error) {
	doStr, err := yaml.Marshal(do)
	if err != nil {
		return "", err
	}
	return string(doStr), nil
}

func (s *Template) LoadFromFile(filename string) error {
	yamlDocument, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = s.LoadFromBytes(true, yamlDocument)
	if err != nil {
		return err
	}
	return nil
}

func (s *Template) LoadFromBytes(isYaml bool, yamlDocument []byte) error {
	var err error
	if isYaml {
		decoder := yaml.NewDecoder(bytes.NewBuffer(yamlDocument))
		decoder.SetStrict(true)
		err = decoder.Decode(s)
	} else {
		decoder := json.NewDecoder(bytes.NewBuffer(yamlDocument))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(s)
	}
	if err != nil {
		return err
	}
	for i := range s.Steps {
		err = (&s.Steps[i]).AdjustUnmarshalStep()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Template) Start(key string, fileName string) (*dao.RunRecord, error) {
	err := s.LoadFromFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", fileName, err)
	}

	runRow, err := s.CreateRun(key)

	return runRow, err
}

func (s *Step) AdjustUnmarshalStep() error {
	if s.Label == "" {
		random, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		s.Label = random.String()
	}

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
				return fmt.Errorf("failed to adjust step do options, invalid do type - type value")
			}
			doType = DoType(doMap["type"].(string))
		case map[string]interface{}:
			doMap := s.Do.(map[string]interface{})
			switch doMap["type"].(type) {
			case string:
			default:
				return fmt.Errorf("failed to adjust step do options, invalid do type - string type value")
			}
			doType = DoType(doMap["type"].(string))
		case map[string]string:
			doMap := s.Do.(map[string]string)
			doType = DoType(doMap["type"])
		default:
			return fmt.Errorf("failed to adjust step do options, invalid do type")
		}

		switch doType {
		case DoTypeREST:
			do := StepDoREST{}
			var md mapstructure.Metadata
			decoder, err := mapstructure.NewDecoder(
				&mapstructure.DecoderConfig{
					Metadata: &md,
					Result:   &do,
				})
			if err != nil {
				return err
			}
			err = decoder.Decode(s.Do)
			if err != nil {
				return err
			}
			if len(md.Unused) > 0 {
				return fmt.Errorf("unsupported attributes provided in do options: %s", strings.Join(md.Unused, ","))
			}
			s.stepDo = do.StepDo
			s.Do = do
		default:
			return fmt.Errorf("unsupported do type: %s", doType)
		}
		s.doType = doType
	}
	return nil
}
