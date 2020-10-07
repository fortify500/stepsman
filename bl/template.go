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
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

type DoType string

const (
	DoTypeShellExecute DoType = "shell execute"
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
	Do          interface{} `json:"do"`
	doType      DoType
	stepRecord  *dao.StepRecord
}

type StepDo struct {
	Type DoType `json:"type"`
}

type DO interface {
	Describe() string
}
type StepDoShellExecute struct {
	StepDo  `yaml:",inline"`
	Options StepDoShellExecuteOptions `json:"options"`
}
type StepDoShellExecuteOptions struct {
	Command   string   `json:"command"`
	Arguments []string `json:"arguments"`
}

func (do StepDoShellExecute) Describe() string {
	strs := []string{do.Options.Command}
	strs = append(strs, do.Options.Arguments...)
	return fmt.Sprintf("%s", strings.Join(strs, " "))
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

	if s.Do != nil {
		stepDo := StepDo{}
		stepDoBytes, err := yaml.Marshal(s.Do)
		if err != nil {
			return err
		}
		decoder := yaml.NewDecoder(bytes.NewBuffer(stepDoBytes))
		decoder.SetStrict(false)
		err = decoder.Decode(&stepDo)
		if err != nil {
			return err
		}
		doType := string(stepDo.Type)
		switch DoType(doType) {
		case "":
			fallthrough
		case DoTypeShellExecute:
			do := StepDoShellExecute{}
			decoder := yaml.NewDecoder(bytes.NewBuffer(stepDoBytes))
			decoder.SetStrict(true)
			err = decoder.Decode(&do)
			if err != nil {
				return err
			}
			s.Do = do
			s.doType = DoTypeShellExecute
		}
	}
	return nil
}
