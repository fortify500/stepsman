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
package bl

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

type DoType string

const (
	DoTypeShellExecute DoType = "shell execute"
)

type Script struct {
	Title string
	Steps []Step
}

type Step struct {
	Name        string
	Description string
	Do          interface{}
	DoType      DoType
	Script      string
	stepRecord  *StepRecord
}

type StepDo struct {
	Type DoType
}

type StepDoShellExecute struct {
	Command   string
	Arguments []string
}

func (s *Script) LoadFromFile(filename string) ([]byte, error) {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	err = s.LoadFromBytes(err, yamlFile)
	if err != nil {
		return nil, err
	}
	return yamlFile, nil
}

func (s *Script) LoadFromBytes(err error, yamlFile []byte) error {
	err = yaml.Unmarshal(yamlFile, s)
	if err != nil {
		return err
	}
	for i, _ := range s.Steps {
		(&s.Steps[i]).AdjustUnmarshalStep(false)
	}
	return nil
}

func (s *Script) Start(fileName string) (*RunRecord, error) {
	yamlBytes, err := s.LoadFromFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", fileName, err)
	}

	runRow := RunRecord{}
	err = runRow.Create(err, s, yamlBytes)

	return &runRow, err
}

func Rollback(tx *sqlx.Tx, err error) error {
	err2 := tx.Rollback()
	if err2 != nil {
		err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	}
	return err
}

func (step *Step) AdjustUnmarshalStep(fillStep bool) error {
	if fillStep {
		script := step.Script
		err := yaml.Unmarshal([]byte(script), step)
		if err != nil {
			return err
		}
		step.Script = script
	} else {
		stepBytes, err := yaml.Marshal(step)
		if err != nil {
			return err
		}
		step.Script = string(stepBytes)
	}
	if step.Do != nil {
		stepDo := StepDo{}
		stepDoBytes, err := yaml.Marshal(step.Do)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(stepDoBytes, &stepDo)
		if err != nil {
			return err
		}
		doType := strings.ToLower(string(stepDo.Type))
		switch DoType(doType) {
		case "":
			fallthrough
		case DoTypeShellExecute:
			do := StepDoShellExecute{}
			err = yaml.Unmarshal(stepDoBytes, &do)
			if err != nil {
				return err
			}
			step.Do = do
			step.DoType = DoTypeShellExecute
		}
	}
	return nil
}
