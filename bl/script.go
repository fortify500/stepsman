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
	"github.com/jedib0t/go-pretty/table"
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

type DO interface {
	Describe() string
}
type StepDoShellExecute struct {
	Options StepDoShellExecuteOptions
}
type StepDoShellExecuteOptions struct {
	Command   string
	Arguments []string
}

func (do StepDoShellExecute) Describe() string {
	strs := []string{do.Options.Command}
	strs = append(strs, do.Options.Arguments...)
	return fmt.Sprintf("%s", strings.Join(strs, " "))
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
	for i := range s.Steps {
		err = (&s.Steps[i]).AdjustUnmarshalStep(false)
		if err != nil {
			return err
		}
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

func (s *Step) AdjustUnmarshalStep(fillStep bool) error {
	if fillStep {
		script := s.Script
		err := yaml.Unmarshal([]byte(script), s)
		if err != nil {
			return err
		}
		s.Script = script
	} else {
		stepBytes, err := yaml.Marshal(s)
		if err != nil {
			return err
		}
		s.Script = string(stepBytes)
	}
	if s.Do != nil {
		stepDo := StepDo{}
		stepDoBytes, err := yaml.Marshal(s.Do)
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
			s.Do = do
			s.DoType = DoTypeShellExecute
		}
	}
	return nil
}

var NoBordersStyle = table.Style{
	Name:    "StyleDefault",
	Box:     table.StyleBoxDefault,
	Color:   table.ColorOptionsDefault,
	Format:  table.FormatOptionsDefault,
	HTML:    table.DefaultHTMLOptions,
	Options: table.OptionsNoBordersAndSeparators,
	Title:   table.TitleOptionsDefault,
}
