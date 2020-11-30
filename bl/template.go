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
	"context"
	"encoding/json"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/dao"
	"github.com/mitchellh/mapstructure"
	"github.com/open-policy-agent/opa/ast"
	"github.com/open-policy-agent/opa/rego"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

type DoType string

const (
	DoTypeREST DoType = "REST"
)

type Rego struct {
	compiler                  *ast.Compiler
	input                     map[string]interface{}
	lastStateHeartBeat        time.Time
	lastStateHeartBeatIndices []int64
	inputMutex                sync.RWMutex
}

type Template struct {
	Title           string `json:"title"`
	Version         int64  `json:"version"`
	Steps           []Step `json:"steps"`
	labelsToIndices map[string]int64
	indicesToLabels map[int64]string
	rego            *Rego
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

func (t *Template) LoadFromBytes(BL *BL, runId string, isYaml bool, yamlDocument []byte) error {
	var err error
	if runId != "" {
		entry, ok := BL.templateCache.Get(runId)
		if ok {
			*t = *entry.(*Template)
			return nil
		}
	}
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
	t.indicesToLabels = make(map[int64]string)
	t.rego = &Rego{}
	for i := range t.Steps {
		err = (&t.Steps[i]).AdjustUnmarshalStep(t, int64(i)+1)
		if err != nil {
			return fmt.Errorf("failed to load from bytes: %w", err)
		}
	}
	return nil
}

func (t *Template) RefreshInput(BL *BL, runId string) {
	var err error
	t.rego.inputMutex.RLock()
	if t.rego.input == nil {
		t.rego.inputMutex.RUnlock()
		if wasInit := t.initInput(); wasInit {
			BL.templateCache.Add(runId, t)
		}
		t.rego.inputMutex.RLock()
	}
	lastStateHeartBeat := t.rego.lastStateHeartBeat
	lastStateHeartBeatIndices := t.rego.lastStateHeartBeatIndices
	t.rego.inputMutex.RUnlock()
	query := &api.ListQuery{
		Filters: []api.Expression{{
			AttributeName: dao.RunId,
			Operator:      "=",
			Value:         runId,
		}, {
			AttributeName: dao.Status,
			Operator:      "=",
			Value:         api.StepDone,
		}, {
			AttributeName: dao.HeartBeat,
			Operator:      ">=",
			Value:         lastStateHeartBeat,
		},
		},
		ReturnAttributes: []string{dao.Index, dao.HeartBeat, dao.State},
	}
	for _, index := range lastStateHeartBeatIndices {
		query.Filters = append(query.Filters, api.Expression{
			AttributeName: dao.Index,
			Operator:      "<>",
			Value:         index,
		})
	}
	stepRecords, _, err := BL.listStepsByQuery(query)
	if err != nil {
		panic(err)
	}
	if len(stepRecords) > 0 {
		var maxHeartBeat time.Time
		var maxHeartBeatIndices []int64
		t.rego.inputMutex.Lock()
		input := t.rego.input
		for _, record := range stepRecords {
			if time.Time(record.Heartbeat).After(maxHeartBeat) {
				maxHeartBeat = time.Time(record.Heartbeat)
				maxHeartBeatIndices = []int64{}
			}
			if time.Time(record.Heartbeat).Equal(maxHeartBeat) {
				maxHeartBeatIndices = append(maxHeartBeatIndices, record.Index)
			}
			var state map[string]interface{}
			err = json.Unmarshal([]byte(record.State), &state)
			if err != nil {
				t.rego.inputMutex.Unlock()
				panic(err)
			}
			input["labels"].(map[string]interface{})[t.indicesToLabels[record.Index]] = state
		}
		t.rego.input = input
		t.rego.lastStateHeartBeat = maxHeartBeat
		t.rego.lastStateHeartBeatIndices = maxHeartBeatIndices
		t.rego.inputMutex.Unlock()
	}
}

func (t *Template) initInput() bool {
	wasInit := false
	t.rego.inputMutex.Lock()
	defer t.rego.inputMutex.Unlock()
	if t.rego.input == nil {
		var err error
		wasInit = true
		t.rego.compiler, err = ast.CompileModules(map[string]string{})
		if err != nil {
			panic(err)
		}
		t.rego.input = map[string]interface{}{
			"template": map[string]interface{}{
				"title":   t.Title,
				"version": t.Version,
			},
		}
		for _, step := range t.Steps {
			t.rego.input["labels"] = map[string]interface{}{
				step.Label: map[string]interface{}{},
			}
			t.rego.input["steps"] = map[string]interface{}{
				step.Label: map[string]interface{}{
					"name":    step.Name,
					"retries": step.Retries,
					"do": map[string]interface{}{
						"type": step.doType,
					},
				},
			}
		}
	}
	return wasInit
}

func (t *Template) LoadAndCreateRun(BL *BL, key string, fileName string, fileType string) (string, error) {
	yamlDocument, err := ioutil.ReadFile(fileName)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", fileName, err)
	}
	isYaml := true
	if strings.EqualFold(fileType, "json") {
		isYaml = false
	}
	err = t.LoadFromBytes(BL, "", isYaml, yamlDocument)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal file %s: %w", fileName, err)
	}
	if dao.IsRemote {
		var runId string
		runId, _, _, err = BL.Client.RemoteCreateRun(&api.CreateRunParams{
			Key:      key,
			Template: t,
		})
		if err != nil {
			return "", fmt.Errorf("failed to create run remotely for file %s: %w", fileName, err)
		}
		return runId, err
	} else {
		var runRow *api.RunRecord
		runRow, err = t.CreateRun(BL, key)
		if err != nil {
			return "", fmt.Errorf("failed to start: %w", err)
		}
		return runRow.Id, err
	}

}

func (s *Step) AdjustUnmarshalStep(t *Template, index int64) error {
	if s.Label == "" {
		return api.NewError(api.ErrInvalidParams, "label must be specified for step: %v", *s)
	}
	_, ok := t.labelsToIndices[s.Label]
	if ok {
		return api.NewError(api.ErrInvalidParams, "label must be unique among steps: %s", s.Label)
	}
	t.labelsToIndices[s.Label] = index
	t.indicesToLabels[index] = s.Label
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
func (t *Template) ResolveCurlyPercent(BL *BL, str string) (string, error) {
	var buffer bytes.Buffer
	tokens, escapedStr := TokenizeCurlyPercent(str)
	if len(tokens) == 0 {
		return str, nil
	}
	tokenEnd := 0
	for _, token := range tokens {
		buffer.WriteString(escapedStr[tokenEnd : token.Start-2])
		tokenEnd = token.End + 2
		queryStr := escapedStr[token.Start:token.End]
		ctx, cancel := context.WithTimeout(BL.ValveCtx, time.Duration(BL.maxRegoEvaluationTimeoutSeconds)*time.Second)
		query, err := rego.New(
			rego.Query(queryStr),
			rego.Compiler(t.rego.compiler),
		).PrepareForEval(ctx)
		cancel()
		if err != nil {
			return "", api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to resolve curly percent for: %s, %w", escapedStr[token.Start:token.End], err)
		}
		var eval rego.ResultSet
		eval, err = t.evaluateCurlyPercent(BL, query)
		if err != nil {
			return "", api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to resolve curly percent for: %s, %w", escapedStr[token.Start:token.End], err)
		}
		if len(eval) > 0 &&
			len(eval[0].Expressions) > 0 &&
			eval[0].Expressions[0].Value != nil {
			buffer.WriteString(fmt.Sprintf("%v", eval[0].Expressions[0].Value))
		} else {
			return "", api.NewError(api.ErrTemplateEvaluationFailed, "failed to resolve curly percent for: %s", escapedStr[token.Start:token.End])
		}
	}
	buffer.WriteString(escapedStr[tokenEnd:])
	return buffer.String(), nil
}

func (t *Template) evaluateCurlyPercent(BL *BL, query rego.PreparedEvalQuery) (rego.ResultSet, error) {
	ctx, cancel := context.WithTimeout(BL.ValveCtx, time.Duration(BL.maxRegoEvaluationTimeoutSeconds)*time.Second)
	defer cancel()
	t.rego.inputMutex.RLock()
	defer t.rego.inputMutex.RUnlock()
	return query.Eval(ctx, rego.EvalInput(t.rego.input))
}
func (t *Template) ResolveContext(BL *BL, context string) (api.Context, error) {
	var result api.Context
	resolvedContextStr, err := t.ResolveCurlyPercent(BL, context)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(resolvedContextStr), &result)
	if err != nil {
		panic(err)
	}
	return result, nil
}
