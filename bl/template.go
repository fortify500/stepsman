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
	"regexp"
	"strings"
	"sync"
	"time"
)

type DoType string

const (
	DoTypeREST     DoType = "REST"
	DoTypeEVALUATE DoType = "EVALUATE"
)

type Rego struct {
	compiler                  *ast.Compiler
	preparedModuleQueries     map[string]rego.PreparedEvalQuery
	input                     map[string]interface{}
	lastStateHeartBeat        time.Time
	lastStateHeartBeatIndices []int64
	inputMutex                sync.RWMutex
}

type Template struct {
	Title           string         `json:"title,omitempty"`
	Version         int64          `json:"version,omitempty"`
	Steps           []Step         `json:"steps,omitempty"`
	Tags            api.Tags       `json:"tags,omitempty"`
	Decisions       []Decision     `json:"decisions,omitempty"`
	Parameters      api.Parameters `json:"parameters,omitempty"`
	labelsToIndices map[string]int64
	indicesToLabels map[int64]string
	rego            *Rego
}
type Decision struct {
	Name   string `json:"name,omitempty"`
	Label  string `json:"label,omitempty"`
	Result string `json:"result,omitempty"`
}

type ThenDo struct {
	Label   string `json:"label,omitempty"`
	Context string `json:"context,omitempty"`
}
type ThenError struct {
	Propagate *bool  `json:"propagate,omitempty"`
	Message   string `json:"message,omitempty"`
	Data      string `json:"data,omitempty"`
}
type Then struct {
	Error *ThenError `json:"error,omitempty"`
	Do    []ThenDo   `json:"do,omitempty"`
}
type Rule struct {
	If   string `json:"if,omitempty"`
	Then *Then  `json:"then,omitempty"`
}
type EventDecision struct {
	Label string `json:"label"`
	Input string `json:"input,omitempty"`
}
type Event struct {
	Decisions []EventDecision `json:"decisions,omitempty"`
	Rules     []Rule          `json:"rules,omitempty"`
}
type On struct {
	InProgress Event `json:"in-progress,omitempty" mapstructure:"in-progress" yaml:"in-progress,omitempty"`
	Pending    Event `json:"pending,omitempty" mapstructure:"pending" yaml:"pending,omitempty"`
	Done       Event `json:"done,omitempty" mapstructure:"done" yaml:"done,omitempty"`
	Failed     Event `json:"failed,omitempty" mapstructure:"failed" yaml:"failed,omitempty"`
}
type Step struct {
	Name        string         `json:"name,omitempty"`
	Tags        api.Tags       `json:"tags,omitempty"`
	Label       string         `json:"label,omitempty"`
	Description string         `json:"description,omitempty"`
	Parameters  api.Parameters `json:"parameters,omitempty"`
	Do          interface{}    `json:"do,omitempty"`
	On          On             `json:"on,omitempty"`
	Retries     int            `json:"retries,omitempty"`
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
type StepDoEvaluate struct {
	StepDo  `yaml:",inline" mapstructure:",squash"`
	Options StepDoEvaluateOptions `json:"options,omitempty"`
}

type StepDoRESTOptions struct {
	Timeout              int64       `json:"timeout,omitempty"`
	Method               string      `json:"method,omitempty"`
	Url                  string      `json:"url,omitempty"`
	Headers              http.Header `json:"headers,omitempty"`
	MaxResponseBodyBytes int64       `json:"max-response-body-bytes,omitempty" mapstructure:"max-response-body-bytes" yaml:"max-response-body-bytes,omitempty"`
	Body                 string      `json:"body,omitempty"`
}
type StepDoEvaluateOptions struct {
	Result string `json:"result,omitempty"`
}

func (do StepDoREST) Describe() string {
	doStr, err := yaml.Marshal(do)
	if err != nil {
		panic(err)
	}
	return string(doStr)
}

var LabelFormat = regexp.MustCompile(`^[a-zA-Z][a-zA-Z_0-9]*`)

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
	err = t.validate(runId)
	if err != nil {
		return fmt.Errorf("failed to load from bytes: %w", err)
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
	err = t.initRegoAndInput(BL)
	if err != nil {
		return fmt.Errorf("failed to load from bytes: %w", err)
	}
	BL.templateCache.Add(runId, t)
	return nil
}

func (t *Template) validate(runId string) error {
	stepLabelsExist := make(map[string]int)
	decisionsLabelsExist := make(map[string]int)
	for i, step := range t.Steps {
		if step.Label == "" {
			return api.NewError(api.ErrInvalidParams, "step label must be specified for run id:index: %s:%d", runId, i+1)
		}
		if !LabelFormat.MatchString(step.Label) {
			return api.NewError(api.ErrInvalidParams, "step label specification must conform to: %s, given:%s", LabelFormat.String(), step.Label)
		}
		index, ok := stepLabelsExist[step.Label]
		if ok {
			return api.NewError(api.ErrInvalidParams, "label for step: %d, already exists in step %d for label:%s", i+1, index, step.Label)
		}
		stepLabelsExist[step.Label] = i + 1
	}
	for i, decision := range t.Decisions {
		if decision.Label == "" {
			return api.NewError(api.ErrInvalidParams, "decision label must be specified for run id:decision: %s:%d", runId, i+1)
		}
		if !LabelFormat.MatchString(decision.Label) {
			return api.NewError(api.ErrInvalidParams, "decision label specification must conform to: %s, given:%s", LabelFormat.String(), decision.Label)
		}
		index, ok := decisionsLabelsExist[decision.Label]
		if ok {
			return api.NewError(api.ErrInvalidParams, "label for step: %d, already exists in step %d for label:%s", i+1, index, decision.Label)
		}
		decisionsLabelsExist[decision.Label] = i + 1
	}
	for _, step := range t.Steps {
		events := []Event{step.On.InProgress, step.On.Failed, step.On.Done, step.On.Pending}
		for _, event := range events {
			for _, rule := range event.Rules {
				if rule.Then == nil {
					continue
				}
				for _, do := range rule.Then.Do {
					_, ok := stepLabelsExist[do.Label]
					if !ok {
						return api.NewError(api.ErrInvalidParams, "do label %s in rule does not exist", do.Label)
					}
				}
			}
			for _, decision := range event.Decisions {
				_, ok := decisionsLabelsExist[decision.Label]
				if !ok {
					return api.NewError(api.ErrInvalidParams, "decision label %s in rule does not exist", decision.Label)
				}
			}
		}
	}

	return nil
}

func (t *Template) RefreshInput(BL *BL, runId string) {
	t.rego.inputMutex.RLock()
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
		labels := t.rego.input["labels"].(map[string]interface{})
		for _, record := range stepRecords {
			if time.Time(record.Heartbeat).After(maxHeartBeat) {
				maxHeartBeat = time.Time(record.Heartbeat)
				maxHeartBeatIndices = []int64{}
			}
			if time.Time(record.Heartbeat).Equal(maxHeartBeat) {
				maxHeartBeatIndices = append(maxHeartBeatIndices, record.Index)
			}
			labels[t.indicesToLabels[record.Index]] = record.State
		}
		t.rego.input["labels"] = labels
		t.rego.lastStateHeartBeat = maxHeartBeat
		t.rego.lastStateHeartBeatIndices = maxHeartBeatIndices
		t.rego.inputMutex.Unlock()
	}
}

func (t *Template) initRegoAndInput(BL *BL) error {
	var err error
	modules := make(map[string]*ast.Module)
	for iStep, step := range t.Steps {
		events := []Event{step.On.InProgress, step.On.Failed, step.On.Done, step.On.Pending}
		for _, event := range events {
			for jRule, rule := range event.Rules {
				if rule.If != "" {
					var parsedModule *ast.Module
					moduleName := regoModuleNameForIf(iStep, jRule)
					module := fmt.Sprintf("package %s\n%s", moduleName, rule.If)
					parsedModule, err = ast.ParseModule(moduleName, module)
					if err != nil {
						return api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to load rego step components: %w", err)
					}
					modules[moduleName] = parsedModule
				}
			}
		}
	}

	for _, decision := range t.Decisions {
		var parsedModule *ast.Module
		moduleName := regoModuleNameForDecision(decision.Label)
		module := fmt.Sprintf("package %s\n%s", moduleName, decision.Result)
		parsedModule, err = ast.ParseModule(moduleName, module)
		if err != nil {
			return api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to load rego decision components: %w", err)
		}
		modules[moduleName] = parsedModule
	}
	var allowedCapabilities ast.Capabilities
	allowedCapabilities.Builtins = DefaultAllowedBuiltins
	compiler := ast.NewCompiler().WithCapabilities(&allowedCapabilities)
	compiler.Compile(modules)
	if compiler.Failed() {
		return api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to compile rego components: %v", compiler.Errors)
	}
	t.rego.inputMutex.Lock()
	defer t.rego.inputMutex.Unlock()

	t.rego.compiler = compiler
	t.rego.preparedModuleQueries = make(map[string]rego.PreparedEvalQuery)
	for iStep, step := range t.Steps {
		events := []Event{step.On.InProgress, step.On.Failed, step.On.Done, step.On.Pending}
		for _, event := range events {
			for jRule, rule := range event.Rules {
				if rule.If != "" {
					moduleName := regoModuleNameForIf(iStep, jRule)
					results := fmt.Sprintf("data.%s.result", moduleName)
					t.rego.preparedModuleQueries[moduleName], err = rego.New(
						rego.Query(results),
						rego.Compiler(compiler),
					).PrepareForEval(BL.ValveCtx)
					if err != nil {
						return api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to compile rego components: %s", results)
					}
				}
			}
		}
	}

	for _, decision := range t.Decisions {
		moduleName := regoModuleNameForDecision(decision.Label)
		results := fmt.Sprintf("data.%s", moduleName)
		t.rego.preparedModuleQueries[moduleName], err = rego.New(
			rego.Query(results),
			rego.Compiler(compiler),
		).PrepareForEval(BL.ValveCtx)
		if err != nil {
			return api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to compile rego components: %s", results)
		}
	}
	t.rego.input = map[string]interface{}{
		"template": map[string]interface{}{
			"title":   t.Title,
			"version": t.Version,
		},
	}
	if t.Parameters != nil {
		t.rego.input["template"] = map[string]interface{}{
			"parameters": t.Parameters,
		}
	}
	labels := map[string]interface{}{}
	inputSteps := map[string]interface{}{}
	for _, step := range t.Steps {
		inputSteps[step.Label] = map[string]interface{}{
			"name":    step.Name,
			"label":   step.Label,
			"retries": step.Retries,
			"do": map[string]interface{}{
				"type": step.doType,
			},
		}
	}
	t.rego.input["labels"] = labels
	t.rego.input["steps"] = inputSteps
	return nil
}

func regoModuleNameForIf(stepIndex int, ruleIndex int) string {
	return fmt.Sprintf("step%drule%d", stepIndex+1, ruleIndex+1)
}

func regoModuleNameForDecision(decisionLabel string) string {
	return fmt.Sprintf("decision_%s", decisionLabel)
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
		case DoTypeEVALUATE:
			doEvaluate := StepDoEvaluate{}
			var md mapstructure.Metadata
			decoder, err := mapstructure.NewDecoder(
				&mapstructure.DecoderConfig{
					Metadata: &md,
					Result:   &doEvaluate,
				})
			if err != nil {
				return api.NewWrapError(api.ErrInvalidParams, err, "failed to adjust step do evaluate: %w", err)
			}
			err = decoder.Decode(s.Do)
			if err != nil {
				return api.NewWrapError(api.ErrInvalidParams, err, "failed to adjust step do evaluate: %w", err)
			}
			if len(md.Unused) > 0 {
				return api.NewError(api.ErrInvalidParams, "unsupported attributes provided in do options: %s", strings.Join(md.Unused, ","))
			}
			s.stepDo = doEvaluate.StepDo
			s.Do = doEvaluate
		default:
			return api.NewError(api.ErrInvalidParams, "unsupported do type: %s", doType)
		}
		s.doType = doType
	}
	return nil
}
func (t *Template) EvaluateCurlyPercent(BL *BL, step *Step, str string, currentContext api.Context, decisions api.Input, uncommitted *uncommittedResult) (string, error) {
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
		t.rego.inputMutex.RLock()
		query, err := rego.New(
			rego.Query(queryStr),
			rego.Compiler(t.rego.compiler),
		).PrepareForEval(ctx)
		t.rego.inputMutex.RUnlock()
		cancel()
		if err != nil {
			return "", api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "rego failed to parse curly percent for: %s, %w", escapedStr[token.Start:token.End], err)
		}
		var eval rego.ResultSet
		eval, err = t.evaluateCurlyPercent(BL, step, query, currentContext, decisions, uncommitted)
		if err != nil {
			return "", api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "rego failed to evaluate curly percent for: %s, %w", escapedStr[token.Start:token.End], err)
		}
		if len(eval) > 0 &&
			len(eval[0].Expressions) > 0 &&
			eval[0].Expressions[0].Value != nil {
			var marshaledValue []byte
			marshaledValue, err = json.Marshal(eval[0].Expressions[0].Value)
			if err != nil {
				return "", api.NewWrapError(api.ErrTemplateEvaluationFailed, err, "failed to extract result from rego for a curly percent for: %s, %w", escapedStr[token.Start:token.End], err)
			}
			s := string(marshaledValue)
			if strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") {
				s = strings.TrimSuffix(strings.TrimPrefix(s, "\""), "\"")
			}
			buffer.WriteString(s)
		}
		// this would return an error on evaluation, but currently the decision is not to fail on missing values. (in the future maybe we will use something like curly percent that does).
		//else {
		//	return "", api.NewError(api.ErrTemplateEvaluationFailed, "failed to evaluate curly percent, rego did not return a result for: %s", escapedStr[token.Start:token.End])
		//}
	}
	buffer.WriteString(escapedStr[tokenEnd:])
	return buffer.String(), nil
}

func (t *Template) evaluateCurlyPercent(BL *BL, step *Step, query rego.PreparedEvalQuery, currentContext api.Context, decisions api.Input, uncommitted *uncommittedResult) (rego.ResultSet, error) {
	newInput := t.fillInput(currentContext, step, decisions, uncommitted)
	ctx, cancel := context.WithTimeout(BL.ValveCtx, time.Duration(BL.maxRegoEvaluationTimeoutSeconds)*time.Second)
	defer cancel()

	return query.Eval(ctx, rego.EvalInput(newInput))
}

type uncommittedResult struct {
	label string
	state api.State
}

func (t *Template) fillInput(currentContext api.Context, step *Step, decisionsResult api.Input, uncommitted *uncommittedResult) map[string]interface{} {
	var newInput map[string]interface{}
	newInput = t.duplicateInput()
	newInput["context"] = currentContext
	if step.Parameters != nil {
		newInput["parameters"] = step.Parameters
	}
	newInput["step"] = newInput["steps"].(map[string]interface{})[step.Label]
	if step.Tags != nil {
		newInput["tags"] = step.Tags
	}
	if decisionsResult != nil && len(decisionsResult) > 0 {
		newInput["decisions"] = decisionsResult
	}
	if uncommitted != nil {
		newInput["labels"].(map[string]interface{})[uncommitted.label] = uncommitted.state
	}
	return newInput
}

func (t *Template) duplicateInput() map[string]interface{} {
	newInput := make(map[string]interface{})
	t.rego.inputMutex.RLock()
	defer t.rego.inputMutex.RUnlock()
	// two levels deep copy should be enough to avoid race conditions at this time. But keep an eye on this.
	for k, v := range t.rego.input {
		switch v.(type) {
		case map[string]interface{}:
			newMap := make(map[string]interface{})
			for subK, subV := range v.(map[string]interface{}) {
				newMap[subK] = subV
			}
			newInput[k] = newMap
		default:
			newInput[k] = v
		}
	}

	return newInput
}
func (t *Template) ResolveContext(BL *BL, step *Step, jsonStr string, currentContext api.Context, uncommitted *uncommittedResult) (api.Context, error) {
	var result api.Context
	if jsonStr == "" {
		jsonStr = "{}"
	}
	resolvedContextStr, err := t.EvaluateCurlyPercent(BL, step, jsonStr, currentContext, nil, uncommitted)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(resolvedContextStr), &result)
	if err != nil {
		return nil, api.NewError(api.ErrTemplateEvaluationFailed, "failed to resolve context for: %s", resolvedContextStr)
	}
	return result, nil
}
func (t *Template) ResolveInput(BL *BL, step *Step, jsonStr string, currentContext api.Context, uncommitted *uncommittedResult) (api.Input, error) {
	var result api.Input
	if jsonStr == "" {
		jsonStr = "{}"
	}
	resolvedInputStr, err := t.EvaluateCurlyPercent(BL, step, jsonStr, currentContext, nil, uncommitted)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal([]byte(resolvedInputStr), &result)
	if err != nil {
		return nil, api.NewError(api.ErrTemplateEvaluationFailed, "failed to resolve input for: %s", resolvedInputStr)
	}
	return result, nil
}

var DefaultAllowedBuiltins = []*ast.Builtin{
	// Unification/equality ("=")
	ast.Equality,

	// Assignment (":=")
	ast.Assign,

	// Comparisons
	ast.GreaterThan,
	ast.GreaterThanEq,
	ast.LessThan,
	ast.LessThanEq,
	ast.NotEqual,
	ast.Equal,

	// Arithmetic
	ast.Plus,
	ast.Minus,
	ast.Multiply,
	ast.Divide,
	ast.Round,
	ast.Abs,
	ast.Rem,

	// Bitwise Arithmetic
	ast.BitsOr,
	ast.BitsAnd,
	ast.BitsNegate,
	ast.BitsXOr,
	ast.BitsShiftLeft,
	ast.BitsShiftRight,

	// Binary
	ast.And,
	ast.Or,

	// Aggregates
	ast.Count,
	ast.Sum,
	ast.Product,
	ast.Max,
	ast.Min,
	ast.Any,
	ast.All,

	// Arrays
	ast.ArrayConcat,
	ast.ArraySlice,

	// Conversions
	ast.ToNumber,

	// Casts (DEPRECATED)
	ast.CastObject,
	ast.CastNull,
	ast.CastBoolean,
	ast.CastString,
	ast.CastSet,
	ast.CastArray,

	// Regular Expressions
	ast.RegexIsValid,
	ast.RegexMatch,
	ast.RegexMatchDeprecated,
	ast.RegexSplit,
	ast.GlobsMatch,
	ast.RegexTemplateMatch,
	ast.RegexFind,
	ast.RegexFindAllStringSubmatch,

	// Sets
	ast.SetDiff,
	ast.Intersection,
	ast.Union,

	// Strings
	ast.Concat,
	ast.FormatInt,
	ast.IndexOf,
	ast.Substring,
	ast.Lower,
	ast.Upper,
	ast.Contains,
	ast.StartsWith,
	ast.EndsWith,
	ast.Split,
	ast.Replace,
	ast.ReplaceN,
	ast.Trim,
	ast.TrimLeft,
	ast.TrimPrefix,
	ast.TrimRight,
	ast.TrimSuffix,
	ast.TrimSpace,
	ast.Sprintf,

	// Numbers
	ast.NumbersRange,

	// Encoding
	ast.JSONMarshal,
	ast.JSONUnmarshal,
	ast.Base64Encode,
	ast.Base64Decode,
	ast.Base64IsValid,
	ast.Base64UrlEncode,
	ast.Base64UrlDecode,
	ast.URLQueryDecode,
	ast.URLQueryEncode,
	ast.URLQueryEncodeObject,
	ast.URLQueryDecodeObject,
	ast.YAMLMarshal,
	ast.YAMLUnmarshal,

	// Object Manipulation
	ast.ObjectUnion,
	ast.ObjectRemove,
	ast.ObjectFilter,
	ast.ObjectGet,

	// JSON Object Manipulation
	ast.JSONFilter,
	ast.JSONRemove,

	// Tokens
	ast.JWTDecode,
	ast.JWTVerifyRS256,
	ast.JWTVerifyRS384,
	ast.JWTVerifyRS512,
	ast.JWTVerifyPS256,
	ast.JWTVerifyPS384,
	ast.JWTVerifyPS512,
	ast.JWTVerifyES256,
	ast.JWTVerifyES384,
	ast.JWTVerifyES512,
	ast.JWTVerifyHS256,
	ast.JWTVerifyHS384,
	ast.JWTVerifyHS512,
	ast.JWTDecodeVerify,
	ast.JWTEncodeSignRaw,
	ast.JWTEncodeSign,

	// Time
	ast.NowNanos,
	ast.ParseNanos,
	ast.ParseRFC3339Nanos,
	ast.ParseDurationNanos,
	ast.Date,
	ast.Clock,
	ast.Weekday,
	ast.AddDate,

	// Crypto
	ast.CryptoX509ParseCertificates,
	ast.CryptoMd5,
	ast.CryptoSha1,
	ast.CryptoSha256,
	ast.CryptoX509ParseCertificateRequest,

	// Graphs
	ast.WalkBuiltin,
	ast.ReachableBuiltin,

	// Sort
	ast.Sort,

	// Types
	ast.IsNumber,
	ast.IsString,
	ast.IsBoolean,
	ast.IsArray,
	ast.IsSet,
	ast.IsObject,
	ast.IsNull,
	ast.TypeNameBuiltin,

	// HTTP
	//HTTPSend,

	// Rego
	//ast.RegoParseModule,

	// OPA
	//ast.OPARuntime,

	// Tracing
	//ast.Trace,

	// CIDR
	ast.NetCIDROverlap,
	ast.NetCIDRIntersects,
	ast.NetCIDRContains,
	ast.NetCIDRContainsMatches,
	ast.NetCIDRExpand,
	ast.NetCIDRMerge,

	// Glob
	ast.GlobMatch,
	ast.GlobQuoteMeta,

	// Units
	ast.UnitsParseBytes,

	// UUIDs
	ast.UUIDRFC4122,

	//SemVers
	ast.SemVerIsValid,
	ast.SemVerCompare,
}
