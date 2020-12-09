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

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/cmd"
	"github.com/fortify500/stepsman/dao"
	"github.com/fortify500/stepsman/serve"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/gobs/args"
	"github.com/google/uuid"
	"github.com/open-policy-agent/opa/ast"
	"github.com/open-policy-agent/opa/rego"
	"github.com/open-policy-agent/opa/types"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"
)

func setup() {
	cmd.InitConfig()
}
func teardown() {
}
func TestParsing(t *testing.T) {
	s := "hello {%input.context[\"status\"]%} d\\%\\}d {{%input.context[\"status\"]%}}"
	var tokens []bl.DoubleCurlyToken
	tokens, s = bl.TokenizeCurlyPercent(s)
	if len(tokens) != 2 {
		t.Error("failed to parse double curly braces - len")
	}

	if len(tokens) > 0 {
		var buffer bytes.Buffer
		tokenEnd := 0
		for i, token := range tokens {
			buffer.WriteString(s[tokenEnd : token.Start-2])
			tokenEnd = token.End + 2
			buffer.WriteString(fmt.Sprintf("%d", i))
			fmt.Println(fmt.Sprintf("token[%d:%d]:%s", token.Start, token.End, s[token.Start:token.End]))
		}
		buffer.WriteString(s[tokens[len(tokens)-1].End+2:])
		fmt.Println(buffer.String())
	}
	token := tokens[0]
	if token.Start != 8 || token.End != 31 {
		t.Error("failed to parse double curly braces")
		return
	}
	input := map[string]interface{}{
		"context": api.Context{
			"status": "in-progress",
		},
	}

	module, err := ast.ParseModule(
		fmt.Sprintf("s%d", 1), fmt.Sprintf("package s%d\n%s", 1, "result{input.context[\"status\"]=\"in-progress\"}"),
	)
	if err != nil {
		t.Error(err)
		return
	}
	capabilitiesForThisVersion := ast.CapabilitiesForThisVersion()
	compiler := ast.NewCompiler().WithCapabilities(capabilitiesForThisVersion)
	compiler.Compile(map[string]*ast.Module{fmt.Sprintf("s%d", 1): module})
	if compiler.Failed() {
		err = fmt.Errorf("failed to compile: %v", compiler.Errors)
		t.Error(err)
		return
	}
	query, err := rego.New(
		rego.Query(fmt.Sprintf("data.s%d.result", 1)),
		rego.Compiler(compiler),
	).PrepareForEval(context.Background())

	if err != nil {
		t.Error(err)
		return
	}
	eval, err := query.Eval(context.Background(), rego.EvalInput(input))
	if err != nil {
		t.Error(err)
		return
	}
	for _, result := range eval {
		fmt.Println(result)
	}
	query, err = rego.New(
		rego.Query(s[token.Start:token.End]),
	).PrepareForEval(context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	eval, err = query.Eval(context.Background(), rego.EvalInput(input))
	if err != nil {
		t.Error(err)
		return
	}
	for _, result := range eval {
		fmt.Println(result)
	}
	compiler = ast.NewCompiler().WithCapabilities(&ast.Capabilities{
		Builtins: []*ast.Builtin{
			ast.Equal,
			ast.NowNanos,
			ast.Equality,
			{
				Name: "foo",
				Decl: types.NewFunction([]types.Type{types.N}, types.B),
			},
		},
	})
	query, err = rego.New(
		rego.Query("time.now_ns()"),
		rego.Compiler(compiler),
	).PrepareForEval(context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	eval, err = query.Eval(context.Background(), rego.EvalInput(input))
	if err != nil {
		t.Error(err)
		return
	}
	for _, result := range eval {
		fmt.Println(result)
	}
}

func TestLocal(t *testing.T) {
	var createdRunId string
	var createdRunIdStepUUID string
	vendors := []string{"postgresql", "sqlite"}
	testCases := []struct {
		command     string
		parseRunId  bool
		parseStepId bool
	}{
		{"create -V %[1]s -M=true run -f examples/basic.yaml", true, false},
		{"list -V %[1]s runs", false, false},
		{"list -V %[1]s steps -r %[2]s", false, true},
		{"describe -V %[1]s run %[2]s", false, false},
		{`update -V %[1]s run %[2]s -s "In Progress"`, false, false},
		{"get -V %[1]s run %[2]s", false, false},
		{"get -V %[1]s run %[2]s --only-template-type json", false, false},
		{"get -V %[1]s step %[3]s", false, false},
		{`update -V %[1]s step %[3]s -s "Failed"`, false, false},
		{`update -V %[1]s step %[3]s -s "Idle"`, false, false},
		{`do -V %[1]s step %[3]s --context {\"email-authorization\":\"dXNlcjpwYXNzd29yZA==\"}`, false, false},
		{"get -V %[1]s step %[3]s", false, false},
		{"describe -V %[1]s run %[2]s", false, false},
		{"create -V %[1]s -M=true run -f examples/basic.yaml", true, false},
		{`delete -V %[1]s run %[2]s`, false, false},
		{"create -V %[1]s -M=true run -f examples/basic.yaml", true, false},
		{`update -V %[1]s run %[2]s -s "In Progress"`, false, false},
		{`delete -V %[1]s run %[2]s -f`, false, false},
		{"create -V %[1]s -M=true run -f examples/basic.yaml", true, false},
		{"do -V %[1]s step %[2]s --label starting --context {\"email-authorization\":\"dXNlcjpwYXNzd29yZA==\"}", false, false},
	}

	for _, vendor := range vendors {
		breakOut := false
		if cmd.BL != nil {
			waitForQueuesToFinish()
			_ = cmd.BL.ShutdownValve.Shutdown(time.Duration(5) * time.Second)
			cmd.BL.CancelValveCtx()
		}
		cmd.BL = nil
	BreakOut:
		for _, tc := range testCases {
			command := fmt.Sprintf(tc.command, vendor, createdRunId, createdRunIdStepUUID)
			t.Run(command, func(t *testing.T) {
				rescueStdout := os.Stdout
				r, w, _ := os.Pipe()
				os.Stdout = w
				cmd.ResetCommandParameters()
				cmd.RootCmd.SetArgs(args.GetArgs(command))
				err := cmd.RootCmd.Execute()
				if err != nil {
					t.Error(err)
				}
				waitForQueuesToFinish()
				cmdErr := &cmd.Error{}
				if cmd.Parameters.Err != nil {
					if errors.As(cmd.Parameters.Err, &cmdErr) {
						t.Error(cmdErr.TechnicalError())
					} else {
						t.Error(cmd.Parameters.Err)
					}
				}
				_ = w.Close()
				out, _ := ioutil.ReadAll(r)
				os.Stdout = rescueStdout

				if tc.parseRunId {
					var re = regexp.MustCompile(`.*:\s(.*-.*-.*-.*-.*).*`)
					for _, match := range re.FindStringSubmatch(string(out)) {
						var parse uuid.UUID
						parse, err = uuid.Parse(match)
						if err != nil {
							continue
						}
						createdRunId = parse.String()
						break
					}
				}
				if tc.parseStepId {
					var re = regexp.MustCompile(`\b[0-9a-f]{8}\b-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-\b[0-9a-f]{12}\b`)
					for _, match := range re.FindStringSubmatch(string(out)) {
						var parse uuid.UUID
						parse, err = uuid.Parse(match)
						if err != nil {
							continue
						}
						createdRunIdStepUUID = parse.String()
						fmt.Println(fmt.Sprintf("located step uuid: %s", createdRunIdStepUUID))
						break
					}
				}
				fmt.Println(string(out))
				if t.Failed() {
					breakOut = true
				}
			})
			if breakOut {
				break BreakOut
			}
		}
	}
	if cmd.BL != nil {
		waitForQueuesToFinish()
		_ = cmd.BL.ShutdownValve.Shutdown(time.Duration(5) * time.Second)
		cmd.BL.CancelValveCtx()
	}
}

func TestRemotePostgreSQL(t *testing.T) {
	testCases := []struct {
		databaseVendor string
		command        string
	}{
		{"postgresql", "serve -M -V %s"},
	}
	breakOut := false
BreakOut:
	for _, tc := range testCases {
		waitForQueuesToFinish()
		cmd.BL = nil
		command := fmt.Sprintf(tc.command, tc.databaseVendor)
		cmd.ResetCommandParameters()
		cmd.RootCmd.SetArgs(args.GetArgs(command))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := cmd.RootCmd.Execute()
			if err != nil {
				t.Error(err)
			}
		}()

		time.Sleep(time.Duration(1) * time.Second)

		httpClient := client.New(false, "localhost", 3333)
		createdRunId := ""
		{
			fileName := "examples/basic.yaml"
			var template bl.Template
			yamlDocument, err := ioutil.ReadFile(fileName)
			if err != nil {
				t.Error(fmt.Errorf("failed to read file %s: %w", fileName, err))
				break BreakOut
			}
			err = template.LoadFromBytes(cmd.BL, "", true, yamlDocument)
			if err != nil {
				t.Error(fmt.Errorf("failed to unmarshal file %s: %w", fileName, err))
				break BreakOut
			}
			{
				t.Run(fmt.Sprintf("%s - %s", command, "RemoteCreateRun"), func(t *testing.T) {
					createdRunId, _, _, err = httpClient.RemoteCreateRun(&api.CreateRunParams{
						Key:      "",
						Template: template,
					})
					if err != nil {
						t.Error(err)
						breakOut = true
					}
				})
				if breakOut {
					break BreakOut
				}
			}
		}
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteListRuns"), func(t *testing.T) {
			runs, _, err := httpClient.RemoteListRuns(&api.ListQuery{
				Filters: []api.Expression{{
					AttributeName: "id",
					Operator:      "=",
					Value:         createdRunId,
				}},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(runs) != 1 {
				t.Error(fmt.Errorf("only one run should be returned, got: %d", len(runs)))
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteListRuns"), func(t *testing.T) {
			runs, _, err := httpClient.RemoteListRuns(&api.ListQuery{
				Filters: []api.Expression{{
					AttributeName: dao.Tags,
					Operator:      "contains",
					Value:         []string{"31be0615-2659-452a-bce3-5d23fec89dfc", "992dca17-7452-492e-a5bd-fa06a7df9fe6"},
				}},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(runs) == 0 {
				t.Error(fmt.Errorf("at least one run should be returned, got: %d", len(runs)))
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteListRuns"), func(t *testing.T) {
			runs, _, err := httpClient.RemoteListRuns(&api.ListQuery{
				Filters: []api.Expression{{
					AttributeName: dao.Tags,
					Operator:      "exists",
					Value:         []string{"31be0615-2659-452a-bce3-5d23fec89dfc", "992dca17-7452-492e-a5bd-fa06a7df9fe6"},
				}},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(runs) == 0 {
				t.Error(fmt.Errorf("at least one run should be returned, got: %d", len(runs)))
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateRun done"), func(t *testing.T) {
			err := httpClient.RemoteUpdateRun(&api.UpdateQueryById{
				Id:      createdRunId,
				Changes: map[string]interface{}{"status": api.RunDone.TranslateRunStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetRuns done"), func(t *testing.T) {
			runs, err := httpClient.RemoteGetRuns(&api.GetRunsQuery{
				Ids:              []string{createdRunId},
				ReturnAttributes: []string{"id", "status"},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(runs) != 1 {
				t.Error(fmt.Errorf("only one run should be returned, got: %d", len(runs)))
				return
			}
			if runs[0].Status != api.RunDone {
				t.Error(fmt.Errorf("status should be done, got: %s", runs[0].Status.TranslateRunStatus()))
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateRun idle"), func(t *testing.T) {
			err := httpClient.RemoteUpdateRun(&api.UpdateQueryById{
				Id:      createdRunId,
				Changes: map[string]interface{}{"status": api.RunIdle.TranslateRunStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetRuns done"), func(t *testing.T) {
			runs, err := httpClient.RemoteGetRuns(&api.GetRunsQuery{
				Ids:              []string{createdRunId},
				ReturnAttributes: []string{"id", "status"},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(runs) != 1 {
				t.Error(fmt.Errorf("only one run should be returned, got: %d", len(runs)))
				return
			}
			if runs[0].Status != api.RunIdle {
				t.Error(fmt.Errorf("status should be idle, got: %s", runs[0].Status.TranslateRunStatus()))
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteListSteps"), func(t *testing.T) {
			steps, _, err := httpClient.RemoteListSteps(&api.ListQuery{
				Sort: api.Sort{
					Fields: []string{"index"},
					Order:  "asc",
				},
				Filters: []api.Expression{{
					AttributeName: dao.Tags,
					Operator:      "contains",
					Value:         []string{"2bab51c3-0f70-44e8-8cc4-4aedbccbbc5c", "ccf295f8-90ff-43ed-87c3-144732d710a8"},
				}},
				ReturnAttributes: nil,
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteListSteps"), func(t *testing.T) {
			steps, _, err := httpClient.RemoteListSteps(&api.ListQuery{
				Sort: api.Sort{
					Fields: []string{"index"},
					Order:  "asc",
				},
				Filters: []api.Expression{{
					AttributeName: dao.Tags,
					Operator:      "exists",
					Value:         []string{"2bab51c3-0f70-44e8-8cc4-4aedbccbbc5c", "ccf295f8-90ff-43ed-87c3-144732d710a8"},
				}},
				ReturnAttributes: nil,
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
			}
		})
		var stepUUIDs []string
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteListSteps"), func(t *testing.T) {
			steps, _, err := httpClient.RemoteListSteps(&api.ListQuery{
				Sort: api.Sort{
					Fields: []string{"index"},
					Order:  "asc",
				},
				Filters: []api.Expression{{
					AttributeName: dao.RunId,
					Operator:      "=",
					Value:         createdRunId,
				}},
				ReturnAttributes: nil,
			})
			if err != nil {
				t.Error(err)
				breakOut = true
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				breakOut = true
				return
			}
			for _, step := range steps {
				stepUUIDs = append(stepUUIDs, step.UUID)
				fmt.Println(fmt.Sprintf("%+v", step))
			}
		})
		if breakOut {
			break BreakOut
		}
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := httpClient.RemoteGetSteps(&api.GetStepsQuery{
				UUIDs: []string{stepUUIDs[0]},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateStepByUUID idle"), func(t *testing.T) {
			err := httpClient.RemoteUpdateStepByUUID(&api.UpdateQueryByUUID{
				UUID:    stepUUIDs[0],
				Changes: map[string]interface{}{"status": api.StepFailed.TranslateStepStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := httpClient.RemoteGetSteps(&api.GetStepsQuery{
				UUIDs: []string{stepUUIDs[0]},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
				if step.Status != api.StepFailed {
					t.Errorf("step status must be failed")
				}
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateStepByUUID idle"), func(t *testing.T) {
			err := httpClient.RemoteUpdateStepByUUID(&api.UpdateQueryByUUID{
				UUID:    stepUUIDs[0],
				Changes: map[string]interface{}{"status": api.StepIdle.TranslateStepStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := httpClient.RemoteGetSteps(&api.GetStepsQuery{
				UUIDs: []string{stepUUIDs[0]},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
				if step.Status != api.StepIdle {
					t.Errorf("step status must be idle")
				}
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteDoStepByUUID"), func(t *testing.T) {
			statusOwner := "allTestOwner"
			response, err := httpClient.RemoteDoStepByUUID(&api.DoStepByUUIDParams{
				UUID:        stepUUIDs[0],
				Context:     api.Context{"email-authorization": "dXNlcjpwYXNzd29yZA=="},
				StatusOwner: statusOwner,
			})
			if err != nil {
				t.Error(err)
				return
			}
			if response.StatusOwner != statusOwner {
				t.Error(fmt.Sprintf("status owner should be: %s, got %s", statusOwner, response.StatusOwner))
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteDoStepByUUID"), func(t *testing.T) {
			statusOwner := "allTestOwner"
			response, err := httpClient.RemoteDoStepByUUID(&api.DoStepByUUIDParams{
				UUID:        stepUUIDs[0],
				StatusOwner: statusOwner,
				Context:     api.Context{"email-authorization": "dXNlcjpwYXNzd29yZA=="},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if response.StatusOwner != statusOwner {
				t.Error(fmt.Sprintf("status owner should be: %s, got %s", statusOwner, response.StatusOwner))
				return
			}
		})
		waitForQueuesToFinish()
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := httpClient.RemoteGetSteps(&api.GetStepsQuery{
				UUIDs: []string{stepUUIDs[0]},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
				if step.Status != api.StepDone {
					t.Errorf("step %s status must be done", step.UUID)
				}
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := httpClient.RemoteGetSteps(&api.GetStepsQuery{
				UUIDs: []string{stepUUIDs[1]},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
				if step.Status != api.StepDone {
					t.Errorf("step %s status must be done", step.UUID)
				}
			}
		})

		{
			fileName := "examples/basic.yaml"
			var template bl.Template
			yamlDocument, err := ioutil.ReadFile(fileName)
			if err != nil {
				t.Error(fmt.Errorf("failed to read file %s: %w", fileName, err))
				break BreakOut
			}
			err = template.LoadFromBytes(cmd.BL, "", true, yamlDocument)
			if err != nil {
				t.Error(fmt.Errorf("failed to unmarshal file %s: %w", fileName, err))
				break BreakOut
			}
			{
				t.Run(fmt.Sprintf("%s - %s", command, "RemoteCreateRun"), func(t *testing.T) {
					createdRunId, _, _, err = httpClient.RemoteCreateRun(&api.CreateRunParams{
						Key:      "",
						Template: template,
					})
					if err != nil {
						t.Error(err)
						breakOut = true
					}
				})
				if breakOut {
					break BreakOut
				}
			}
		}

		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateRun done"), func(t *testing.T) {
			err := httpClient.RemoteUpdateRun(&api.UpdateQueryById{
				Id:      createdRunId,
				Changes: map[string]interface{}{"status": api.RunInProgress.TranslateRunStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})

		t.Run(fmt.Sprintf("%s - %s", command, "RemoteDeleteRun done"), func(t *testing.T) {
			err := httpClient.RemoteDeleteRuns(&api.DeleteQuery{
				Ids:   []string{createdRunId},
				Force: false,
			})
			if err != nil {
				var apiErr *api.Error
				if !errors.As(err, &apiErr) && apiErr.Code() == api.ErrCannotDeleteRunIsInProgress {
					t.Error(err)
					return
				}
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteDeleteRun done"), func(t *testing.T) {
			err := httpClient.RemoteDeleteRuns(&api.DeleteQuery{
				Ids:   []string{createdRunId},
				Force: true,
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		{
			fileName := "examples/basic.yaml"
			var template bl.Template
			yamlDocument, err := ioutil.ReadFile(fileName)
			if err != nil {
				t.Error(fmt.Errorf("failed to read file %s: %w", fileName, err))
				break BreakOut
			}
			err = template.LoadFromBytes(cmd.BL, "", true, yamlDocument)
			if err != nil {
				t.Error(fmt.Errorf("failed to unmarshal file %s: %w", fileName, err))
				break BreakOut
			}
			{
				t.Run(fmt.Sprintf("%s - %s", command, "RemoteCreateRun"), func(t *testing.T) {
					createdRunId, _, _, err = httpClient.RemoteCreateRun(&api.CreateRunParams{
						Key:      "",
						Template: template,
					})
					if err != nil {
						t.Error(err)
						breakOut = true
					}
				})
				if breakOut {
					break BreakOut
				}
			}
		}
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteDoStepByLabel"), func(t *testing.T) {
			statusOwner := "allTestOwner"
			response, err := httpClient.RemoteDoStepByLabel(&api.DoStepByLabelParams{
				RunId:       createdRunId,
				Label:       "starting",
				StatusOwner: statusOwner,
				Context:     api.Context{"email-authorization": "dXNlcjpwYXNzd29yZA=="},
			})
			if err != nil {
				t.Error(err)
				return
			}
			if response.StatusOwner != statusOwner {
				t.Error(fmt.Sprintf("status owner should be: %s, got %s", statusOwner, response.StatusOwner))
				return
			}
		})
		waitForQueuesToFinish()
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, _, err := httpClient.RemoteListSteps(&api.ListQuery{
				Range: api.RangeQuery{},
				Sort:  api.Sort{},
				Filters: []api.Expression{
					{
						AttributeName: dao.RunId,
						Operator:      "=",
						Value:         createdRunId,
					},
					{
						AttributeName: dao.Label,
						Operator:      "=",
						Value:         "emails",
					},
				},
				ReturnAttributes: nil,
			})
			if err != nil {
				t.Error(err)
				return
			}
			if len(steps) <= 0 {
				t.Error(fmt.Errorf("at least one step should be returned, got: %d", len(steps)))
				return
			}
			for _, step := range steps {
				fmt.Println(fmt.Sprintf("%+v", step))
				if step.Status != api.StepInProgress {
					t.Errorf("step %s status must be done", step.UUID)
				}
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateStepByLabel emails done"), func(t *testing.T) {
			currentStatusOwnerMu.Lock()
			statusOwner := currentStatusOwner
			currentStatusOwnerMu.Unlock()
			err := httpClient.RemoteUpdateStepByLabel(&api.UpdateQueryByLabel{
				RunId:       createdRunId,
				StatusOwner: statusOwner,
				Label:       "emails",
				Force:       false,
				Changes: map[string]interface{}{
					"status": api.StepDone.TranslateStepStatus(),
					"state": map[string]interface{}{
						"result": map[string]interface{}{
							"status": "sent",
						},
					},
				},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		serve.InterruptServe <- os.Interrupt
		wg.Wait()
		fmt.Println("end")
	}
}

var currentStatusOwnerMu sync.Mutex
var currentStatusOwner string

func mockServer() http.Server {
	// Mock
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(60 * time.Second))
	r.Route("/", func(r chi.Router) {
		r.Get("/async", func(w http.ResponseWriter, r *http.Request) {
			statusOwner := r.Header.Get("Stepsman-Status-Owner")
			currentStatusOwnerMu.Lock()
			currentStatusOwner = statusOwner
			currentStatusOwnerMu.Unlock()
			w.Header().Set("Stepsman-Async", "true")
			w.WriteHeader(200)
			test := map[string]interface{}{
				"test": "async",
			}
			if err := json.NewEncoder(w).Encode(test); err != nil {
				panic(err)
			}
		})
		r.Get("/sync/{id}", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			test := map[string]interface{}{
				"test": "sync",
			}
			if err := json.NewEncoder(w).Encode(test); err != nil {
				panic(err)
			}
		})
	})
	return http.Server{Addr: ":3335", Handler: r}
}

func waitForQueuesToFinish() {
	i := 0
	for !cmd.BL.QueuesIdle() {
		i++
		time.Sleep(1 * time.Second)
		if i > 60 {
			break
		}
	}
}
func TestMain(m *testing.M) {
	mockContext, mockCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mockCancel()
	mockSrv := mockServer()
	go func() {
		err := mockSrv.ListenAndServe()
		if err != nil {
			log.Warn(err)
		}
	}()
	defer func() {
		err := mockSrv.Shutdown(mockContext)
		if err != nil {
			log.Warn(err)
		}
	}()
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
