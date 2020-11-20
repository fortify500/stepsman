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
	"errors"
	"fmt"
	"github.com/fortify500/stepsman/api"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/client"
	"github.com/fortify500/stepsman/cmd"
	"github.com/fortify500/stepsman/dao"
	"github.com/fortify500/stepsman/serve"
	"github.com/gobs/args"
	"github.com/google/uuid"
	"github.com/open-policy-agent/opa/rego"
	"io/ioutil"
	"os"
	"regexp"
	"sync"
	"testing"
	"time"
)

func setup() {
	cmd.InitConfig()
	cmd.AllowChangeVendor = true
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
		"context": map[string]interface{}{
			"status": "in-progress",
		},
	}
	query, err := rego.New(
		rego.Query(fmt.Sprintf("data.s%d.result", 1)),
		rego.Module(fmt.Sprintf("s%d", 1), fmt.Sprintf("package s%d\n%s", 1, "result{input.context[\"status\"]=\"in-progress\"}")),
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
		{`update -V %[1]s step %[3]s -s "Done"`, false, false},
		{`update -V %[1]s step %[3]s -s "Idle"`, false, false},
		{"do -V %[1]s step %[3]s", false, false},
		{"get -V %[1]s step %[3]s", false, false},
	}
	breakOut := false
BreakOut:
	for _, vendor := range vendors {
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
		command := fmt.Sprintf(tc.command, tc.databaseVendor)
		cmd.ResetCommandParameters()
		cmd.RootCmd.SetArgs(args.GetArgs(command))
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			err := cmd.RootCmd.Execute()
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}()
		time.Sleep(time.Duration(2) * time.Second)
		client.InitClient(false, "localhost", 3333)
		createdRunId := ""
		{
			fileName := "examples/basic.yaml"
			var template bl.Template
			yamlDocument, err := ioutil.ReadFile(fileName)
			if err != nil {
				t.Error(fmt.Errorf("failed to read file %s: %w", fileName, err))
				break BreakOut
			}
			err = template.LoadFromBytes(true, yamlDocument)
			if err != nil {
				t.Error(fmt.Errorf("failed to unmarshal file %s: %w", fileName, err))
				break BreakOut
			}
			{
				t.Run(fmt.Sprintf("%s - %s", command, "RemoteCreateRun"), func(t *testing.T) {
					createdRunId, _, _, err = client.RemoteCreateRun(&api.CreateRunParams{
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
			runs, _, err := client.RemoteListRuns(&api.ListQuery{
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
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateRun done"), func(t *testing.T) {
			err := client.RemoteUpdateRun(&api.UpdateQuery{
				Id:      createdRunId,
				Changes: map[string]interface{}{"status": api.RunDone.TranslateRunStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetRuns done"), func(t *testing.T) {
			runs, err := client.RemoteGetRuns(&api.GetRunsQuery{
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
			err := client.RemoteUpdateRun(&api.UpdateQuery{
				Id:      createdRunId,
				Changes: map[string]interface{}{"status": api.RunIdle.TranslateRunStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetRuns done"), func(t *testing.T) {
			runs, err := client.RemoteGetRuns(&api.GetRunsQuery{
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
		var stepUUIDs []string
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteListSteps"), func(t *testing.T) {
			steps, _, err := client.RemoteListSteps(&api.ListQuery{
				Filters: []api.Expression{{
					AttributeName: dao.RunId,
					Operator:      "=",
					Value:         createdRunId,
				}},
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
			steps, err := client.RemoteGetSteps(&api.GetStepsQuery{
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
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateStep idle"), func(t *testing.T) {
			err := client.RemoteUpdateStep(&api.UpdateQuery{
				Id:      stepUUIDs[0],
				Changes: map[string]interface{}{"status": api.StepDone.TranslateStepStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := client.RemoteGetSteps(&api.GetStepsQuery{
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
					t.Errorf("step status must be done")
				}
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteUpdateStep idle"), func(t *testing.T) {
			err := client.RemoteUpdateStep(&api.UpdateQuery{
				Id:      stepUUIDs[0],
				Changes: map[string]interface{}{"status": api.StepIdle.TranslateStepStatus()},
			})
			if err != nil {
				t.Error(err)
				return
			}
		})
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := client.RemoteGetSteps(&api.GetStepsQuery{
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
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteDoStep"), func(t *testing.T) {
			statusOwner := "allTestOwner"
			response, err := client.RemoteDoStep(&api.DoStepParams{
				UUID:        stepUUIDs[0],
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
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteDoStep"), func(t *testing.T) {
			statusOwner := "allTestOwner"
			response, err := client.RemoteDoStep(&api.DoStepParams{
				UUID:        stepUUIDs[0],
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
		waitForQueuesToFinish()
		t.Run(fmt.Sprintf("%s - %s", command, "RemoteGetSteps"), func(t *testing.T) {
			steps, err := client.RemoteGetSteps(&api.GetStepsQuery{
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
			steps, err := client.RemoteGetSteps(&api.GetStepsQuery{
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
		serve.InterruptServe <- os.Interrupt
		wg.Wait()
		fmt.Println("end")
	}
}

func waitForQueuesToFinish() {
	i := 0
	for !bl.QueuesIdle() {
		i++
		time.Sleep(1 * time.Second)
		if i > 60 {
			break
		}
	}
}
func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
