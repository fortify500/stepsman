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
	"errors"
	"fmt"
	"github.com/fortify500/stepsman/cmd"
	"github.com/gobs/args"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"regexp"
	"testing"
)

func setup() {
	cmd.InitConfig()
	cmd.AllowChangeVendor = true
}
func teardown() {
}
func TestLocal(t *testing.T) {
	var createdRunId string
	testCases := []struct {
		databaseVendor string
		command        string
		parseRunId     bool
	}{
		{"postgresql", "create -V %[1]s -M=true run -f examples/basic.yaml", true},
		{"postgresql", "list -V %[1]s runs", false},
		{"postgresql", "list -V %[1]s steps -r %[2]s", false},
		{"postgresql", "describe -V %[1]s run %[2]s", false},
		{"postgresql", `update -V %[1]s run %[2]s -s "In Progress"`, false},
		{"postgresql", "get -V %[1]s run %[2]s", false},
		{"postgresql", "get -V %[1]s run %[2]s --only-template-type json", false},

		{"sqlite", "create -V %[1]s -M=true run -f examples/basic.yaml", true},
		{"sqlite", "list -V %[1]s runs", false},
		{"sqlite", "list -V %[1]s steps -r %[2]s", false},
		{"sqlite", "describe -V %[1]s run %[2]s", false},
		{"sqlite", `update -V %[1]s run %[2]s -s "In Progress"`, false},
		{"sqlite", "get -V %[1]s run %[2]s", false},
		{"sqlite", "get -V %[1]s run %[2]s --only-template-type yaml", false},
	}
	for _, tc := range testCases {
		command := fmt.Sprintf(tc.command, tc.databaseVendor, createdRunId)
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
			cmdErr := &cmd.Error{}
			if cmd.Parameters.Err != nil {
				if errors.As(cmd.Parameters.Err, &cmdErr) {
					t.Error(cmdErr.TechnicalError())
				} else {
					t.Error(cmd.Parameters.Err)
				}
			}
			w.Close()
			out, _ := ioutil.ReadAll(r)
			os.Stdout = rescueStdout
			var re = regexp.MustCompile(`.*\:\s(.*\-.*\-.*\-.*\-.*).*`)

			if tc.parseRunId {
				for _, match := range re.FindStringSubmatch(string(out)) {
					parse, err := uuid.Parse(match)
					if err != nil {
						continue
					}
					createdRunId = parse.String()
				}
			}
			fmt.Println(string(out))
		})
	}
}

//func TestRemotePostgreSQL(t *testing.T) {
//	testCases := []struct {
//		databaseVendor string
//		command        string
//	}{
//		{"postgresql", "serve -V %s"},
//	}
//	for _, tc := range testCases {
//		command := fmt.Sprintf(tc.command, tc.databaseVendor)
//		cmd.ResetCommandParameters()
//		cmd.RootCmd.SetArgs(args.GetArgs(command))
//		var wg sync.WaitGroup
//		wg.Add(1)
//		go func() {
//			err := cmd.RootCmd.Execute()
//			if err != nil {
//				t.Error(err)
//			}
//			wg.Done()
//		}()
//		fmt.Println("start")
//		t.Run(tc.command, func(t *testing.T) {
//			t.Run("testtest", func(t *testing.T) {
//				t.Parallel()
//				fmt.Println("hello world")
//				time.Sleep(5 * time.Second)
//				fmt.Println("hello world3")
//			})
//			t.Run("testtest", func(t *testing.T) {
//				t.Parallel()
//			})
//			t.Run("testtest", func(t *testing.T) {
//				t.Parallel()
//				fmt.Println("hello world2")
//			})
//		})
//		serve.InterruptServe <- os.Interrupt
//		wg.Wait()
//		fmt.Println("end")
//	}
//}
func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
