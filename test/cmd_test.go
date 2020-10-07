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
	"fmt"
	"github.com/fortify500/stepsman/cmd"
	"github.com/fortify500/stepsman/serve"
	"github.com/gobs/args"
	"os"
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
func TestLocal(t *testing.T) {
	testCases := []struct {
		databaseVendor string
		command        string
	}{
		{"postgresql", "create -V %s run -f examples/basic.yaml"},
		{"postgresql", "list -V %s runs"},
	}
	for _, tc := range testCases {
		command := fmt.Sprintf(tc.command, tc.databaseVendor)
		t.Run(command, func(t *testing.T) {
			cmd.ResetParameters()
			cmd.RootCmd.SetArgs(args.GetArgs(command))
			err := cmd.RootCmd.Execute()
			if err != nil {
				t.Error(err)
			}
		})
	}
}
func TestRemotePostgreSQL(t *testing.T) {
	testCases := []struct {
		databaseVendor string
		command        string
	}{
		{"postgresql", "serve -V %s"},
	}
	for _, tc := range testCases {
		command := fmt.Sprintf(tc.command, tc.databaseVendor)
		cmd.ResetParameters()
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
		fmt.Println("start")
		t.Run(tc.command, func(t *testing.T) {
			t.Run("testtest", func(t *testing.T) {
				t.Parallel()
				fmt.Println("hello world")
				time.Sleep(5 * time.Second)
				fmt.Println("hello world3")
			})
			t.Run("testtest", func(t *testing.T) {
				t.Parallel()
			})
			t.Run("testtest", func(t *testing.T) {
				t.Parallel()
				fmt.Println("hello world2")
			})
		})
		serve.InterruptServe <- os.Interrupt
		wg.Wait()
		fmt.Println("end")
	}
}
func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}
