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
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/exec"
)

var bangCmd = &cobra.Command{
	Use:   "!",
	Short: "! will execute a shell command from the prompt command.",
	Long:  `! will execute a shell command from the prompt command and will not be logged.`,
	Run: func(cmd *cobra.Command, args []string) {
		defer recoverAndLog("failed to execute !")
		var err error
		var newArgs []string
		Parameters.CurrentCommand = CommandBang
		if !Parameters.InPromptMode {
			msg := "! is only available from within prompt"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
		newArgs = append(newArgs, "-c")
		newArgs = append(newArgs, args...)
		bangC := exec.Command("/bin/sh", newArgs...)
		bangC.Stdin = os.Stdin
		bangC.Stdout = os.Stdout
		bangC.Stderr = os.Stderr
		err = bangC.Run()
		if err != nil {
			msg := "failed to execute \"!\""
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg+": %w", err),
				Friendly:  msg,
			}
			return
		}
	},
}

func init() {
	RootCmd.AddCommand(bangCmd)
}
