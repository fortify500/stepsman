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
package cmd

import (
	"github.com/spf13/cobra"
	"os"
	"os/exec"
)

var bangCmd = &cobra.Command{
	Use:   "!",
	Short: "! will execute a shell command.",
	Long: `! will execute a shell command and will not be logged.`,
	Run: func(cmd *cobra.Command, args []string){
		var newArgs []string
		newArgs = append(newArgs, "-c")
		newArgs = append(newArgs, args...)
		bangCmd := exec.Command("/bin/sh", newArgs...)
		bangCmd.Stdin = os.Stdin
		bangCmd.Stdout = os.Stdout
		bangCmd.Stderr = os.Stderr
		bangCmd.Run()
	},
}



func init() {
	RootCmd.AddCommand(bangCmd)
}
