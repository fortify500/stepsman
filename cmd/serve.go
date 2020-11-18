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
	serveServe "github.com/fortify500/stepsman/serve"
	"github.com/spf13/cobra"
)

var ServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "serve will enter server mode and serve http requests",
	Long:  `Use serve to remote control stepsman via http. You can query, monitor and do remotely.`,
	Run: func(cmd *cobra.Command, args []string) {
		defer recoverAndLog("failed to serve")
		syncServeParams()
		if Parameters.InPromptMode {
			msg := "serve is not available from within prompt"
			Parameters.Err = &Error{
				Technical: fmt.Errorf(msg),
				Friendly:  msg,
			}
			return
		}
		serveServe.Serve(Parameters.ServerPort, Parameters.ServerHealthPort)
	},
}

var serveParams AllParameters

func syncServeParams() {
	Parameters.ServerPort = serveParams.ServerPort
	Parameters.ServerHealthPort = serveParams.ServerHealthPort
}
func init() {
	ServeCmd.Flags().Int64VarP(&serveParams.ServerPort, "serve-port", "p", 3333, "server port number")
	ServeCmd.Flags().Int64VarP(&serveParams.ServerHealthPort, "serve-health-port", "t", 3334, "server health port number")
	RootCmd.AddCommand(ServeCmd)
}
