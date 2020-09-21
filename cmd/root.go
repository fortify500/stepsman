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
	"fmt"
	"github.com/fortify500/stepsman/bl"
	"github.com/spf13/cobra"
	"path"
)

var RootCmd = &cobra.Command{
	Use:   "stepsman",
	Short: "Step by step managed script",
	Long: `Stepsman is a command line utility to manage processes such as:
* Installations
* Upgrades
* Migrations
* Tests
* Anything that looks like a list of steps to complete

hint: "stepsman prompt" will enter interactive mode`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if Parameters.DriverName == "" {
			msg := "you must specify a supported driver (-d/--driver) or set in config file"
			return fmt.Errorf(msg)
		}
		err := bl.InitBL(Parameters.DriverName, Parameters.DataSourceName)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&Parameters.DriverName, "driver", "d", "", "supported storage drivers: sqlite3")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DataSourceName, "datasource", "o", path.Join(StoreDir, "stepsman.db"), "specify a datasource configuration for the driver selected")
	RootCmd.PersistentFlags().StringVarP(&Parameters.CfgFile, "config", "c", "", "config file (default is $HOME/.stepsman.yaml)")
}
