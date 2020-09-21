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
	"strings"
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
		switch strings.TrimSpace(Parameters.DriverName) {
		case "postgresql":
			sslmode := "disable"
			if Parameters.DatabaseSSLMode {
				sslmode = "enable"
			}
			Parameters.DataSourceName = fmt.Sprintf("host=%s port=%d user=%s "+
				"password=%s dbname=%s sslmode=%s",
				Parameters.DatabaseHost,
				Parameters.DatabasePort,
				Parameters.DatabaseUserName,
				Parameters.DatabasePassword,
				Parameters.DatabaseName,
				sslmode)
		}
		err := bl.InitBL(Parameters.DriverName, Parameters.DataSourceName)
		if err != nil {
			return err
		}
		return nil
	},
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&Parameters.DriverName, "driver", "d", "", "supported storage drivers: sqlite, postgresql")
	RootCmd.PersistentFlags().StringVar(&Parameters.DataSourceName, "db-file-name", path.Join(StoreDir, "stepsman.db"), "sqlite file location")
	RootCmd.PersistentFlags().StringVarP(&Parameters.CfgFile, "config", "c", "", "config file (default is $HOME/.stepsman.yaml)")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseHost, "db-host", "t", "localhost", "database host address")
	RootCmd.PersistentFlags().Int64VarP(&Parameters.DatabasePort, "db-port", "p", 5432, "database port address")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseName, "db-name", "n", "stepsman", "database name")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseUserName, "db-user-name", "u", "stepsman", "database user name")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabasePassword, "db-password", "w", "stepsman", "database password")
	RootCmd.PersistentFlags().BoolVarP(&Parameters.DatabaseSSLMode, "db-enable-ssl", "e", false, "database ssl on database port")
}
