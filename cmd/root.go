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
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/dao"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

var BL *bl.BL
var RootCmd = &cobra.Command{
	Use:   "stepsman",
	Short: "Step by step workflow manager",
	Long: `StepsMan is a step by step event driven business process and workflow manager.

hint: "stepsman prompt" will enter interactive mode`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return PersistentPreRunE()
	},
}

func PersistentPreRunE() error {
	var err error
	if Parameters.DatabaseVendor == "" {
		msg := "you must specify a supported db-vendor (-V/--db-vendor) or set in config file"
		return fmt.Errorf(msg)
	}
	Parameters.GroupId, err = uuid.Parse(Parameters.GroupIdStr)
	if err != nil {
		msg := "failed to parse group id"
		return &Error{
			Technical: fmt.Errorf(msg+": %w", err),
			Friendly:  msg,
		}
	}

	if BL == nil {
		var newBL *bl.BL
		newBL, err = bl.New(&dao.ParametersType{
			DataSourceName:      Parameters.DataSourceName,
			DatabaseVendor:      Parameters.DatabaseVendor,
			DatabaseHost:        Parameters.DatabaseHost,
			DatabasePort:        Parameters.DatabasePort,
			DatabaseName:        Parameters.DatabaseName,
			DatabaseSSLMode:     Parameters.DatabaseSSLMode,
			DatabaseAutoMigrate: Parameters.DatabaseAutoMigrate,
			DatabaseUserName:    Parameters.DatabaseUserName,
			DatabasePassword:    Parameters.DatabasePassword,
			DatabaseSchema:      Parameters.DatabaseSchema,
		})
		if err != nil {
			return fmt.Errorf("failed to run pre run initializations: %w", err)
		}
		BL = newBL
	}
	return nil
}

func init() {
	RootCmd.PersistentFlags().StringVarP(&Parameters.GroupIdStr, "group-id", "G", "00000000-0000-0000-0000-000000000000", "group id provides isolation between workflows and steps")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseVendor, "db-vendor", "V", "", "supported database vendors: sqlite, postgresql, remote")
	RootCmd.PersistentFlags().StringVar(&Parameters.DataSourceName, "db-file-name", ":memory:", "sqlite file location")
	RootCmd.PersistentFlags().StringVarP(&Parameters.CfgFile, "config", "c", "", "config file (default is $HOME/.stepsman.yaml)")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseHost, "db-host", "H", "localhost", "database host address")
	RootCmd.PersistentFlags().Int64VarP(&Parameters.DatabasePort, "db-port", "P", 5432, "database port address")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseName, "db-name", "D", "stepsman", "database name")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseUserName, "db-user-name", "U", "stepsman", "database user name")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabasePassword, "db-password", "W", "stepsman", "database password")
	RootCmd.PersistentFlags().StringVarP(&Parameters.DatabaseSchema, "db-schema", "A", "", "database schema")
	RootCmd.PersistentFlags().BoolVarP(&Parameters.DatabaseSSLMode, "db-enable-ssl", "L", false, "database ssl on database port")
	RootCmd.PersistentFlags().BoolVarP(&Parameters.DatabaseAutoMigrate, "db-auto-migrate", "M", false, "database will auto migrate to match code version if it is lower")
}
