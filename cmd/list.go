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
	"github.com/jedib0t/go-pretty/table"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"os"
)

// listCmd represents the list command
var listCmd = &cobra.Command{
	Use:   "list",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		MyStyle := table.Style{
			Name:    "StyleDefault",
			Box:     table.StyleBoxDefault,
			Color:   table.ColorOptionsDefault,
			Format:  table.FormatOptionsDefault,
			HTML:    table.DefaultHTMLOptions,
			Options: table.OptionsNoBordersAndSeparators,
			Title:   table.TitleOptionsDefault,
		}
		t := table.NewWriter()
		t.SetStyle(MyStyle)
		t.SetOutputMirror(os.Stdout)
		t.AppendHeader(table.Row{"#", "UUID", "Name", "Status"})
		runs, err := bl.ListRuns()
		if err!=nil{
			msg := "failed to list runs"
			fmt.Println(msg + SeeLogMsg)
			log.Fatal(fmt.Errorf(msg+": %w", err))
		}
		for _, run := range runs {
			status, err := bl.TranslateRunStatus(run.Status)
			if err!=nil{
				msg := "failed to list runs"
				fmt.Println(msg + SeeLogMsg)
				log.Fatal(fmt.Errorf(msg+": %w", err))
			}
			t.AppendRows([]table.Row{
				{run.Id, run.UUID, run.Name, status},
			})
		}
		//t.AppendSeparator()
		//t.AppendRow([]interface{}{300, "Tyrion", "Lannister", 5000})
		//t.AppendFooter(table.Row{"", "", "Total", 10000})
		t.Render()
	},
}

func init() {
	rootCmd.AddCommand(listCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// listCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// listCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
