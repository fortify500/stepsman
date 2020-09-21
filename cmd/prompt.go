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
	"github.com/c-bata/go-prompt"
	"github.com/fortify500/stepsman/bl"
	"github.com/gobs/args"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"os"
	"strings"
)

const prefix = "[stepsman]: "

var promptCmd = &cobra.Command{
	Use:   "prompt",
	Short: "Prompt is an interactive command interface",
	Long:  `Use prompt to interact with the command with command suggestions.`,
	Run: func(cmd *cobra.Command, args []string) {
		resetParameters()
		var history []string
		fmt.Println("Usage:")
		fmt.Println("* `exit` or CTRL-D to exit this program.")
		fmt.Println("* `Tab` to enable suggestions or `Esc` to stop suggesting.")
		fmt.Println("* Examples: \"help\", \"list runs\", \"list run 1\", \"do run 1\", \"create run -f examples/basic.yaml\"")
		fmt.Println("* Note: `Enter` key will also execute from a suggestion so type normally after a selection to continue without execution.")
		for {
			wasEnter := false
			s := prompt.Input(prefix, completer, prompt.OptionTitle("stepsman: step by step managed script"),
				prompt.OptionPrefix(prefix),
				prompt.OptionCompletionOnDown(),
				prompt.OptionShowCompletionAtStart(),
				prompt.OptionInitialBufferText(Parameters.InitialInput),
				prompt.OptionInputTextColor(prompt.Yellow),
				prompt.OptionHistory(history),
				prompt.OptionAddKeyBind(prompt.KeyBind{
					Key: prompt.ControlM,
					Fn: func(buffer *prompt.Buffer) {
						wasEnter = true
					},
				}),
			)
			if strings.TrimSpace(s) != "" {
				history = append(history, s)
			}
			executor(s, wasEnter)
		}
	},
}

func init() {
	RootCmd.AddCommand(promptCmd)
}

func executor(s string, wasEnter bool) {
	s = strings.TrimSpace(s)
	if s == "" && wasEnter {
		return
	} else if strings.EqualFold(s, "quit") || strings.EqualFold(s, "exit") || (s == "" && !wasEnter) {
		fmt.Println("So Long, and Thanks for All the Fish!")
		os.Exit(0)
	}
	RootCmd.SetArgs(args.GetArgs(s))
	wasError := Execute()
	var describeRunCursorStep []string
	currentRunIdStr := fmt.Sprintf("%d", Parameters.CurrentRunId)
	if Parameters.CurrentRunId > 0 {
		describeRunCursorStep = []string{"describe", "run", currentRunIdStr, "--step"}
	}
	runStatus := bl.RunInProgress
	if Parameters.CurrentRun != nil {
		runStatus = Parameters.CurrentRun.Status
	}
	currentCommand := Parameters.CurrentCommand
	nextInitialInput := ""
	listRunsRunId := []string{"list", "runs", "--run", currentRunIdStr}
	resetParameters()
	switch currentCommand {
	case CommandCreateRun:
		if !wasError {
			fmt.Println(fmt.Sprintf("%s%s", prefix, strings.Join(listRunsRunId, " ")))
			RootCmd.SetArgs(listRunsRunId)
			Execute()
			resetParameters()
			fmt.Println(fmt.Sprintf("%s%s", prefix, strings.Join(describeRunCursorStep, " ")))
			RootCmd.SetArgs(describeRunCursorStep)
			Execute()
		}
	case CommandDoRun:
		if !wasError {
			fmt.Println(fmt.Sprintf("%s%s", prefix, strings.Join(describeRunCursorStep, " ")))
			RootCmd.SetArgs(describeRunCursorStep)
			Execute()
			if runStatus == bl.RunDone {
				resetParameters()
				fmt.Println(fmt.Sprintf("%s%s", prefix, strings.Join(listRunsRunId, " ")))
				RootCmd.SetArgs(listRunsRunId)
				Execute()
			} else {
				nextInitialInput = s
			}
		} else {
			if runStatus == bl.RunDone {
				nextInitialInput = strings.Join(listRunsRunId, " ")
			} else {
				nextInitialInput = strings.Join(describeRunCursorStep, " ")
			}
		}
	case CommandSkipRun:
		if !wasError {
			fmt.Println(fmt.Sprintf("%s%s", prefix, strings.Join(describeRunCursorStep, " ")))
			RootCmd.SetArgs(describeRunCursorStep)
			Execute()
			if runStatus == bl.RunDone {
				resetParameters()
				fmt.Println(fmt.Sprintf("%s%s", prefix, strings.Join(listRunsRunId, " ")))
				RootCmd.SetArgs(listRunsRunId)
				Execute()
			} else {
				nextInitialInput = s
			}
		} else {
			if runStatus == bl.RunDone {
				nextInitialInput = strings.Join(listRunsRunId, " ")
			} else {
				nextInitialInput = strings.Join(describeRunCursorStep, " ")
			}
		}
	case CommandStopRun:
		if !wasError {
			fmt.Println(fmt.Sprintf("%s%s", prefix, strings.Join(listRunsRunId, " ")))
			RootCmd.SetArgs(listRunsRunId)
			Execute()
		}
	default:
		nextInitialInput = ""
	}
	resetParameters()
	Parameters.InitialInput = nextInitialInput
}

func resetParameters() {
	Parameters.CurrentCommand = CommandUndetermined
	Parameters.CurrentRunId = -1
	Parameters.Err = nil
	for _, flagsReInit := range Parameters.FlagsReInit {
		flagsReInit()
	}
}

func completer(d prompt.Document) []prompt.Suggest {
	if d.LastKeyStroke() == prompt.Escape {
		Parameters.DisableSuggestions = true
		return []prompt.Suggest{}

	}
	if Parameters.DisableSuggestions && d.LastKeyStroke() == prompt.Tab {
		Parameters.DisableSuggestions = false
	} else if Parameters.DisableSuggestions {
		return []prompt.Suggest{}
	}

	var s []prompt.Suggest
	currentWord := d.GetWordBeforeCursorUntilSeparator(" ")

	words := args.GetArgs(d.TextBeforeCursor())
	relevantCommand := RootCmd

	//Determine the existence of the potential command and subcommands
OUT:
	for i, word1 := range words {
		for _, command := range RootCmd.Commands() {
			if strings.EqualFold(command.Use, word1) {
				relevantCommand = command
				for _, word2 := range words[i:] {
					for _, subCommand := range command.Commands() {
						if strings.EqualFold(subCommand.Use, word2) {
							relevantCommand = subCommand
							break OUT
						}
					}
				}
				break OUT
			}
		}
	}

	// get the previous word before the space. we want to know if it is run because we want to
	// pull out the suggestions of possible run ids.
	runWord := d.GetWordBeforeCursorUntilSeparatorIgnoreNextToCursor(" ")
	if strings.EqualFold(relevantCommand.Use, "run") &&
		len(runWord) > len("run") &&
		strings.EqualFold(strings.TrimSpace(runWord), "run") {
		runs, err := bl.ListRuns()
		if err != nil {
			log.Error(fmt.Errorf("failed to list runs: %w", err))
			return []prompt.Suggest{}
		}
		for _, run := range runs {
			s = append(s, prompt.Suggest{
				Text:        fmt.Sprintf("%d", run.Id),
				Description: run.Title,
			})
		}
		return s
	} else {
		for _, command := range relevantCommand.Commands() {
			s = append(s, prompt.Suggest{
				Text:        command.Use,
				Description: command.Short,
			})
		}
		relevantCommand.LocalFlags().VisitAll(func(flag *pflag.Flag) {
			s = append(s, prompt.Suggest{
				Text:        "--" + flag.Name,
				Description: flag.Usage,
			})
		})
		s = append(s, prompt.Suggest{
			Text:        "--help",
			Description: "help for stepsman or a command",
		})
	}
	for _, suggest := range s {
		if strings.EqualFold(suggest.Text, currentWord) {
			return []prompt.Suggest{}
		}
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}
