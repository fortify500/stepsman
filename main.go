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
package main

import (
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/fortify500/stepsman/bl"
	"github.com/fortify500/stepsman/cmd"
	"github.com/gobs/args"
	"github.com/spf13/pflag"
	"github.com/yeqown/log"
	"os"
	"strings"
)

var untilSpace = false

func main() {
	if len(os.Args) > 1 {
		cmd.Execute()
	} else {
		fmt.Println("Usage:")
		fmt.Println("* `exit` or `Ctrl-D` to exit this program.")
		fmt.Println("* `Tab` to enable suggestions or `Esc` to stop suggesting.")
		fmt.Println("* Examples: \"help\", \"list runs\", \"list run 1\", \"do run 1\"")
		fmt.Println("* Note: `Enter` key will also execute from a suggestion so type normally after a selection to continue without execution.")
		for {
			s := prompt.Input(">>> ", completer, prompt.OptionTitle("stepsman: step by step managed script"),
				prompt.OptionPrefix(">>> "),
				prompt.OptionCompletionOnDown(),
				prompt.OptionShowCompletionAtStart(),
				//prompt.OptionInitialBufferText("list"),
				prompt.OptionInputTextColor(prompt.Yellow))
			executor(s)
		}
	}
}
func executor(s string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return
	} else if strings.EqualFold(s, "quit") || strings.EqualFold(s, "exit") {
		fmt.Println("So Long, and Thanks for All the Fish! (https://en.wikipedia.org/wiki/The_Hitchhiker%27s_Guide_to_the_Galaxy)")
		os.Exit(0)
	}
	prev := cmd.Parameters.InPromptMode
	cmd.Parameters.InPromptMode = true
	cmd.RootCmd.SetArgs(args.GetArgs(s))
	cmd.Execute()
	cmd.Parameters.InPromptMode = prev
}

func completer(d prompt.Document) []prompt.Suggest {
	if d.LastKeyStroke() == prompt.Escape {
		untilSpace = true
		return []prompt.Suggest{}

	}
	if untilSpace && d.LastKeyStroke() == prompt.Tab {
		untilSpace = false
	} else if untilSpace {
		return []prompt.Suggest{}
	}

	s := []prompt.Suggest{
	}
	currentWord := d.GetWordBeforeCursorUntilSeparator(" ")

	words := args.GetArgs(d.TextBeforeCursor())
	relevantCommand := cmd.RootCmd

OUT:
	for i, word1 := range words {
		for _, command := range cmd.RootCmd.Commands() {
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
	}
	for _, suggest := range s {
		if strings.EqualFold(suggest.Text, currentWord) {
			return []prompt.Suggest{}
		}
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}
