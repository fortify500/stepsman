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
	"github.com/fortify500/stepsman/cmd"
	"github.com/gobs/args"
	"os"
	"strings"
)

func main() {
	if len(os.Args) > 1 {
		cmd.Execute()
	} else {
		fmt.Println("Usage:")
		fmt.Println("* `exit` or `Ctrl-D` to exit this program.")
		fmt.Println("* `Tab` to enable suggestions or `Esc` to stop suggesting.")
		fmt.Println("* Examples: \"help\", \"list\", \"list run 1\", \"do run 1\"")
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

var untilSpace = false

func completer(d prompt.Document) []prompt.Suggest {
	if d.LastKeyStroke() == prompt.Escape {
		untilSpace = true
		return prompt.FilterHasPrefix([]prompt.Suggest{}, d.GetWordBeforeCursor(), true)

	}
	if untilSpace && d.LastKeyStroke() == prompt.Tab {
		untilSpace = false
	} else if untilSpace {
		return prompt.FilterHasPrefix([]prompt.Suggest{}, d.GetWordBeforeCursor(), true)
	}

	s := []prompt.Suggest{
		{Text: "list", Description: "Store the username and age"},
		{Text: "help", Description: "Store the article text posted by user"},
		{Text: "comments", Description: "Store the text commented to articles"},
	}
	return prompt.FilterHasPrefix(s, d.GetWordBeforeCursor(), true)
}
