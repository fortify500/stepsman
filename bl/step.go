package bl

import (
	"gopkg.in/yaml.v2"
	"strings"
)

type Step struct {
	Heading  string
	Content  string
	Expected string
	Run      interface{}
	runType  string
}

type StepRun struct {
	Type string
}

type StepRunShellExecute struct {
	Command   string
	Arguments []string
}

func (step *Step) AdjustUnmarshalOptions() error {
	if step.Run != nil {
		stepRun := StepRun{}
		stepRunBytes, err := yaml.Marshal(step.Run)
		if err != nil {
			return err
		}
		err = yaml.Unmarshal(stepRunBytes, &stepRun)
		if err != nil {
			return err
		}
		runType := strings.ToLower(stepRun.Type)
		switch runType {
		case "":
			fallthrough
		case "shell execute":
			run := StepRunShellExecute{}
			err = yaml.Unmarshal(stepRunBytes, &run)
			if err != nil {
				return err
			}
			step.Run = run
		}
	}
	return nil
}
