package bl

import "gopkg.in/yaml.v2"

type Step struct {
	Description string
	Type        string
	Method      string
	Options     interface{}
}

type ShellExecuteOptions struct {
	Cmd       string
	Arguments []string
}

func (step *Step) AdjustUnmarshalOptions() error {
	if step.Method != "" {
		options := ShellExecuteOptions{}
		optionsBytes, err := yaml.Marshal(step.Options)
		if err != nil {
			return err
		}
		switch step.Method {
		case "shell_execute":
			err = yaml.Unmarshal(optionsBytes, &options)
			if err != nil {
				return err
			}
			step.Options = options
		}
	}
	return nil
}
