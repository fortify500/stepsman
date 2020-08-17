package bl

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

type Run struct {
	Name  string
	Steps []Step
}

func (r *Run) LoadFromFile(filename string) error {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(yamlFile, r)
	if err != nil {
		return err
	}
	return nil
}

func (r *Run) Start(fileName string) error {
	err := r.LoadFromFile(fileName)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", fileName, err)
	}
	_, err = os.Stat("runs")
	if os.IsNotExist(err) {
		err = os.MkdirAll("runs", 0700)
		if err != nil {
			fmt.Println()
			return fmt.Errorf("failed to create the runs diretory: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to determine existance of runs directory: %w", err)
	}
	return nil
}
