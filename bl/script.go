package bl

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Script struct {
	Title  string
	Steps []Step
}

func (s *Script) LoadFromFile(filename string) ([]byte, error) {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	err = s.LoadFromBytes(err, yamlFile)
	if err != nil {
		return nil, err
	}
	return yamlFile, nil
}

func (s *Script) LoadFromBytes(err error, yamlFile []byte) error {
	err = yaml.Unmarshal(yamlFile, s)
	if err != nil {
		return err
	}
	for i, _ := range s.Steps {
		(&s.Steps[i]).AdjustUnmarshalOptions()
	}
	return nil
}

func (s *Script) Start(fileName string) (*RunRow, error) {
	yamlBytes, err := s.LoadFromFile(fileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", fileName, err)
	}

	runRow := RunRow{}
	err = runRow.Start(err, s, yamlBytes)

	// we'll store the external logs for shell_execute
	//_, err = os.Stat("runs")
	//if os.IsNotExist(err) {
	//	err = os.MkdirAll("runs", 0700)
	//	if err != nil {
	//		return fmt.Errorf("failed to create the runs diretory: %w", err)
	//	}
	//} else if err != nil {
	//	return fmt.Errorf("failed to determine existance of runs directory: %w", err)
	//}
	return &runRow, err
}

func Rollback(tx *sqlx.Tx, err error) error {
	err2 := tx.Rollback()
	if err2 != nil {
		err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	}
	return err
}
