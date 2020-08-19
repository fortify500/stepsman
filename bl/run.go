package bl

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Template struct {
	Name  string
	Steps []Step
}

type RunStatusType int

const (
	NotStarted RunStatusType = 0
	InProgress RunStatusType = 1
	Stopped    RunStatusType = 2
)

var ErrActiveRunsWithSameNameExists = errors.New("active runs with the same name detected")

func (t *Template) LoadFromFile(filename string) ([]byte, error) {
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	err = t.LoadFromBytes(err, yamlFile)
	if err != nil {
		return nil, err
	}
	return yamlFile, nil
}

func (t *Template) LoadFromBytes(err error, yamlFile []byte) error {
	err = yaml.Unmarshal(yamlFile, t)
	if err != nil {
		return err
	}
	for i, _ := range t.Steps {
		(&t.Steps[i]).AdjustUnmarshalOptions()
	}
	return nil
}

func (t *Template) Start(fileName string) error {
	yamlBytes, err := t.LoadFromFile(fileName)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", fileName, err)
	}

	tx, err := DB.Beginx()
	if err != nil {
		return fmt.Errorf("failed to create database transaction: %w", err)
	}

	{
		count := -1
		err = tx.Get(&count, "SELECT count(*) FROM runs where status=? and name=?", InProgress, t.Name)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to query database runs table count: %w", err)
		}
		if count != 0 {
			err = Rollback(tx, ErrActiveRunsWithSameNameExists)
			return err
		}
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to generate uuid: %w", err)
	}
	runRow := RunRow{
		UUID:     uuid.String(),
		Name:     t.Name,
		Status:   int(InProgress),
		Template: string(yamlBytes),
	}
	_, err = tx.NamedExec("INSERT INTO runs(uuid, name, status, template) values(:uuid,:name,:status,:template)", &runRow)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to insert database runs row: %w", err)
	}
	//rows, err := tx.Queryx("SELECT * FROM runs where status=? and name=?", InProgress, t.Name)
	//if err != nil {
	//	err2 := tx.Rollback()
	//	if err2 != nil {
	//		err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	//	}
	//	return fmt.Errorf("failed to query database runs table: %w", err)
	//}
	//
	//for rows.Next() {
	//	var run RunRow
	//	err = rows.StructScan(&run)
	//	if err != nil {
	//		err2 := tx.Rollback()
	//		if err2 != nil {
	//			err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	//		}
	//		return fmt.Errorf("failed to parse database runs row: %w", err)
	//	}
	//}
	//if err != nil {
	//	err2 := tx.Rollback()
	//	if err2 != nil {
	//		err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	//	}
	//}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
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
	return nil
}

func Rollback(tx *sqlx.Tx, err error) error {
	err2 := tx.Rollback()
	if err2 != nil {
		err = fmt.Errorf("failed to Rollback transaction: %s after %w", err2.Error(), err)
	}
	return err
}
