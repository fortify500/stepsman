package bl

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

var ErrActiveRunsWithSameNameExists = errors.New("active runs with the same name detected")

const (
	NotStarted RunStatusType = 0
	InProgress RunStatusType = 1
	Stopped    RunStatusType = 2
)

func TranslateRunStatus(status RunStatusType) (string, error){
	switch status {
	case NotStarted:
		return "Not Started", nil
	case InProgress:
		return "In Progress", nil
	case Stopped:
		return "Stopped", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}

type RunStatusType int

type RunRow struct {
	Id       int64
	UUID     string
	Name     string
	Status   RunStatusType
	Template string
}

func ListRuns() ([]*RunRow, error) {
	var result []*RunRow
	rows, err := DB.Queryx("SELECT * FROM runs where status=?", InProgress)
	if err != nil {
		return nil, fmt.Errorf("failed to query database runs table: %w", err)
	}

	for rows.Next() {
		var run RunRow
		err = rows.StructScan(&run)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database runs row: %w", err)
		}
		result = append(result, &run)
	}
	return result, nil
}

func (r *RunRow) Start(err error, t *Template, yamlBytes []byte) error {
	tx, err := DB.Beginx()
	if err != nil {
		return fmt.Errorf("failed to create database transaction: %w", err)
	}

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
	count = 0
	err = tx.Get(&count, "SELECT count(*) FROM runs limit 1", InProgress, t.Name)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to query database runs table count: %w", err)
	}

	uuid, err := uuid.NewRandom()
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to generate uuid: %w", err)
	}
	runRow := RunRow{
		UUID:     uuid.String(),
		Name:     t.Name,
		Status:   InProgress,
		Template: string(yamlBytes),
	}

	exec, err := tx.NamedExec("INSERT INTO runs(uuid, name, status, template) values(:uuid,:name,:status,:template)", &runRow)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to insert database runs row: %w", err)
	}
	runRow.Id, err = exec.LastInsertId()
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to retrieve database runs row autoincremented id: %w", err)
	}
	*r = runRow

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
	return nil
}
