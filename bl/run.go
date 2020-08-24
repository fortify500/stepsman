package bl

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

var ErrActiveRunsWithSameNameExists = errors.New("active runs with the same name detected")

type CheckedType int

const (
	NotDone CheckedType = 0
	Done    CheckedType = 1
)

type RunStatusType int

const (
	NotStarted RunStatusType = 0
	Pending    RunStatusType = 1 // for example, waiting for a label or a job (future) slot to finish.
	InProgress RunStatusType = 2
	Canceled   RunStatusType = 3
	Failed     RunStatusType = 4
	Success    RunStatusType = 5
)

func TranslateRunStatus(status RunStatusType) (string, error) {
	switch status {
	case NotStarted:
		return "Not Started", nil
	case InProgress:
		return "In Progress", nil
	case Canceled:
		return "Canceled", nil
	case Failed:
		return "Failed", nil
	case Success:
		return "Success", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}

type RunRow struct {
	Id       int64
	UUID     string
	Name     string
	// Cursor   int64 // points to the id of the current step.
	Status RunStatusType
	Script string
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

func (r *RunRow) Start(err error, s *Script, yamlBytes []byte) error {
	tx, err := DB.Beginx()
	if err != nil {
		return fmt.Errorf("failed to create database transaction: %w", err)
	}

	count := -1
	err = tx.Get(&count, "SELECT count(*) FROM runs where status=? and name=?", InProgress, s.Title)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to query database runs table count: %w", err)
	}
	if count != 0 {
		err = Rollback(tx, ErrActiveRunsWithSameNameExists)
		return err
	}
	count = 0
	err = tx.Get(&count, "SELECT count(*) FROM runs limit 1", InProgress, s.Title)
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
		UUID:   uuid.String(),
		Name:   s.Title,
		Status: InProgress,
		Script: string(yamlBytes),
	}

	exec, err := tx.NamedExec("INSERT INTO runs(uuid, name, status, script) values(:uuid,:name,:status,:script)", &runRow)
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
