package bl

import (
	"errors"
	"fmt"
	"github.com/google/uuid"
)

var ErrActiveRunsWithSameTitleExists = errors.New("active runs with the same title detected")

type RunStatusType int64
type SummaryType int64

const (
	RunNotStarted RunStatusType = 10
	RunInProgress RunStatusType = 12
	RunStopped    RunStatusType = 13
)

const (
	SummaryNone                   = 20
	SummaryInProgress SummaryType = 21
	SummaryFailing    SummaryType = 22
	SummaryDone       SummaryType = 23
)

type RunRecord struct {
	Id      int64
	UUID    string
	Title   string
	Cursor  int64
	Status  RunStatusType
	Summary SummaryType
	Script  string
}

func TranslateSummary(status SummaryType) (string, error) {
	switch status {
	case SummaryNone:
		return "None", nil
	case SummaryInProgress:
		return "Steps In Progress", nil
	case SummaryFailing:
		return "Steps Failing", nil
	case SummaryDone:
		return "Done", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}

func TranslateRunStatus(status RunStatusType) (string, error) {
	switch status {
	case RunNotStarted:
		return "Not Started", nil
	case RunInProgress:
		return "In Progress", nil
	case RunStopped:
		return "Stopped", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}

func ListRuns() ([]*RunRecord, error) {
	var result []*RunRecord
	rows, err := DB.Queryx("SELECT * FROM runs where status=?", RunInProgress)
	if err != nil {
		return nil, fmt.Errorf("failed to query database runs table: %w", err)
	}

	for rows.Next() {
		var run RunRecord
		err = rows.StructScan(&run)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database runs row: %w", err)
		}
		result = append(result, &run)
	}
	return result, nil
}

func (r *RunRecord) Create(err error, s *Script, yamlBytes []byte) error {
	tx, err := DB.Beginx()
	if err != nil {
		return fmt.Errorf("failed to create database transaction: %w", err)
	}

	count := -1
	err = tx.Get(&count, "SELECT count(*) FROM runs where status=? and title=?", RunInProgress, s.Title)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to query database runs table count for in progress rows: %w", err)
	}
	if count != 0 {
		err = Rollback(tx, ErrActiveRunsWithSameTitleExists)
		return err
	}
	count = 0
	err = tx.Get(&count, "SELECT count(*) FROM runs limit 1")
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to query database runs table count: %w", err)
	}

	uuid4, err := uuid.NewRandom()
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to generate uuid: %w", err)
	}
	runRecord := RunRecord{
		UUID:   uuid4.String(),
		Title:  s.Title,
		Status: RunInProgress,
		Summary: SummaryNone,
		Script: string(yamlBytes),
	}

	exec, err := tx.NamedExec("INSERT INTO runs(uuid, title, status, summary, script) values(:uuid,:title,:status,:summary,:script)", &runRecord)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to insert database runs row: %w", err)
	}
	runRecord.Id, err = exec.LastInsertId()
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to retrieve database runs row autoincremented id: %w", err)
	}

	for i, step := range s.Steps {
		uuid4, err := uuid.NewRandom()
		stepRecord := StepRecord{
			RunId:   runRecord.Id,
			StepId:  int64(i) + 1,
			UUID:    uuid4.String(),
			Heading: step.Heading,
			Status:  StepNotStarted,
			Done:    DoneNotDone,
			Script:  step.Script,
		}
		_, err = tx.NamedExec("INSERT INTO steps(run_id, step_id, uuid, heading, status, done, script) values(:run_id,:step_id,:uuid,:heading,:status,:done,:script)", &stepRecord)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to insert database steps row: %w", err)
		}
	}

	*r = runRecord

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit database transaction: %w", err)
	}
	return nil
}
