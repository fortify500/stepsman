package bl

import fmt "fmt"

type StepStatusType int64
type DoneType int64

const (
	StepNotStarted StepStatusType = 0
	StepPending    StepStatusType = 1 // for example, waiting for a label or a job (future) slot to finish.
	StepInProgress StepStatusType = 2
	StepCanceled   StepStatusType = 3
	StepFailed     StepStatusType = 4
	StepSuccess    StepStatusType = 5
	StepSkipped    StepStatusType = 6
)

const (
	DoneNotDone DoneType = 0
	DoneDone    DoneType = 1
)

type StepRecord struct {
	RunId   int64 `db:"run_id"`
	StepId  int64 `db:"step_id"`
	UUID    string
	Heading string
	Status  StepStatusType
	Done    DoneType
	Script  string
}

func TranslateStepStatus(status StepStatusType) (string, error) {
	switch status {
	case StepNotStarted:
		return "Not Started", nil
	case StepPending:
		return "Pending", nil
	case StepInProgress:
		return "In Progress", nil
	case StepCanceled:
		return "Canceled", nil
	case StepFailed:
		return "Failed", nil
	case StepSuccess:
		return "Success", nil
	case StepSkipped:
		return "Skipped", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}
func ListSteps(runId int64) ([]*StepRecord, error) {
	var result []*StepRecord
	rows, err := DB.Queryx("SELECT * FROM steps WHERE run_id=?", runId)
	if err != nil {
		return nil, fmt.Errorf("failed to query database steps table: %w", err)
	}

	for rows.Next() {
		var step StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row: %w", err)
		}
		result = append(result, &step)
	}
	return result, nil
}
