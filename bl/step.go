package bl

import (
	fmt "fmt"
	"github.com/jmoiron/sqlx"
	"github.com/yeqown/log"
	"sync"
	"time"
)

type StepStatusType int64
type DoneType int64

const HeartBeatInterval = 10

const (
	StepNotStarted StepStatusType = 0
	StepInProgress StepStatusType = 2
	StepCanceled   StepStatusType = 3
	StepFailed     StepStatusType = 4
	StepDone       StepStatusType = 5
	StepSkipped    StepStatusType = 6
)

type StepRecord struct {
	RunId     int64 `db:"run_id"`
	StepId    int64 `db:"step_id"`
	UUID      string
	Name      string
	Status    StepStatusType
	HeartBeat int64
	Script    string
}

func TranslateStepStatus(status StepStatusType) (string, error) {
	switch status {
	case StepNotStarted:
		return "Not Started", nil
	case StepInProgress:
		return "In Progress", nil
	case StepCanceled:
		return "Canceled", nil
	case StepFailed:
		return "Failed", nil
	case StepDone:
		return "Done", nil
	case StepSkipped:
		return "Skipped", nil
	default:
		return "", fmt.Errorf("failed to translate run status: %d", status)
	}
}
func ListSteps(runId int64) ([]*StepRecord, error) {
	return listSteps(nil, runId)
}
func listSteps(tx *sqlx.Tx, runId int64) ([]*StepRecord, error) {
	var result []*StepRecord
	var rows *sqlx.Rows
	var err error
	const query = "SELECT * FROM steps WHERE run_id=?"
	if tx == nil {
		rows, err = DB.Queryx(query, runId)
	} else {
		rows, err = tx.Queryx(query, runId)
	}
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

func getStep(tx *sqlx.Tx, runId int64, stepId int64) (*StepRecord, error) {
	var result *StepRecord

	const query = "SELECT * FROM steps where run_id=? and step_id=?"
	var rows *sqlx.Rows
	var err error
	if tx == nil {
		rows, err = DB.Queryx(query, runId, stepId)
	} else {
		rows, err = tx.Queryx(query, runId, stepId)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to query database steps table - get: %w", err)
	}

	for rows.Next() {
		var step StepRecord
		err = rows.StructScan(&step)
		if err != nil {
			return nil, fmt.Errorf("failed to parse database steps row - get: %w", err)
		}
		result = &step
	}
	if result == nil {
		return nil, ErrRecordNotFound
	}
	return result, nil
}
func (s *StepRecord) ToStep() (*Step, error) {
	step := Step{
		Script: s.Script,
	}
	err := step.AdjustUnmarshalStep(true)
	if err != nil {
		return nil, err
	}
	step.stepRecord = s
	return &step, nil
}
func (s *StepRecord) UpdateHeartBeat() error {
	_, err := DB.Exec("update steps set UpdateHeheartbeat=? where run_id=? and step_id=?", time.Now().Unix(), s.RunId, s.StepId)
	if err != nil {
		return fmt.Errorf("failed to update database step heartbeat: %w", err)
	}
	return nil
}
func (s *StepRecord) UpdateStatus(newStatus StepStatusType, doFinish bool) error {
	tx, err := DB.Beginx()
	if err != nil {
		return fmt.Errorf("failed to start a database transaction: %w", err)
	}
	step, err := getStep(tx, s.RunId, s.StepId)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to update database step row: %w", err)
	}

	if !doFinish && step.Status == StepInProgress {
		delta := time.Now().Sub(time.Unix(step.HeartBeat, 0))
		if delta < 0 {
			delta = delta * -1
		}
		// +5 for slow downs
		if delta <= (HeartBeatInterval+5)*time.Second {
			err = Rollback(tx, fmt.Errorf("step is already in progress and has a heartbeat with an interval of %d", HeartBeatInterval))
			return fmt.Errorf("failed to update database step row: %w", err)
		}
	}
	// don't change done if the status did not change.
	// must be after (because we want to raise StepInProgress=StepInProgress error
	if newStatus == s.Status {
		tx.Rollback()
		return nil
	}
	heartBeat := step.HeartBeat
	if newStatus == StepInProgress {
		if doFinish {
			heartBeat = 0
		} else {
			heartBeat = time.Now().Unix()
		}
	}
	_, err = tx.Exec("update steps set status=?, heartbeat=? where run_id=? and step_id=?", newStatus, heartBeat, s.RunId, s.StepId)
	if err != nil {
		err = Rollback(tx, err)
		return fmt.Errorf("failed to update database step row: %w", err)
	}

	if newStatus != StepNotStarted {
		var run *RunRecord
		run, err = GetRun(s.RunId)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to update database step row: %w", err)
		}
		if run.Status != RunInProgress {
			_, err = tx.Exec("update runs set status=? where run_id=?", RunInProgress, s.RunId)
			if err != nil {
				err = Rollback(tx, err)
				return fmt.Errorf("failed to update database run row: %w", err)
			}
		}
	}

	if newStatus == StepDone || newStatus == StepSkipped {
		var steps []*StepRecord
		steps, err = listSteps(tx, s.RunId)
		if err != nil {
			err = Rollback(tx, err)
			return fmt.Errorf("failed to list database run rows: %w", err)
		}
		allDoneOrSkipped := true
		for _, stepRecord := range steps {
			if stepRecord.Status != StepDone && stepRecord.Status != StepSkipped {
				allDoneOrSkipped = false
				break
			}
		}
		if allDoneOrSkipped{
			_, err = tx.Exec("update runs set status=? where run_id=?", RunDone, s.RunId)
			if err != nil {
				err = Rollback(tx, err)
				return fmt.Errorf("failed to update database run row: %w", err)
			}
		}
	}

	tx.Commit()
	s.Status = newStatus
	return nil
}
func (s *Step) StartDo() error {
	err := s.stepRecord.UpdateStatus(StepInProgress, false)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	heartBeatDone1 := make(chan int)
	heartBeatDone2 := make(chan int)
	heartbeat := func() {
	OUT:
		for {
			select {
			case <-heartBeatDone1:
				break OUT
			case <-heartBeatDone2:
				break OUT
			case <-time.After(HeartBeatInterval * time.Second):
				errBeat := s.stepRecord.UpdateHeartBeat()
				if errBeat != nil {
					log.Warn(fmt.Errorf("while trying to update heartbeat: %w", errBeat))
				}
			}
		}
		wg.Done()
	}
	wg.Add(1)
	go heartbeat()
	defer close(heartBeatDone1) //in case of a panic
	newStatus, err := do(s.DoType, s.Do, true)
	if err != nil {
		return err
	}
	close(heartBeatDone2)
	wg.Wait()
	err = s.stepRecord.UpdateStatus(newStatus, true)
	return err
}
