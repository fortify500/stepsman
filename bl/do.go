package bl

import (
	"context"
	"errors"
	"fmt"
	"github.com/yeqown/log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
)

var ErrNoRunsDirectory = fmt.Errorf("no runs directory detected and make directory flag is false")

func do(doType DoType, doI interface{}, mkdir bool) (StepStatusType, error) {
	result := StepDone
	//_, err := os.Stat("runs")
	//if os.IsNotExist(err) {
	//	if !mkdir {
	//		return ErrNoRunsDirectory
	//	}
	//	err = os.MkdirAll("runs", 0700)
	//	if err != nil {
	//		return fmt.Errorf("failed to create the runs diretory: %w", err)
	//	}
	//} else if err != nil {
	//	return fmt.Errorf("failed to determine existance of runs directory: %w", err)
	//}
	if doI != nil {
		switch doType {
		case DoTypeShellExecute:
			do := doI.(StepDoShellExecute)
			termination := make(chan os.Signal, 1)
			signal.Notify(termination, os.Interrupt)
			signal.Notify(termination, syscall.SIGTERM)
			defer signal.Stop(termination)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cmd := exec.CommandContext(ctx, do.Command, do.Arguments...)
			cmd.Env = os.Environ()
			// if we want that the terminal will not send to the group and close both us and the child
			//if cmd.SysProcAttr != nil {
			//	cmd.SysProcAttr.Setpgid = true
			//	cmd.SysProcAttr.Pgid = 0
			//} else {
			//	cmd.SysProcAttr = &syscall.SysProcAttr{
			//		Setsid: true,
			//	}
			//}
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			var wg sync.WaitGroup
			go func() {
				defer wg.Done()
				defer cancel()
				select {
				case <-ctx.Done():
					break
				case <-termination:
					result = StepCanceled
					break
				}
			}()
			wg.Add(1)
			errRun := cmd.Run()
			cancel()
			wg.Wait()
			if errRun != nil {
				var exitError *exec.ExitError
				if errors.As(errRun, &exitError) && exitError.ExitCode() > 0 {
					log.Debug(fmt.Sprintf("Exit error code: %d", exitError.ExitCode()))
					if result == StepDone {
						result = StepFailed
					}
				}
				log.Debug(fmt.Errorf("command failed: %w", errRun))
				if result == StepDone {
					result = StepFailed
				}
			} else {
				log.Debug(fmt.Sprintf("Exit error code: %d", 0))
			}
		}
	}

	return result, nil
}
