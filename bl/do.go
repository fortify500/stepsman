/*
Copyright © 2020 stepsman authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bl

import (
	"context"
	"errors"
	"fmt"
	"github.com/fortify500/stepsman/dao"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
)

//var ErrNoRunsDirectory = fmt.Errorf("no runs directory detected and make directory flag is false")

func do(doType DoType, doI interface{}) (dao.StepStatusType, error) {
	result := StepDone
	//_, err := os.Stat("runs")
	//if os.IsNotExist(err) {
	//	if !mkdir {
	//		return ErrNoRunsDirectory
	//	}
	//	err = os.MkdirAll("runs", 0700)
	//	if err != nil {
	//		return fmt.Errorf("failed to create the runs directory: %w", err)
	//	}
	//} else if err != nil {
	//	return fmt.Errorf("failed to determine existence of runs directory: %w", err)
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
			cmd := exec.CommandContext(ctx, do.Options.Command, do.Options.Arguments...)
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
					result = dao.StepCanceled
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
						result = StepDone // We don't want to fail like this
					}
				} else if result == StepDone {
					result = dao.StepFailed
				}
				log.Debug(fmt.Errorf("command failed: %w", errRun))

			} else {
				log.Debug(fmt.Sprintf("Exit error code: %d", 0))
			}
		}
	}

	return result, nil
}
