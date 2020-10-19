/*
 * Copyright © 2020 stepsman authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bl

import (
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

//var ErrNoRunsDirectory = fmt.Errorf("no runs directory detected and make directory flag is false")

func do(doType DoType, doI interface{}) error {
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
		case DoTypeREST:
			do := doI.(StepDoREST)
			var timeout int64 = 60
			if do.Timeout > 0 {
				timeout = do.Timeout
			}
			var maxResponseHeaderBytes int64 = 1024 * 1024 * 10
			if do.Options.MaxResponseHeaderBytes > 0 {
				maxResponseHeaderBytes = do.Options.MaxResponseHeaderBytes
			}
			var body io.ReadCloser = nil
			if len(do.Options.Body) > 0 {
				body = ioutil.NopCloser(strings.NewReader(do.Options.Body))
			}
			var netTransport = &http.Transport{
				Dial: (&net.Dialer{
					Timeout: time.Duration(timeout) * time.Second,
				}).Dial,
				TLSHandshakeTimeout:    time.Duration(timeout) * time.Second,
				ResponseHeaderTimeout:  time.Duration(timeout) * time.Second,
				MaxResponseHeaderBytes: maxResponseHeaderBytes,
			}
			var netClient = &http.Client{
				Transport: netTransport,
				Timeout:   time.Second * time.Duration(timeout),
			}
			url, err := url.Parse(do.Options.Url)
			if err != nil {
				return err
			}
			request := &http.Request{
				Method: do.Options.Method,
				URL:    url,
				Header: do.Options.Headers,
				Body:   body,
			}
			_, err = netClient.Do(request)
			if err != nil {
				return err
			}
			//termination := make(chan os.Signal, 1)
			//signal.Notify(termination, os.Interrupt)
			//signal.Notify(termination, syscall.SIGTERM)
			//defer signal.Stop(termination)
			//ctx, cancel := context.WithCancel(context.Background())
			//defer cancel()
			//cmd := exec.CommandContext(ctx, do.Options.Command, do.Options.Arguments...)
			//cmd.Env = os.Environ()
			// if we want that the terminal will not send to the group and close both us and the child
			//if cmd.SysProcAttr != nil {
			//	cmd.SysProcAttr.Setpgid = true
			//	cmd.SysProcAttr.Pgid = 0
			//} else {
			//	cmd.SysProcAttr = &syscall.SysProcAttr{
			//		Setsid: true,
			//	}
			//}
			//	cmd.Stdin = os.Stdin
			//	cmd.Stdout = os.Stdout
			//	cmd.Stderr = os.Stderr
			//	var wg sync.WaitGroup
			//	go func() {
			//		defer wg.Done()
			//		defer cancel()
			//		select {
			//		case <-ctx.Done():
			//			break
			//		case <-termination:
			//			result = dao.StepDone
			//			break
			//		}
			//	}()
			//	wg.Add(1)
			//	errRun := cmd.Run()
			//	cancel()
			//	wg.Wait()
			//	if errRun != nil {
			//		var exitError *exec.ExitError
			//		if errors.As(errRun, &exitError) && exitError.ExitCode() > 0 {
			//			log.Debug(fmt.Sprintf("Exit error code: %d", exitError.ExitCode()))
			//			//TODO: we need to fill the state here when there will be state handling
			//			if result == dao.StepDone {
			//				result = dao.StepDone
			//			}
			//		} else if result == dao.StepDone {
			//			result = dao.StepDone
			//		}
			//		log.Debug(fmt.Errorf("command failed: %w", errRun))
			//
			//	} else {
			//		log.Debug(fmt.Sprintf("Exit error code: %d", 0))
			//	}
		}
	}

	return nil
}
