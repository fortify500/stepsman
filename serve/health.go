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

package serve

import (
	"encoding/json"
	"fmt"
	"github.com/InVisionApp/go-health/v2"
	log "github.com/sirupsen/logrus"
	"net/http"
)

type jsonStatus struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

func CustomJSONHandlerFunc(h health.IHealth, custom map[string]interface{}) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		states, failed, err := h.State()
		if err != nil {
			writeJSONStatus(rw, "error", fmt.Sprintf("unable to fetch states: %v", err), http.StatusOK)
			return
		}

		msg := "ok"
		statusCode := http.StatusOK

		if len(states) == 0 {
			writeJSONStatus(rw, msg, "health check not yet available", statusCode)
			return
		}

		if failed {
			statusCode = http.StatusServiceUnavailable
			msg = "failed"
		}

		fullBody := map[string]interface{}{
			"status":  msg,
			"details": states,
		}

		for k, v := range custom {
			if k != "status" && k != "details" {
				fullBody[k] = v
			}
		}

		data, err := json.Marshal(fullBody)
		if err != nil {
			writeJSONStatus(rw, "DOWN", fmt.Sprintf("failed to marshal state data: %v", err), http.StatusOK)
			return
		}

		writeJSONResponse(rw, statusCode, data)
	}
}

func writeJSONStatus(rw http.ResponseWriter, status, message string, statusCode int) {
	jsonData, _ := json.Marshal(&jsonStatus{
		Message: message,
		Status:  status,
	})

	writeJSONResponse(rw, statusCode, jsonData)
}

func writeJSONResponse(rw http.ResponseWriter, statusCode int, content []byte) {
	rw.Header().Set("Content-Type", "application/json")
	rw.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	rw.WriteHeader(statusCode)
	_, err := rw.Write(content)
	if err != nil {
		log.Errorf("failed to write json response: %w", err)
	}
}
