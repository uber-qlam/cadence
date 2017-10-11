// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package chronos

import (
	"fmt"
	"net/http"

	"time"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
)

type HTTPCallback struct {
	URL     string
	Headers map[string]string
}

type httpHandler struct {
	metricScope tally.Scope
}

func RegisterHandlers(scope tally.Scope) {
	handler := &httpHandler{scope}
	http.HandleFunc("/jobs/create", handler.createHandler)
}

func (h *httpHandler) createHandler(w http.ResponseWriter, r *http.Request) {
	h.metricScope.Counter("callback.received").Inc(1)
	name := r.URL.Query().Get("name")
	token := r.Header.Get("auth-token")
	host := r.Header.Get("host")

	if len(token) == 0 {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}

	w.Header().Add("name", name)
	w.Header().Add("job-id", uuid.New())
	w.Header().Add("host-id", "localhost")
	w.Header().Add("caller", host)
	fmt.Fprintf(w, "Success")
}

func (cb *HTTPCallback) invoke(port int) error {
	client := &http.Client{Timeout: time.Minute}
	url := fmt.Sprintf("http://127.0.0.1:%v%s", port, cb.URL)
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	for k, v := range cb.Headers {
		req.Header.Add(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
