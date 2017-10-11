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
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/cadence"
)

type (
	workflowTestSuite struct {
		suite.Suite
		cadence.WorkflowTestSuite
		env *cadence.TestWorkflowEnvironment
	}
)

func TestWorkflowTestSuite(t *testing.T) {
	suite.Run(t, new(workflowTestSuite))
}

func (s *workflowTestSuite) SetupTest() {
	s.env = s.NewTestWorkflowEnvironment()
}

func (s *workflowTestSuite) TearDownTest() {
	s.env.AssertExpectations(s.T())
}

func (s *workflowTestSuite) TestHighFreqTimerWF() {
	s.env.OnActivity(highFreqTimerActivity, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Times(12)
	oldCronFreq := schedFreq
	schedFreq = time.Second
	s.env.ExecuteWorkflow(highFreqTimerWorkflow, time.Now().UnixNano(), time.Second, HTTPCallback{})
	schedFreq = oldCronFreq
	s.True(s.env.IsWorkflowCompleted())
	err := s.env.GetWorkflowError()
	_, ok := err.(*cadence.ContinueAsNewError)
	s.True(ok)
}

func (s *workflowTestSuite) TestHTTPCallback() {
	listener, err := s.startHTTPServer()
	s.NoError(err)
	defer listener.Close()
	callback := HTTPCallback{
		URL: "/jobs/create?name=foo-bar",
		Headers: map[string]string{
			"auth-token": uuid.New(),
			"host":       "localhost",
		},
	}
	portStr := strings.Split(listener.Addr().String(), ":")[1]
	port, err := strconv.Atoi(portStr)
	s.NoError(err)
	err = callback.invoke(port)
	s.NoError(err)
}

func (s *workflowTestSuite) startHTTPServer() (net.Listener, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	RegisterHandlers(tally.NoopScope)
	go func() { http.Serve(listener, nil) }()
	return listener, nil
}
