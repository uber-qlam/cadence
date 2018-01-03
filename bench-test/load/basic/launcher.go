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

package basic

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/bench-test/lib"
	"github.com/uber/cadence/bench-test/load/common"

	"go.uber.org/cadence/client"
	"go.uber.org/zap"

	"github.com/pborman/uuid"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"
)

type (
	launcher struct {
		config       *lib.BasicTestConfig
		numTasklist  int
		successCount int64
		failedCount  int64
		client       *lib.CadenceClient
		metrics      tally.Scope
		logger       *zap.Logger
		startWG      sync.WaitGroup
		stopC        chan struct{}
	}
)

func NewLoadGenerator(
	basicTestconfig *lib.BasicTestConfig,
	workerConfig *lib.WorkerConfig,
	client *lib.CadenceClient,
	metrics tally.Scope,
	logger *zap.Logger) lib.LoadGenerator {

	return &launcher{
		config:      basicTestconfig,
		numTasklist: workerConfig.NumTaskLists,
		client:      client,
		metrics:     metrics,
		logger:      logger,
		stopC:       make(chan struct{}),
	}
}

func (l *launcher) Start() error {
	if l.config.RoutineCount <= 0 {
		return errors.New("Invalid routine count.")
	}

	l.logger.Sugar().Infof("Starting Basic Test.  NumTasklist: %v, TotalLaunchCount: %v, RoutineCount: %v, ChainSequence: %v, ConcurrentCount: %v, PayloadSizeBytes: %v",
		l.numTasklist, l.config.TotalLaunchCount, l.config.RoutineCount, l.config.ChainSequence, l.config.ConcurrentCount, l.config.PayloadSizeBytes)
	go l.launch()

	return nil
}

func (l *launcher) Stop() {
	if !l.quit() {
		close(l.stopC)
	}
}

func (g *launcher) quit() bool {
	select {
	case <-g.stopC:
		return true
	default:
		return false
	}
}

func (l *launcher) launch() {
	workflowsPerStarter := l.config.TotalLaunchCount / l.config.RoutineCount
	for i := 0; i < l.config.RoutineCount; i++ {
		l.startWG.Add(1)
		go l.workerLoop(i, workflowsPerStarter)

		jitter := time.Duration(rand.Intn(2))
		time.Sleep(jitter * time.Millisecond)
	}

	l.startWG.Wait()
	l.logger.Sugar().Infof("Launch Complete.  SuccessCount: %v, FailedCount: %v", l.successCount, l.failedCount)
}

func (l *launcher) workerLoop(routineId int, count int) {
	defer l.startWG.Done()

	started := 0
	input := WorkflowParams{
		ChainSequence:    l.config.ChainSequence,
		ConcurrentCount:  l.config.ConcurrentCount,
		PayloadSizeBytes: l.config.PayloadSizeBytes,
	}
	workflowOptions := client.StartWorkflowOptions{
		ExecutionStartToCloseTimeout:    5 * time.Minute,
		DecisionTaskStartToCloseTimeout: 10 * time.Second,
	}

startLoop:
	for ; started < count; started++ {
		if l.quit() {
			break startLoop
		}

		input.TaskListNumber = rand.Intn(l.numTasklist)

		workflowOptions.ID = fmt.Sprintf("%s-%d-%d", uuid.New(), routineId, started)
		workflowOptions.TaskList = common.GetTaskListName(input.TaskListNumber)

		we, err := l.client.StartWorkflow(context.Background(), workflowOptions, stressWorkflowExecute, input)
		if err == nil {
			atomic.AddInt64(&l.successCount, 1)
			l.logger.Sugar().Debugf("Created Workflow - workflow Id: %s, run Id: %s \n", we.ID, we.RunID)
		} else {
			atomic.AddInt64(&l.failedCount, 1)
			l.logger.Sugar().Errorf(
				"Failed to start workflow execution. ID: %v, Error: %v", workflowOptions.ID, err,
			)
		}
		jitter := time.Duration(75 + rand.Intn(25))
		time.Sleep(jitter * time.Millisecond)
	}
}
