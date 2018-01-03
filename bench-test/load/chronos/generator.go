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
	"context"
	"math/rand"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/bench-test/lib"
	"github.com/uber/cadence/bench-test/load/common"
	"go.uber.org/cadence/client"
	"go.uber.org/zap"
)

type (
	loadGenerator struct {
		config         *lib.LoadTestConfig
		numTasklist    int
		client         *lib.CadenceClient
		metrics        tally.Scope
		logger         *zap.Logger
		httpServerPort int
		jobsC          chan jobInput
		stopC          chan struct{}
	}

	jobInput struct {
		freq time.Duration
	}
)

const (
	TestTypeMPChronos10        = "chronos_marketplace_10"
	TestTypeMPChronos100       = "chronos_marketplace_100"
	TestTypeMPChronos1k        = "chronos_marketplace_1k"
	TestTypeMPChronos10k       = "chronos_marketplace_10k"
	TestTypeMPChronos16k       = "chronos_marketplace_16k"
	TestTypeMPChronosHailstorm = "chronos_marketplace_hs"
)

const numWorkers = 50

func NewLoadGenerator(
	loadTestconfig *lib.LoadTestConfig,
	workerConfig *lib.WorkerConfig,
	serviceConfig *lib.ServiceConfig,
	client *lib.CadenceClient,
	metrics tally.Scope,
	logger *zap.Logger) lib.LoadGenerator {

	return &loadGenerator{
		config:         loadTestconfig,
		numTasklist:    workerConfig.NumTaskLists,
		client:         client,
		metrics:        metrics,
		logger:         logger,
		httpServerPort: serviceConfig.HTTPListenPort,
		jobsC:          make(chan jobInput, loadTestconfig.WorkflowRPS),
		stopC:          make(chan struct{}),
	}
}

func (g *loadGenerator) Start() error {
	for i := 0; i < numWorkers; i++ {
		go g.workerLoop()
	}
	go g.generator()
	return nil
}

func (g *loadGenerator) generator() error {
	g.logger.Info("loadgen started", zap.String("testName", g.config.TestName))
	dist := computeWorkloadDistribution(g.config.TestName, g.config.WorkflowRPS)
	for !g.quit() {
		g.createJobs(dist)
		time.Sleep(time.Second)
	}
	close(g.jobsC)
	g.logger.Info("loadgen done")
	return nil
}

func (g *loadGenerator) Stop() {
	if !g.quit() {
		close(g.stopC)
	}
}

func (g *loadGenerator) workerLoop() {
	g.logger.Info("loadgen worker started")
	for job := range g.jobsC {
		g.startJob(job)
	}
	g.logger.Info("loadgen worker done")
}

func (g *loadGenerator) startJob(job jobInput) error {
	taskList := common.GetTaskListName(rand.Intn(g.numTasklist))
	now := time.Now().UnixNano()
	var err error
	switch {
	case job.freq <= 60*time.Second:
		wo := g.workflowOptions(taskList, 30*time.Second)
		_, err = g.client.StartWorkflow(context.Background(), wo, highFreqTimerWorkflow, now, job.freq, g.newCallback())
		if err == nil {
			g.metrics.Counter("workflow.highfreq.started").Inc(1)
		}
	default:
		wo := g.workflowOptions(taskList, time.Second*20)
		_, err = g.client.StartWorkflow(context.Background(), wo, lowFreqTimerWorkflow, now, job.freq, g.newCallback())
		if err == nil {
			g.metrics.Counter("workflow.lowfreq.started").Inc(1)
		}
	}

	if err != nil {
		g.logger.Error("error starting workflow", zap.Error(err))
	}

	return nil
}

func (g *loadGenerator) workflowOptions(taskList string, decisionTimeout time.Duration) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		TaskList:                        taskList,
		ExecutionStartToCloseTimeout:    10 * time.Minute,
		DecisionTaskStartToCloseTimeout: decisionTimeout,
	}
}

func (g *loadGenerator) createJobs(dist workloadDistribution) {
	for i := range dist {
		for j := 0; j < dist[i].rps && dist[i].totalCount > 0; j++ {
			g.jobsC <- jobInput{freq: dist[i].timerFreq}
			dist[i].totalCount -= 1
		}
	}
}

func (g *loadGenerator) newCallback() HTTPCallback {
	return HTTPCallback{
		URL: "/jobs/create?name=foo-bar",
		Headers: map[string]string{
			"auth-token": uuid.New(),
			"host":       "localhost",
		},
	}
}

func (g *loadGenerator) quit() bool {
	select {
	case <-g.stopC:
		return true
	default:
		return false
	}
}
