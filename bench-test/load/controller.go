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

package load

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/uber/cadence/bench-test/cadence-client-go/factory"
	"github.com/uber/cadence/bench-test/lib"

	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/zap"
)

type Controller struct {
	sync.Mutex
	client  lib.CadenceClient
	runtime *lib.RuntimeContext
	factory *LoadGeneratorFactory
	loadgen lib.LoadGenerator
}

func NewController(cfg *lib.Config) (lib.Runnable, error) {
	rc, err := lib.NewRuntimeContext(cfg)
	if err != nil {
		return nil, err
	}

	client, err := lib.NewCadenceClient(rc)
	if err != nil {
		return nil, err
	}

	return &Controller{
		runtime: rc,
		client:  client,
		factory: NewLoadGeneratorFactory(&client, rc.Metrics, rc.Logger),
	}, nil
}

func (c *Controller) Run() error {
	var err error
	log := c.runtime.Logger

	c.runtime.Metrics.Counter("controller.restarts").Inc(1)

	if err = c.createDomain(); err != nil {
		log.Error("createDomain failed", zap.Error(err))
		return err
	}

	log.Info("Starting controller")
	http.HandleFunc("/stop", c.stopHandler)
	http.HandleFunc("/start", c.startHandler)
	addr := fmt.Sprintf(":%d", c.runtime.Service.HTTPListenPort)
	return http.ListenAndServe(addr, nil)
}

func (c *Controller) startHandler(w http.ResponseWriter, r *http.Request) {
	c.Lock()
	defer c.Unlock()
	testName := r.URL.Query().Get("test")
	if c.loadgen != nil {
		http.Error(w, "test already running", http.StatusInternalServerError)
		return
	}
	if len(testName) == 0 {
		http.Error(w, "missing test query param", http.StatusBadRequest)
		return
	}

	log := c.runtime.Logger
	var loadgen lib.LoadGenerator
	var err error
	if testName == "basic" {
		loadgen, err = c.createBasicLoadGenerator(r)
	} else {
		loadgen, err = c.createLoadGenerator(testName, r)
	}
	if err != nil {
		log.Error("failed to create load generator", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := loadgen.Start(); err != nil {
		log.Error("failed to start load generator", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.loadgen = loadgen
	c.runtime.Metrics.Counter("controller.test.started").Inc(1)
	log.Info("load test initiated", zap.String("name", testName))
}

func (c *Controller) stopHandler(w http.ResponseWriter, r *http.Request) {
	c.Lock()
	defer c.Unlock()

	if c.loadgen != nil {
		c.loadgen.Stop()
		c.loadgen = nil
		c.runtime.Metrics.Counter("controller.test.stopped").Inc(1)
		c.runtime.Logger.Info("load test stopped")
	}

	pageSz := int32(100)
	pageToken := []byte(nil)
	workerCh := make(chan *shared.WorkflowExecution, 10)

	for i := 0; i < 10; i++ {
		go func() {
			for e := range workerCh {
				wfID := e.GetWorkflowId()
				runID := e.GetRunId()
				err := c.client.TerminateWorkflow(context.Background(), wfID, runID, "stop command", nil)
				if err != nil {
					c.runtime.Logger.Error("terminate workflow failed", zap.Error(err))
				}
			}
		}()
	}

	for {
		startTime := time.Now().UnixNano() - int64(time.Hour*24)
		endTime := time.Now().UnixNano()
		request := &shared.ListOpenWorkflowExecutionsRequest{
			MaximumPageSize: &pageSz,
			NextPageToken:   pageToken,
			StartTimeFilter: &shared.StartTimeFilter{
				EarliestTime: &startTime,
				LatestTime:   &endTime,
			},
		}
		resp, err := c.client.ListOpenWorkflow(context.Background(), request)
		if err != nil {
			c.runtime.Logger.Error("list open workflow failed", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		for _, e := range resp.Executions {
			workerCh <- e.Execution
		}
		pageToken = resp.NextPageToken
		if len(pageToken) == 0 {
			return
		}
	}

	close(workerCh)
}

func (c *Controller) createBasicLoadGenerator(r *http.Request) (lib.LoadGenerator, error) {
	basicConfig := c.runtime.Basic
	workerConfig := c.runtime.Worker
	totalLaunchCountStr := r.URL.Query().Get("totalLaunchCount")
	if len(totalLaunchCountStr) > 0 {
		totalLaunchCount, _ := strconv.Atoi(totalLaunchCountStr)
		basicConfig.TotalLaunchCount = totalLaunchCount
	}
	routineCountStr := r.URL.Query().Get("routineCount")
	if len(routineCountStr) > 0 {
		routineCount, _ := strconv.Atoi(routineCountStr)
		basicConfig.RoutineCount = routineCount
	}
	chainSequenceStr := r.URL.Query().Get("chainSequence")
	if len(chainSequenceStr) > 0 {
		chainSequence, _ := strconv.Atoi(chainSequenceStr)
		basicConfig.ChainSequence = chainSequence
	}
	concurrentCountStr := r.URL.Query().Get("concurrentCount")
	if len(chainSequenceStr) > 0 {
		concurrentCount, _ := strconv.Atoi(concurrentCountStr)
		basicConfig.ConcurrentCount = concurrentCount
	}
	payloadSizeBytesStr := r.URL.Query().Get("payloadSizeBytes")
	if len(payloadSizeBytesStr) > 0 {
		payloadSizeBytes, _ := strconv.Atoi(payloadSizeBytesStr)
		basicConfig.PayloadSizeBytes = payloadSizeBytes
	}
	return c.factory.CreateBasic(&basicConfig, &workerConfig)
}

func (c *Controller) createLoadGenerator(testName string, r *http.Request) (lib.LoadGenerator, error) {
	loadCfg := c.runtime.Load
	loadCfg.TestName = testName
	return c.factory.Create(&loadCfg, &c.runtime.Worker, &c.runtime.Service)
}

func (c *Controller) createDomain() error {
	// only create the domain in development environment
	if c.runtime.Service.Env != string(factory.Development) {
		return nil
	}
	name := c.runtime.Service.Domain
	desc := "Domain for running cadence load test"
	owner := "cadence-oncall-group@uber.com"
	return c.client.CreateDomain(name, desc, owner)
}
