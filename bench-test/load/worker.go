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
	"math/rand"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/bench-test/lib"
	"github.com/uber/cadence/bench-test/load/chronos"
	"github.com/uber/cadence/bench-test/load/common"

	"go.uber.org/cadence"
	"go.uber.org/zap"

	"golang.org/x/time/rate"
)

type (
	loadTestWorker struct {
		rand          *rand.Rand
		client        lib.CadenceClient
		runtime       *lib.RuntimeContext
		workerMetrics []lib.WorkerMetrics
	}
)

const (
	activityWorkerConcurrency = 5000
)

func NewWorker(cfg *lib.Config) (lib.Runnable, error) {
	rc, err := lib.NewRuntimeContext(cfg)
	if err != nil {
		return nil, err
	}

	client, err := lib.NewCadenceClient(rc)
	if err != nil {
		return nil, err
	}

	return &loadTestWorker{
		runtime:       rc,
		client:        client,
		rand:          rand.New(rand.NewSource(lib.RandInt64())),
		workerMetrics: make([]lib.WorkerMetrics, rc.Worker.NumTaskLists),
	}, nil
}

func (w *loadTestWorker) Run() error {
	w.runtime.Metrics.Counter("worker.restarts").Inc(1)
	config := &w.runtime.Worker
	for i := 0; i < config.NumTaskLists; i++ {
		taskList := common.GetTaskListName(i)
		worker := cadence.NewWorker(w.client.TChan, w.runtime.Service.Domain, taskList, w.newWorkerOptions(i))
		if err := worker.Start(); err != nil {
			return err
		}
	}
	go w.emitWorkerMetrics()
	chronos.RegisterHandlers(w.runtime.Metrics)
	addr := fmt.Sprintf("127.0.0.1:%d", w.runtime.Service.HTTPListenPort)
	return http.ListenAndServe(addr, nil)
}

func (w *loadTestWorker) emitWorkerMetrics() {
	hostname, err := os.Hostname()
	if err != nil {
		w.runtime.Logger.Error("os.Hostname() failed", zap.Error(err))
		return
	}
	scope := w.runtime.Metrics.Tagged(map[string]string{"hostname": hostname})
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sum := int64(0)
			for i := range w.workerMetrics {
				val := atomic.LoadInt64(&w.workerMetrics[i].NumActivities)
				sum += val
				tlScope := scope.Tagged(map[string]string{"tasklist": fmt.Sprintf("tl_%v", i)})
				tlScope.Counter("worker.tl.numActivities").Inc(val)
			}
			scope.Gauge("worker.numActivities").Update(float64(sum))
		}
	}
}

func (w *loadTestWorker) newWorkerOptions(id int) cadence.WorkerOptions {
	return cadence.WorkerOptions{
		Logger:                             w.runtime.Logger,
		MetricsScope:                       w.runtime.Metrics,
		MaxConcurrentActivityExecutionSize: activityWorkerConcurrency,
		BackgroundActivityContext:          w.newActivityContext(id),
		MaxActivityExecutionPerSecond:      10,
	}
}

func (w *loadTestWorker) newActivityContext(id int) context.Context {
	ctx := context.Background()
	limiter := rate.NewLimiter(rate.Every(time.Hour), 1)
	ctx = context.WithValue(ctx, lib.CtxKeyRebalanceRateLimiter, limiter)
	ctx = context.WithValue(ctx, lib.CtxKeyWorkerMetrics, &w.workerMetrics[id])
	return context.WithValue(ctx, lib.CtxKeyServiceConfig, &w.runtime.Service)
}
