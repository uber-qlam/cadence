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
	"sync/atomic"
	"time"

	"github.com/uber/cadence/bench-test/lib"
	"go.uber.org/cadence"
	"go.uber.org/zap"
)

func init() {
	cadence.RegisterWorkflow(lowFreqTimerWorkflow)
	cadence.RegisterActivity(lowFreqTimerActivity)
}

func lowFreqTimerWorkflow(ctx cadence.Context, scheduledTimeNanos int64, freq time.Duration, callback HTTPCallback) error {
	profile, err := lib.BeginWorkflow(ctx, "workflow.lowfreq", scheduledTimeNanos)
	if err != nil {
		return err
	}

	taskList := cadence.GetWorkflowInfo(ctx).TaskListName

	activityOpts := cadence.ActivityOptions{
		TaskList:               taskList,
		StartToCloseTimeout:    2*time.Minute + 5*time.Second,
		ScheduleToStartTimeout: time.Minute,
		ScheduleToCloseTimeout: 2 * time.Minute,
	}

	for i := 0; i < 10; i++ {
		aCtx := cadence.WithActivityOptions(ctx, activityOpts)
		f := cadence.ExecuteActivity(aCtx, lowFreqTimerActivity, cadence.Now(ctx).UnixNano(), callback)
		f.Get(ctx, nil)
		start := cadence.Now(ctx)
		cadence.Sleep(ctx, freq)
		diff := cadence.Now(ctx).Sub(start)
		drift := lib.MaxInt64(0, int64(diff-freq))
		profile.Scope.Timer(lib.TimerDriftLatency).Record(time.Duration(drift))
	}

	profile.End(nil)
	return cadence.NewContinueAsNewError(ctx, lowFreqTimerWorkflow, cadence.Now(ctx).UnixNano(), freq, callback)
}

func lowFreqTimerActivity(ctx context.Context, scheduledTimeNanos int64, callback HTTPCallback) error {
	m := cadence.GetActivityMetricsScope(ctx)
	scope, sw := lib.RecordActivityStart(m, "activity.lowfreq", scheduledTimeNanos)
	defer sw.Stop()

	cfg := lib.GetActivityServiceConfig(ctx)
	if cfg == nil {
		cadence.GetActivityLogger(ctx).Error("context missing service config")
		return nil
	}

	wm := lib.GetActivityWorkerMetrics(ctx)
	if wm == nil {
		cadence.GetActivityLogger(ctx).Error("context missing worker metrics")
		return nil
	}

	atomic.AddInt64(&wm.NumActivities, 1)
	defer atomic.AddInt64(&wm.NumActivities, -1)

	scope.Counter("callback.invoked").Inc(1)
	err := callback.invoke(cfg.HTTPListenPort)
	if err != nil {
		cadence.GetActivityLogger(ctx).Error("callback.invoke() error",
			zap.String("url", callback.URL), zap.Int("port", cfg.HTTPListenPort), zap.Error(err))
		scope.Counter("callback.errors").Inc(1)
		return err
	}
	return nil
}
