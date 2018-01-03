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
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

func init() {
	workflow.Register(lowFreqTimerWorkflow)
	activity.Register(lowFreqTimerActivity)
}

func lowFreqTimerWorkflow(ctx workflow.Context, scheduledTimeNanos int64, freq time.Duration, callback HTTPCallback) error {
	profile, err := lib.BeginWorkflow(ctx, "workflow.lowfreq", scheduledTimeNanos)
	if err != nil {
		return err
	}

	taskList := workflow.GetInfo(ctx).TaskListName

	activityOpts := workflow.ActivityOptions{
		TaskList:               taskList,
		StartToCloseTimeout:    2*time.Minute + 5*time.Second,
		ScheduleToStartTimeout: time.Minute,
		ScheduleToCloseTimeout: 2 * time.Minute,
	}

	for i := 0; i < 10; i++ {
		aCtx := workflow.WithActivityOptions(ctx, activityOpts)
		f := workflow.ExecuteActivity(aCtx, lowFreqTimerActivity, workflow.Now(ctx).UnixNano(), callback)
		f.Get(ctx, nil)
		start := workflow.Now(ctx)
		workflow.Sleep(ctx, freq)
		diff := workflow.Now(ctx).Sub(start)
		drift := lib.MaxInt64(0, int64(diff-freq))
		profile.Scope.Timer(lib.TimerDriftLatency).Record(time.Duration(drift))
	}

	profile.End(nil)
	return workflow.NewContinueAsNewError(ctx, lowFreqTimerWorkflow, workflow.Now(ctx).UnixNano(), freq, callback)
}

func lowFreqTimerActivity(ctx context.Context, scheduledTimeNanos int64, callback HTTPCallback) error {
	m := activity.GetMetricsScope(ctx)
	scope, sw := lib.RecordActivityStart(m, "activity.lowfreq", scheduledTimeNanos)
	defer sw.Stop()

	cfg := lib.GetActivityServiceConfig(ctx)
	if cfg == nil {
		activity.GetLogger(ctx).Error("context missing service config")
		return nil
	}

	wm := lib.GetActivityWorkerMetrics(ctx)
	if wm == nil {
		activity.GetLogger(ctx).Error("context missing worker metrics")
		return nil
	}

	atomic.AddInt64(&wm.NumActivities, 1)
	defer atomic.AddInt64(&wm.NumActivities, -1)

	scope.Counter("callback.invoked").Inc(1)
	err := callback.invoke(cfg.HTTPListenPort)
	if err != nil {
		activity.GetLogger(ctx).Error("callback.invoke() error",
			zap.String("url", callback.URL), zap.Int("port", cfg.HTTPListenPort), zap.Error(err))
		scope.Counter("callback.errors").Inc(1)
		return err
	}
	return nil
}
