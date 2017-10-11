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
	"sync/atomic"
	"time"

	"github.com/uber/cadence/bench-test/lib"

	"go.uber.org/cadence"
	"go.uber.org/zap"
)

var schedFreq = 10 * time.Minute

func init() {
	cadence.RegisterWorkflow(highFreqTimerWorkflow)
	cadence.RegisterActivity(highFreqTimerActivity)
}

func highFreqTimerWorkflow(ctx cadence.Context, scheduledTimeNanos int64, freq time.Duration, callback HTTPCallback) error {
	profile, err := lib.BeginWorkflow(ctx, "workflow.highfreq", scheduledTimeNanos)
	if err != nil {
		return err
	}

	taskList := cadence.GetWorkflowInfo(ctx).TaskListName
	activityOpts := cadence.ActivityOptions{
		TaskList:               taskList,
		StartToCloseTimeout:    schedFreq + 30*time.Second,
		ScheduleToStartTimeout: 30 * time.Second,
		ScheduleToCloseTimeout: schedFreq,
		HeartbeatTimeout:       2 * time.Minute,
	}

	for i := 0; i < 12; i++ {
		aCtx := cadence.WithActivityOptions(ctx, activityOpts)
		f := cadence.ExecuteActivity(aCtx, highFreqTimerActivity, cadence.Now(ctx).UnixNano(), freq, callback)
		err := f.Get(ctx, nil)
		if err != nil {
			cadence.GetMetricsScope(ctx).Counter("workflow.highfreq.errActivity").Inc(1)
			cadence.GetLogger(ctx).Error("activity error", zap.Error(err))
		}
	}

	profile.End(nil)
	return cadence.NewContinueAsNewError(ctx, highFreqTimerWorkflow, cadence.Now(ctx).UnixNano(), freq, callback)
}

func highFreqTimerActivity(ctx context.Context, scheduledTimeNanos int64, freq time.Duration, callback HTTPCallback) error {
	m := cadence.GetActivityMetricsScope(ctx)
	scope, sw := lib.RecordActivityStart(m, "activity.highfreq", scheduledTimeNanos)
	defer func() {
		scope.Counter("activity.highfreq.stopped").Inc(1)
		sw.Stop()
	}()

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

	rl := lib.GetActivityRebalanceLimiter(ctx)
	if rl == nil {
		cadence.GetActivityLogger(ctx).Error("context missing rate limiter")
		return nil
	}

	atomic.AddInt64(&wm.NumActivities, 1)
	defer atomic.AddInt64(&wm.NumActivities, -1)

	now := time.Now().UnixNano()
	nextHBTime := now + int64(time.Minute)
	rebalanceTime := now + int64(time.Minute*12) + rand.Int63n(int64(time.Minute*45))

	for {
		scope.Counter("callback.invoked").Inc(1)
		if err := callback.invoke(cfg.HTTPListenPort); err != nil {
			cadence.GetActivityLogger(ctx).Error("callback.invoke() error",
				zap.String("url", callback.URL), zap.Int("port", cfg.HTTPListenPort), zap.Error(err))
			scope.Counter("callback.errors").Inc(1)
		}

		now := time.Now().UnixNano()
		if now >= rebalanceTime {
			if rl.Allow() {
				return nil
			}
			rebalanceTime = now + int64(freq)
		}

		if now >= nextHBTime {
			cadence.RecordActivityHeartbeat(ctx)
			nextHBTime = now + int64(time.Minute)
			scope.Counter("activity.highfreq.hb").Inc(1)
			if ctx.Err() != nil {
				scope.Counter("activity.highfreq.errctx").Inc(1)
				cadence.GetActivityLogger(ctx).Error("activity context error", zap.Error(ctx.Err()))
				return nil
			}
		}

		time.Sleep(time.Duration(int64(freq)))
	}

	return nil
}
