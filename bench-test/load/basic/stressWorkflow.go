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
	"time"

	"github.com/uber/cadence/bench-test/load/common"

	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/workflow"
)

type (
	// WorkflowParams inputs to workflow.
	WorkflowParams struct {
		ChainSequence    int
		ConcurrentCount  int
		TaskListNumber   int
		PayloadSizeBytes int
		ActivitySleepMin time.Duration
		ActivitySleepMax time.Duration
	}

	sleepActivityParams struct {
		Payload []byte
	}
)

func init() {
	workflow.Register(stressWorkflowExecute)
	activity.Register(sleepActivityExecute)
}

func stressWorkflowExecute(ctx workflow.Context, workflowInput WorkflowParams) (result []byte, err error) {
	activityParams := sleepActivityParams{
		Payload: make([]byte, workflowInput.PayloadSizeBytes)}

	ao := workflow.ActivityOptions{
		TaskList:               common.GetTaskListName(workflowInput.TaskListNumber),
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       20 * time.Second,
	}
	ctx1 := workflow.WithActivityOptions(ctx, ao)

	for i := 0; i < workflowInput.ChainSequence; i++ {
		selector := workflow.NewSelector(ctx)
		var activityErr error
		for j := 0; j < workflowInput.ConcurrentCount; j++ {
			selector.AddFuture(workflow.ExecuteActivity(ctx1, sleepActivityExecute, activityParams), func(f workflow.Future) {
				err := f.Get(ctx, nil)
				if err != nil {
					activityErr = err
				}
			})
		}

		for i := 0; i < workflowInput.ConcurrentCount; i++ {
			selector.Select(ctx) // this will wait for one branch
			if activityErr != nil {
				return nil, activityErr
			}
		}
	}
	return nil, nil
}

func sleepActivityExecute(ctx context.Context, activityParams sleepActivityParams) ([]byte, error) {
	return nil, nil
}
