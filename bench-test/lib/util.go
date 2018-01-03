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

package lib

import (
	crypto "crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"go.uber.org/cadence/workflow"
	"go.uber.org/zap"
)

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func MinInt64(a, b int64) int64 {
	if a > b {
		return b
	}
	return a
}

func RandInt64() int64 {
	bytes := make([]byte, 8)
	_, err := crypto.Read(bytes)
	if err != nil {
		return time.Now().UnixNano()
	}
	val := binary.BigEndian.Uint64(bytes)
	return int64(val & 0x3FFFFFFFFFFFFFFF)
}

const workflowVersion = 1
const workflowChangeID = "initial"

// checkWFVersionCompatibility takes a cadence.Context param and
// validates that the workflow task currently being handled
// is compatible with this version of the bench - this method
// MUST only be called within a workflow function and it MUST
// be the first line in the workflow function
// Returns an error if the version is incompatible
func checkWFVersionCompatibility(ctx workflow.Context) error {
	version := workflow.GetVersion(ctx, workflowChangeID, workflowVersion, workflowVersion)
	if version != workflowVersion {
		workflow.GetLogger(ctx).Error("workflow version mismatch",
			zap.Int("want", int(workflowVersion)), zap.Int("got", int(version)))
		return fmt.Errorf("workflow version mismatch, want=%v, got=%v", workflowVersion, version)
	}
	return nil
}

// beginWorkflow executes the common steps involved in all the workflow functions
// It checks for workflow task version compatibility and also records the execution
// in m3. This function must be the first call in every workflow function
// Returns metrics scope on success, error on failure
func BeginWorkflow(ctx workflow.Context, wfType string, scheduledTimeNanos int64) (*workflowMetricsProfile, error) {
	profile := recordWorkflowStart(ctx, wfType, scheduledTimeNanos)
	if err := checkWFVersionCompatibility(ctx); err != nil {
		profile.Scope.Counter(errIncompatibleVersion).Inc(1)
		return nil, err
	}
	return profile, nil
}
