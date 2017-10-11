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
	"time"
)

type (
	workload struct {
		rps        int
		totalCount int
		timerFreq  time.Duration
	}

	workloadDistribution []workload
)

func computeWorkloadDistribution(testName string, timerCreateRate int) workloadDistribution {
	switch testName {
	case TestTypeMPChronos10:
		return getDistForChronos10()
	case TestTypeMPChronos100:
		return getDistForChronos100()
	case TestTypeMPChronos1k:
		return getDistForChronos1k()
	case TestTypeMPChronos10k:
		return getDistForChronos10k()
	case TestTypeMPChronos16k:
		return getDistForChronos16k()
	case TestTypeMPChronosHailstorm:
		return getDistForChronosHailstorm()
	}
	return workloadDistribution([]workload{})
}

func getDistForChronos10() workloadDistribution {
	return workloadDistribution{
		{6, 360, 60 * time.Second},
		{2, 90, 45 * time.Second},
		{1, 30, 30 * time.Second},
		{1, 300, 300 * time.Second},
	}
}

func getDistForChronos100() workloadDistribution {
	return workloadDistribution{
		{60, 3600, 60 * time.Second},
		{20, 900, 45 * time.Second},
		{10, 300, 30 * time.Second},
		{10, 3000, 300 * time.Second},
	}
}

func getDistForChronos1k() workloadDistribution {
	return workloadDistribution{
		{60, 36000, 60 * time.Second},
		{20, 9000, 45 * time.Second},
		{10, 3000, 30 * time.Second},
		{10, 30000, 300 * time.Second},
	}
}

func getDistForChronos10k() workloadDistribution {
	return workloadDistribution{
		{60, 360000, 60 * time.Second},
		{20, 90000, 45 * time.Second},
		{10, 30000, 30 * time.Second},
		{10, 300000, 300 * time.Second},
	}
}

func getDistForChronos16k() workloadDistribution {
	return workloadDistribution{
		{60, 576000, 60 * time.Second},
		{20, 144000, 45 * time.Second},
		{10, 48000, 30 * time.Second},
		{10, 480000, 300 * time.Second},
	}
}

func getDistForChronosHailstorm() workloadDistribution {
	return workloadDistribution{
		{1000, 10000000, 60 * time.Second},
	}
}

func computeChronosWorkloadForRate(createRate int, fireRate int) workloadDistribution {
	percentDist := []int{58, 24, 12, 4, 2}
	output := workloadDistribution{
		{timerFreq: 60 * time.Second},
		{timerFreq: 45 * time.Second},
		{timerFreq: 300 * time.Second},
		{timerFreq: time.Second},
		{timerFreq: 30 * time.Second},
	}
	for i := range output {
		output[i].rps = (percentDist[i] * createRate) / 100
		output[i].totalCount = ((percentDist[i] * fireRate) / 100) * int(output[i].timerFreq/time.Second)
	}
	return output
}
