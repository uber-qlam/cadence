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
	"github.com/uber/cadence/bench-test/lib"
	"github.com/uber/cadence/bench-test/load/basic"
	"github.com/uber/cadence/bench-test/load/chronos"

	"go.uber.org/zap"

	"github.com/uber-go/tally"
)

type (
	LoadGeneratorFactory struct {
		client  *lib.CadenceClient
		metrics tally.Scope
		logger  *zap.Logger
	}
)

func NewLoadGeneratorFactory(client *lib.CadenceClient, metrics tally.Scope, logger *zap.Logger) *LoadGeneratorFactory {
	return &LoadGeneratorFactory{
		client:  client,
		metrics: metrics,
		logger:  logger,
	}
}

func (f *LoadGeneratorFactory) Create(config *lib.LoadTestConfig, workerConfig *lib.WorkerConfig,
	serviceConfig *lib.ServiceConfig) (lib.LoadGenerator, error) {
	return chronos.NewLoadGenerator(config, workerConfig, serviceConfig, f.client, f.metrics, f.logger), nil
}

func (f *LoadGeneratorFactory) CreateBasic(config *lib.BasicTestConfig,
	workerConfig *lib.WorkerConfig) (lib.LoadGenerator, error) {
	return basic.NewLoadGenerator(config, workerConfig, f.client, f.metrics, f.logger), nil
}
