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
	"fmt"
	//"time"

	//"code.uber.internal/devexp/cadence-client-go/factory"
	//"code.uber.internal/go-common.git/x/config"
	"context"
	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	"go.uber.org/zap"
	//"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
)

type (
	Config struct {
		M3      m3.Configuration `yaml:"m3"`
		Zap     zap.Config       `yaml:"zap"`
		Load    LoadTestConfig   `yaml:"load"`
		Basic   BasicTestConfig  `yaml:"basic"`
		Worker  WorkerConfig     `yaml:"worker"`
		Service ServiceConfig    `yaml:"service"`
	}

	ServiceConfig struct {
		Env            string `yaml:"env"`
		Deployment     string `yaml:"deployment"`
		Role           string `yaml:"role"`
		Domain         string `yaml:"domain"`
		ServerHostPort string `yaml:"serverHostPort"`
		HTTPListenPort int    `yaml:"httpListenPort"`
	}

	LoadTestConfig struct {
		TestName    string `yaml:"testName"`
		WorkflowRPS int    `yaml:"workflowRPS"`
	}

	WorkerConfig struct {
		NumTaskLists int `yaml:"numTaskLists"`
	}

	BasicTestConfig struct {
		TotalLaunchCount int `yaml:"totalLaunchCount"`
		RoutineCount     int `yaml:"routineCount"`
		ChainSequence    int `yaml:"chainSequence"`
		ConcurrentCount  int `yaml:"concurrentCount"`
		PayloadSizeBytes int `yaml:"payloadSizeBytes"`
	}
)

func LoadConfig() (*Config, error) {
	var cfg Config
	//if err := config.Load(&cfg); err != nil {
	//	return nil, err
	//}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) validate() error {
	if !isValidEnv(c.Service.Env) {
		return fmt.Errorf("invalid value for env: %v", c.Service.Env)
	}
	if c.Service.HTTPListenPort == 0 {
		return fmt.Errorf("http listen port cannot be empty")
	}
	if len(c.Service.Deployment) == 0 {
		c.Service.Deployment = "test"
	}
	return nil
}

func isValidEnv(env string) bool {
	//validSet := []factory.Environment{factory.Development, factory.Staging, factory.Production}
	//for _, e := range validSet {
	//	if env == string(e) {
	//		return true
	//	}
	//}
	return false
}

type RuntimeContext struct {
	Logger  *zap.Logger
	Metrics tally.Scope
	Load    LoadTestConfig
	Basic   BasicTestConfig
	Service ServiceConfig
	Worker  WorkerConfig
}

// NewRuntimeContext builds a runtime context from the config
func NewRuntimeContext(cfg *Config) (*RuntimeContext, error) {
	scope, err := newTallyScope(&cfg.M3)
	if err != nil {
		return nil, err
	}
	logger, err := newLogger(&cfg.Zap, cfg.Service.Env)
	if err != nil {
		return nil, err
	}
	return &RuntimeContext{
		Logger:  logger,
		Metrics: scope,
		Load:    cfg.Load,
		Basic:   cfg.Basic,
		Service: cfg.Service,
		Worker:  cfg.Worker,
	}, nil
}

// newTallyScope builds and returns a tally scope from m3 configuration
func newTallyScope(cfg *m3.Configuration) (tally.Scope, error) {
	//reporter, err := cfg.NewReporter()
	//if err != nil {
	//	return nil, err
	//}
	//scopeOpts := tally.ScopeOptions{CachedReporter: reporter}
	//scope, _ := tally.NewRootScope(scopeOpts, time.Second)
	return nil, nil
}

// newLogger creates and returns a new instance of bark logger
func newLogger(cfg *zap.Config, env string) (*zap.Logger, error) {
	//if factory.Environment(env) == factory.Development {
	//	devConfig := zap.NewDevelopmentConfig()
	//	devConfig.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	//	return devConfig.Build()
	//}
	//cfg.Encoding = "json"
	//cfg.EncoderConfig = zap.NewProductionEncoderConfig()
	//cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	//cfg.Development = false
	//return cfg.Build()
	return nil, nil
}

const CtxKeyServiceConfig = "serviceConfig"
const CtxKeyWorkerMetrics = "workerMetrics"
const CtxKeyRebalanceRateLimiter = "rebalanceRate"

func GetActivityServiceConfig(ctx context.Context) *ServiceConfig {
	val := ctx.Value(CtxKeyServiceConfig)
	if val == nil {
		return nil
	}
	return val.(*ServiceConfig)
}

func GetActivityWorkerMetrics(ctx context.Context) *WorkerMetrics {
	val := ctx.Value(CtxKeyWorkerMetrics)
	if val == nil {
		return nil
	}
	return val.(*WorkerMetrics)
}

func GetActivityRebalanceLimiter(ctx context.Context) *rate.Limiter {
	val := ctx.Value(CtxKeyRebalanceRateLimiter)
	if val == nil {
		return nil
	}
	return val.(*rate.Limiter)
}
