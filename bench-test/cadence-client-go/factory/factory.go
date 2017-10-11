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

package factory

import (
	"github.com/pkg/errors"
	"github.com/uber-go/tally"
	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"go.uber.org/cadence"
	m "go.uber.org/cadence/.gen/go/cadence"
)

// Environment is the cadence environment
type Environment string

const (
	_cadenceClientName      = "cadence-client"
	_cadenceFrontendService = "cadence-frontend"
)

const (
	// Development is for laptop or development testing
	Development Environment = "development"
	// Staging is the cadence staging cluster
	Staging Environment = "staging"
	// Production is the cadence production cluster
	Production Environment = "prod"
)

// WorkflowClientBuilder build client to cadence service
// Use HostPort to connect to a specific host (for oenbox case), or UNSName to connect to cadence server by UNS name.
type WorkflowClientBuilder struct {
	tchanClient    thrift.TChanClient
	hostPort       string
	needsInitUNS   bool
	domain         string
	clientIdentity string
	metricsScope   tally.Scope
	env            Environment
}

// NewBuilder creates a new WorkflowClientBuilder
func NewBuilder() *WorkflowClientBuilder {
	return &WorkflowClientBuilder{env: Development}
}

// SetHostPort sets the hostport for the builder
func (b *WorkflowClientBuilder) SetHostPort(hostport string) *WorkflowClientBuilder {
	b.hostPort = hostport
	return b
}

// SetNeedsInitUNS sets the flag to indicate how to call unsclient.SetParams()
// If needsInit is true, builder will call unsclient.SetParams for you. If it is false, you are responsible to call it.
// Default value is false.
func (b *WorkflowClientBuilder) SetNeedsInitUNS(needsInit bool) *WorkflowClientBuilder {
	b.needsInitUNS = needsInit
	return b
}

// SetDomain sets the domain for the builder
func (b *WorkflowClientBuilder) SetDomain(domain string) *WorkflowClientBuilder {
	b.domain = domain
	return b
}

// SetClientIdentity sets the identity for the builder
func (b *WorkflowClientBuilder) SetClientIdentity(identity string) *WorkflowClientBuilder {
	b.clientIdentity = identity
	return b
}

// SetMetricsScope sets the metrics scope for the builder
func (b *WorkflowClientBuilder) SetMetricsScope(metricsScope tally.Scope) *WorkflowClientBuilder {
	b.metricsScope = metricsScope
	return b
}

// SetEnv sets the cadence environment to call (test, staging, prod)
func (b *WorkflowClientBuilder) SetEnv(env Environment) *WorkflowClientBuilder {
	b.env = env
	return b
}

// BuildCadenceClient builds a client to cadence service
func (b *WorkflowClientBuilder) BuildCadenceClient() (cadence.Client, error) {
	service, err := b.BuildServiceClient()
	if err != nil {
		return nil, err
	}

	return cadence.NewClient(
		service, b.domain, &cadence.ClientOptions{Identity: b.clientIdentity, MetricsScope: b.metricsScope}), nil
}

// BuildCadenceDomainClient builds a domain client to cadence service
func (b *WorkflowClientBuilder) BuildCadenceDomainClient() (cadence.DomainClient, error) {
	service, err := b.BuildServiceClient()
	if err != nil {
		return nil, err
	}

	return cadence.NewDomainClient(
		service, &cadence.ClientOptions{Identity: b.clientIdentity, MetricsScope: b.metricsScope}), nil
}

// BuildServiceClient builds a thrift service client to cadence service
func (b *WorkflowClientBuilder) BuildServiceClient() (m.TChanWorkflowService, error) {
	if err := b.build(); err != nil {
		return nil, err
	}

	return m.NewTChanWorkflowServiceClient(b.tchanClient), nil
}

func (b *WorkflowClientBuilder) build() error {
	if b.tchanClient != nil {
		return nil
	}
	if len(b.hostPort) <= 0 {
		return errors.New("HostPort must NOT be empty")
	}

	var tchan *tchannel.Channel
	var err error

	tchan, err = tchannel.NewChannel(_cadenceClientName, nil)

	if err != nil {
		return err
	}

	var opts *thrift.ClientOptions
	if len(b.hostPort) > 0 {
		opts = &thrift.ClientOptions{HostPort: b.hostPort}
	}

	svc := getServiceName(b.env)
	b.tchanClient = thrift.NewClient(tchan, svc, opts)
	return nil
}

func getServiceName(env Environment) string {
	svc := _cadenceFrontendService
	if len(string(env)) > 0 && env != Production && env != Development {
		svc = _cadenceFrontendService + "-" + string(env)
	}
	return svc
}
