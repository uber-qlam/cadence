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
	"log"

	"github.com/pkg/errors"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/client"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/transport/tchannel"
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
	dispatcher     *yarpc.Dispatcher
	hostPort       string
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
func (b *WorkflowClientBuilder) BuildCadenceClient() (client.Client, error) {
	service, err := b.BuildServiceClient()
	if err != nil {
		return nil, err
	}

	return client.NewClient(
		service, b.domain, &client.Options{Identity: b.clientIdentity, MetricsScope: b.metricsScope}), nil
}

// BuildCadenceDomainClient builds a domain client to cadence service
func (b *WorkflowClientBuilder) BuildCadenceDomainClient() (client.DomainClient, error) {
	service, err := b.BuildServiceClient()
	if err != nil {
		return nil, err
	}

	return client.NewDomainClient(
		service, &client.Options{Identity: b.clientIdentity, MetricsScope: b.metricsScope}), nil
}

// BuildServiceClient builds a thrift service client to cadence service
func (b *WorkflowClientBuilder) BuildServiceClient() (workflowserviceclient.Interface, error) {
	if err := b.build(); err != nil {
		return nil, err
	}

	if b.dispatcher == nil {
		log.Fatalf("No RPC dispatcher provided to create a connection to Cadence Service")
	}

	serviceName := getServiceName(b.env)
	return workflowserviceclient.New(b.dispatcher.ClientConfig(serviceName)), nil
}

func (b *WorkflowClientBuilder) build() error {
	if b.dispatcher != nil {
		return nil
	}

	if len(b.hostPort) <= 0 {
		return errors.New("HostPort must NOT be empty")
	}

	serviceName := getServiceName(b.env)

	if len(b.hostPort) > 0 {
		ch, err := tchannel.NewChannelTransport(
			tchannel.ServiceName(_cadenceClientName))
		if err != nil {
			log.Fatalf("Failed to create transport channel with error: %v", err)
		}

		b.dispatcher = yarpc.NewDispatcher(yarpc.Config{
			Name: _cadenceClientName,
			Outbounds: yarpc.Outbounds{
				serviceName: {Unary: ch.NewSingleOutbound(b.hostPort)},
			},
		})
	} else {
		ch, err := tchannel.NewChannelTransport(
			tchannel.ServiceName(serviceName))
		if err != nil {
			log.Fatalf("Failed to create transport channel with error: %v", err)
		}

		b.dispatcher = yarpc.NewDispatcher(yarpc.Config{
			Name: _cadenceClientName,
			Outbounds: yarpc.Outbounds{
				serviceName: {Unary: ch.NewSingleOutbound(b.hostPort)},
			},
		})
	}

	if b.dispatcher != nil {
		if err := b.dispatcher.Start(); err != nil {
			log.Fatalf("Failed to create outbound transport channel: %v", err)
		}
	}

	return nil
}

func getServiceName(env Environment) string {
	svc := _cadenceFrontendService
	if len(string(env)) > 0 && env != Production && env != Development {
		svc = _cadenceFrontendService + "-" + string(env)
	}
	return svc
}
