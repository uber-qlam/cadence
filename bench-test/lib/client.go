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
	"context"

	"github.com/uber/cadence/bench-test/cadence-client-go/factory"

	//"code.uber.internal/go-common.git/x/tchannel"

	"math/rand"
	"time"

	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/client"
)

const workflowRetentionDays = 1

// CadenceClient is an abstraction on top of
// the cadence library client that serves as
// a union of all the client interfaces that
// the library exposes
type CadenceClient struct {
	client.Client
	// domainClient only exposes domain API
	client.DomainClient
	// this low level tchan client is needed to start the workers
	Service workflowserviceclient.Interface
}

// createDomain creates a cadence domain with the given name and description
// if the domain already exist, this method silently returns success
func (client *CadenceClient) CreateDomain(name string, desc string, owner string) error {
	emitMetric := true
	retention := int32(workflowRetentionDays)
	req := &shared.RegisterDomainRequest{
		Name:                                   &name,
		Description:                            &desc,
		OwnerEmail:                             &owner,
		WorkflowExecutionRetentionPeriodInDays: &retention,
		EmitMetric:                             &emitMetric,
	}
	err := client.Register(context.Background(), req)
	if err != nil {
		if _, ok := err.(*shared.DomainAlreadyExistsError); !ok {
			return err
		}
	}
	return nil
}

// NewCadenceClient builds a CadenceClient from the runtimeContext
func NewCadenceClient(runtime *RuntimeContext) (CadenceClient, error) {
	var err error

	builder := factory.NewBuilder()
	builder.SetDomain(runtime.Service.Domain)
	builder.SetEnv(factory.Environment(runtime.Service.Env))
	builder.SetMetricsScope(runtime.Metrics)

	rand.Seed(time.Now().Unix())
	hosts := runtime.Service.ServerHostPort
	builder.SetHostPort(hosts[rand.Intn(len(hosts))])

	var client CadenceClient
	client.Client, err = builder.BuildCadenceClient()
	if err != nil {
		return CadenceClient{}, err
	}
	client.DomainClient, err = builder.BuildCadenceDomainClient()
	if err != nil {
		return CadenceClient{}, err
	}
	client.Service, err = builder.BuildServiceClient()
	if err != nil {
		return CadenceClient{}, err
	}
	return client, nil
}
