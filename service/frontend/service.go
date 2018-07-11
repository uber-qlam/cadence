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

package frontend

import (
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/cassandra"
	"github.com/uber/cadence/common/service"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

// Config represents configuration for cadence-frontend service
type Config struct {
	VisibilityMaxPageSize dynamicconfig.IntPropertyFnWithDomainFilter
	HistoryMaxPageSize    dynamicconfig.IntPropertyFnWithDomainFilter
	RPS                   dynamicconfig.IntPropertyFn

	// Persistence settings
	HistoryMgrNumConns dynamicconfig.IntPropertyFn
}

// NewConfig returns new service config with default values
func NewConfig(dc *dynamicconfig.Collection) *Config {
	return &Config{
		VisibilityMaxPageSize: dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendVisibilityMaxPageSize, 1000),
		HistoryMaxPageSize:    dc.GetIntPropertyFilteredByDomain(dynamicconfig.FrontendHistoryMaxPageSize, 1000),
		RPS:                   dc.GetIntProperty(dynamicconfig.FrontendRPS, 1200),
		HistoryMgrNumConns:    dc.GetIntProperty(dynamicconfig.FrontendHistoryMgrNumConns, 10),
	}
}

// Service represents the cadence-frontend service
type Service struct {
	stopC  chan struct{}
	config *Config
	params *service.BootstrapParams
}

// NewService builds a new cadence-frontend service
func NewService(params *service.BootstrapParams) common.Daemon {
	return &Service{
		params: params,
		config: NewConfig(dynamicconfig.NewCollection(params.DynamicConfig, params.Logger)),
		stopC:  make(chan struct{}),
	}
}

// Start starts the service
func (s *Service) Start() {

	var p = s.params
	var log = p.Logger

	log.Infof("%v starting", common.FrontendServiceName)

	base := service.New(p)

	metadata, err := cassandra.NewMetadataManagerProxy(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		p.ClusterMetadata.GetCurrentClusterName(),
		p.Logger)

	if err != nil {
		log.Fatalf("failed to create metadata manager: %v", err)
	}
	metadata = persistence.NewMetadataPersistenceClient(metadata, base.GetMetricsClient(), log)

	visibility, err := cassandra.NewVisibilityPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.VisibilityKeyspace,
		p.Logger)

	if err != nil {
		log.Fatalf("failed to create visiblity manager: %v", err)
	}
	visibility = persistence.NewVisibilityPersistenceClient(visibility, base.GetMetricsClient(), log)

	history, err := cassandra.NewHistoryPersistence(p.CassandraConfig.Hosts,
		p.CassandraConfig.Port,
		p.CassandraConfig.User,
		p.CassandraConfig.Password,
		p.CassandraConfig.Datacenter,
		p.CassandraConfig.Keyspace,
		s.config.HistoryMgrNumConns(),
		p.Logger)

	if err != nil {
		log.Fatalf("Creating Cassandra history manager persistence failed: %v", err)
	}

	history = persistence.NewHistoryPersistenceClient(history, base.GetMetricsClient(), log)

	// TODO when global domain is enabled, uncomment the line below and remove the line after
	var kafkaProducer messaging.Producer
	if base.GetClusterMetadata().IsGlobalDomainEnabled() {
		kafkaProducer, err = base.GetMessagingClient().NewProducer(base.GetClusterMetadata().GetCurrentClusterName())
		if err != nil {
			log.Fatalf("Creating kafka producer failed: %v", err)
		}
	} else {
		kafkaProducer = &mocks.KafkaProducer{}
	}

	wfHandler := NewWorkflowHandler(base, s.config, metadata, history, visibility, kafkaProducer)
	wfHandler.Start()

	adminHandler := NewAdminHandler(base, p.CassandraConfig.NumHistoryShards, metadata)
	adminHandler.Start()

	log.Infof("%v started", common.FrontendServiceName)

	<-s.stopC

	base.Stop()
}

// Stop stops the service
func (s *Service) Stop() {
	select {
	case s.stopC <- struct{}{}:
	default:
	}
	s.params.Logger.Infof("%v stopped", common.FrontendServiceName)
}
