// Copyright (c) 2018 Uber Technologies, Inc.
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

package persistence

import (
	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber/cadence/common/cluster"
	"os"
	"testing"

	gen "github.com/uber/cadence/.gen/go/shared"
)

type (
	mysqlMetadataPersistenceSuite struct {
		suite.Suite
		TestBase
		*require.Assertions
	}
)

func TestMysqlMetadataPersistenceSuite(t *testing.T) {
	s := new(mysqlMetadataPersistenceSuite)
	suite.Run(t, s)
}

func (m *mysqlMetadataPersistenceSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	m.SetupWorkflowStore()
}

func (m *mysqlMetadataPersistenceSuite) SetupTest() {
	// Have to define our overridden assertions in the test setup. If we did it earlier, s.T() will return nil
	m.Assertions = require.New(m.T())
}

func (m *mysqlMetadataPersistenceSuite) TearDownSuite() {
	m.TearDownWorkflowStore()
}

func (m *mysqlMetadataPersistenceSuite) TestCreateDomain() {
	id := uuid.New()
	name := "create-domain-test-name"
	status := DomainStatusRegistered
	description := "create-domain-test-description"
	owner := "create-domain-test-owner"
	retention := int32(10)
	emitMetric := true
	isGlobalDomain := false
	configVersion := int64(0)
	failoverVersion := int64(0)

	resp0, err0 := m.CreateDomain(
		&DomainInfo{
			ID:          id,
			Name:        name,
			Status:      status,
			Description: description,
			OwnerEmail:  owner,
		},
		&DomainConfig{
			Retention:  retention,
			EmitMetric: emitMetric,
		},
		&DomainReplicationConfig{},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)

	m.Nil(err0)
	m.NotNil(resp0)
	m.Equal(id, resp0.ID)

	// for domain which do not have replication config set, will default to
	// use current cluster as active, with current cluster as all clusters
	resp1, err1 := m.GetDomain(id, "")
	m.Nil(err1)
	m.NotNil(resp1)
	m.Equal(id, resp1.Info.ID)
	m.Equal(name, resp1.Info.Name)
	m.Equal(status, resp1.Info.Status)
	m.Equal(description, resp1.Info.Description)
	m.Equal(owner, resp1.Info.OwnerEmail)
	m.Equal(retention, resp1.Config.Retention)
	m.Equal(emitMetric, resp1.Config.EmitMetric)
	m.Equal(cluster.TestCurrentClusterName, resp1.ReplicationConfig.ActiveClusterName)
	m.Equal(1, len(resp1.ReplicationConfig.Clusters))
	m.Equal(isGlobalDomain, resp1.IsGlobalDomain)
	m.Equal(configVersion, resp1.ConfigVersion)
	m.Equal(failoverVersion, resp1.FailoverVersion)
	m.True(resp1.ReplicationConfig.Clusters[0].ClusterName == cluster.TestCurrentClusterName)
	m.Equal(int64(0), resp1.DBVersion)

	resp2, err2 := m.CreateDomain(
		&DomainInfo{
			ID:          uuid.New(),
			Name:        name,
			Status:      status,
			Description: "fail",
			OwnerEmail:  "fail",
		},
		&DomainConfig{
			Retention:  100,
			EmitMetric: false,
		},
		&DomainReplicationConfig{},
		isGlobalDomain,
		configVersion,
		failoverVersion,
	)
	m.NotNil(err2)
	m.IsType(&gen.DomainAlreadyExistsError{}, err2)
	m.Nil(resp2)
}

func (m *mysqlMetadataPersistenceSuite) CreateDomain(info *DomainInfo, config *DomainConfig,
	replicationConfig *DomainReplicationConfig, isGlobalDomain bool, configVersion int64, failoverVersion int64) (*CreateDomainResponse, error) {
	return m.MetadataManager.CreateDomain(&CreateDomainRequest{
		Info:              info,
		Config:            config,
		ReplicationConfig: replicationConfig,
		IsGlobalDomain:    isGlobalDomain,
		ConfigVersion:     configVersion,
		FailoverVersion:   failoverVersion,
	})
}

func (m *mysqlMetadataPersistenceSuite) GetDomain(id, name string) (*GetDomainResponse, error) {
	return m.MetadataManager.GetDomain(&GetDomainRequest{
		ID:   id,
		Name: name,
	})
}
