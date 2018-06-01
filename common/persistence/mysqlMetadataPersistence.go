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
	"fmt"
	_ "github.com/go-sql-driver/mysql" // MySQL driver
	"github.com/jmoiron/sqlx"

	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	mysqlMetadataManager struct {
		db *sqlx.DB
	}
)

const (
	templateCreateDomainSqlQuery = `INSERT INTO domains (` +
		`id,` +
		`retention, emit_metric,` +
		`config_version,` +
		`name, status, description, owner_email,` +
		`failover_version, is_global_domain,` +
		`active_cluster_name, clusters` +
		`)` +
		`VALUES(` +
		`:id,` +
		`:retention, :emit_metric,` +
		`:config_version,` +
		`:name, :status, :description, :owner_email,` +
		`:failover_version, :is_global_domain,` +
		`:active_cluster_name, :clusters` +
		`)`
)

func (m *mysqlMetadataManager) Close() {
	panic("implement me")
}

func (m *mysqlMetadataManager) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	tx, err1 := m.db.Beginx()
	if err1 != nil {
		return nil, err1
	}

	if _, err2 := tx.NamedExec(templateCreateDomainSqlQuery, map[string]interface{}{
		"id":                  request.Info.ID,
		"retention":           request.Config.Retention,
		"emit_metric":         request.Config.EmitMetric,
		"config_version":      request.ConfigVersion,
		"name":                request.Info.Name,
		"status":              request.Info.Status,
		"description":         request.Info.Description,
		"owner_email":         request.Info.OwnerEmail,
		"failover_version":    request.FailoverVersion,
		"is_global_domain":    request.IsGlobalDomain,
		"active_cluster_name": request.ReplicationConfig.ActiveClusterName,
		"clusters":            false, // TO BE IMPLEMENTED
	}); err2 != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Inserting into domains table. Error: %v", err2),
		}
	}

	if err3 := tx.Commit(); err3 != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateDomain operation failed. Committing. Error: %v", err3),
		}
	}

	return &CreateDomainResponse{ID: request.Info.ID}, nil
}

func (m *mysqlMetadataManager) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	return nil, nil
	//return &GetDomainResponse{
	//	Info: request // this Info object has to go from CreateDomain -> ... -> and back out as a response
	//}, nil
}

func (m *mysqlMetadataManager) UpdateDomain(request *UpdateDomainRequest) error {
	panic("implement me")
}

func (m *mysqlMetadataManager) DeleteDomain(request *DeleteDomainRequest) error {
	panic("implement me")
}

func (m *mysqlMetadataManager) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	panic("implement me")
}

// NewMysqlMetadataPersistence creates an instance of mysqlMetadataManager
func NewMysqlMetadataPersistence() (MetadataManager, error) {
	var db, err = sqlx.Connect("mysql", "uber:uber@tcp(127.0.0.1:3306)/catalyst_test")
	if err != nil {
		return nil, err
	}

	return &mysqlMetadataManager{
		db: db,
	}, nil
}
