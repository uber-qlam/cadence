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
	sqlMetadataManager struct {
		db *sqlx.DB
	}
)

const (
	domainsSchema = `
`

	templateCreateDomainSqlQuery = `INSERT INTO domains (
		id,
		retention, emit_metric,
		config_version,
		name, 
		status, 
		description, 
		owner_email,
		failover_version, is_global_domain,
		active_cluster_name, clusters
		)
		VALUES(
		:domain_info_id,
		:domain_config_retention, :domain_config_emit_metric,
		:config_version,
		:domain_info_name, 
		:domain_info_status, 
		:domain_info_description, 
		:domain_info_owner_email,
		:failover_version, :is_global_domain,
		:domain_replication_config_active_cluster_name, :domain_replication_config_clusters
		)`

	templateGetDomainByIdSqlQuery = `SELECT * FROM domains WHERE id = :id`
	templateGetDomainByNameSqlQuery = `SELECT * FROM domains WHERE name = :name`
)

func (m *sqlMetadataManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlMetadataManager) CreateDomain(request *CreateDomainRequest) (*CreateDomainResponse, error) {
	tx, err := m.db.Beginx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Disallow creating more than one domain with the same name, even if the UUID is different.
	_, err = m.GetDomain(&GetDomainRequest{Name: request.Info.Name})
	switch err.(type) {
	case *workflow.EntityNotExistsError:
		// Domain does not already exist. Create it.

		if _, err := tx.NamedExec(templateCreateDomainSqlQuery, &FlatCreateDomainRequest{
			DomainInfo:   *(request.Info),
			DomainConfig: *(request.Config),
			// TODO Extracting the fields from DomainReplicationConfig since we don't currently support
			// TODO DomainReplicationConfig.Clusters

			//DomainReplicationConfig: *(request.ReplicationConfig),
			ActiveClusterName: request.ReplicationConfig.ActiveClusterName,
			Clusters:          false,

			// TODO

			IsGlobalDomain:  request.IsGlobalDomain,
			ConfigVersion:   request.ConfigVersion,
			FailoverVersion: request.FailoverVersion,
		}); err != nil {
			print(err.Error())
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateDomain operation failed. Inserting into domains table. Error: %v", err),
			}
		}

		tx.Commit()
		return &CreateDomainResponse{ID: request.Info.ID}, nil

	case nil:
		// The domain already exists.
		return nil, &workflow.DomainAlreadyExistsError{
			Message: fmt.Sprintf("Domain already exists.  DomainId: %v", request.Info.ID),
		}

	default:
		print("default")
		print(err.Error())
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf(
					"CreateDomain operation failed. Could not check if domain already existed. Error: %v", err),
		}
	}
}

func (m *sqlMetadataManager) GetDomain(request *GetDomainRequest) (*GetDomainResponse, error) {
	var rows *sqlx.Rows
	var err error

	if len(request.Name) > 0 {
		rows, err = m.db.NamedQuery(templateGetDomainByNameSqlQuery, map[string]interface{}{
			"name": request.Name,
		})
	} else if len(request.ID) > 0 {
		rows, err = m.db.NamedQuery(templateGetDomainByIdSqlQuery, map[string]interface{}{
			"id": request.ID,
		})
	}

	if err != nil {
		return nil, err
	}

	for rows.Next() {
		// Since IDs/names are unique, there should only be one row as a result, and we should be able to return.

		result := make(map[string]interface{})
		err2 := rows.MapScan(result)
		if err2 != nil {
			return nil, err2
		}
		// Done with the rows, since there was only one.
		rows.Close()

		// int to bool conversions
		int2bool := func(key string) bool {
			if result[key].(int64) == 1 {
				return true
			}
			return false
		}

		return &GetDomainResponse{
			Info: &DomainInfo{
				ID:   string(result["id"].([]byte)),
				Name: string(result["name"].([]byte)),
				// All integer types are scanned as int64, regardless of the table schema
				// https://github.com/go-sql-driver/mysql/issues/549
				Status:      int(result["status"].(int64)),
				Description: string(result["description"].([]byte)),
				OwnerEmail:  string(result["owner_email"].([]byte)),
			},
			Config: &DomainConfig{
				Retention:  int32(result["retention"].(int64)),
				EmitMetric: int2bool("emit_metric"),
			},
			ReplicationConfig: &DomainReplicationConfig{
				ActiveClusterName: GetOrUseDefaultActiveCluster("active",
					string(result["active_cluster_name"].([]byte))), // TODO TO BE IMPLEMENTED
				Clusters: []*ClusterReplicationConfig{
					&ClusterReplicationConfig{
						ClusterName: "active",
					},
				}, // TODO TO BE IMPLEMENTED
			},
			IsGlobalDomain:  int2bool("is_global_domain"),
			ConfigVersion:   result["config_version"].(int64),
			FailoverVersion: result["failover_version"].(int64),
		}, nil
	}

	// We did not return in the above for-loop because there were no rows.
	identity := request.Name
	if len(request.ID) > 0 {
		identity = request.ID
	}

	return nil, &workflow.EntityNotExistsError{
		Message: fmt.Sprintf("Domain %s does not exist.", identity),
	}
}

func (m *sqlMetadataManager) UpdateDomain(request *UpdateDomainRequest) error {
	panic("implement me")
}

func (m *sqlMetadataManager) DeleteDomain(request *DeleteDomainRequest) error {
	panic("implement me")
}

func (m *sqlMetadataManager) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	panic("implement me")
}

// NewMysqlMetadataPersistence creates an instance of sqlMetadataManager
func NewMysqlMetadataPersistence(username, password, host, port, dbName string) (MetadataManager, error) {
	var db, err = sqlx.Connect("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, host, port, dbName))
	if err != nil {
		return nil, err
	}

	return &sqlMetadataManager{
		db: db,
	}, nil
}
