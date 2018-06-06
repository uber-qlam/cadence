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

	"bytes"
	"encoding/gob"
	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	sqlMetadataManager struct {
		db *sqlx.DB
	}
)

const (
	templateCreateDomainSqlQuery = `INSERT INTO domains (
		id,
		retention, 
		emit_metric,
		config_version,
		name, 
		status, 
		description, 
		owner_email,
		failover_version, 
		is_global_domain,
		active_cluster_name, 
		clusters
		)
		VALUES(
		:domain_info_id,
		:domain_config_retention, 
		:domain_config_emit_metric,
		:config_version,
		:domain_info_name, 
		:domain_info_status, 
		:domain_info_description, 
		:domain_info_owner_email,
		:failover_version, 
		:is_global_domain,
		:domain_replication_config_active_cluster_name, 
		:domain_replication_config_clusters
		)`

	getDomainPart = `SELECT
		id,
		retention, 
		emit_metric,
		config_version,
		name, 
		status, 
		description, 
		owner_email,
		failover_version, 
		is_global_domain,
		active_cluster_name, 
		clusters,
		db_version
FROM domains
`
	templateGetDomainByIdSqlQuery = getDomainPart +
		`WHERE id = :id`
	templateGetDomainByNameSqlQuery = getDomainPart +
		`WHERE name = :name`

	updateDomainPart = `UPDATE domains
SET
		id = :domain_info_id,
		retention = :domain_config_retention, 
		emit_metric = :domain_config_emit_metric,
		config_version = :config_version,
		name = :domain_info_name, 
		status = :domain_info_status, 
		description = :domain_info_description, 
		owner_email = :domain_info_owner_email,
		failover_version = :failover_version, 
		active_cluster_name = :domain_replication_config_active_cluster_name,  
		clusters = :domain_replication_config_clusters,
		db_version = :next_db_version
WHERE
name = :domain_info_name
AND
`
	templateUpdateDomainWhereCurrentVersionIsNullSqlQuery = updateDomainPart +
		`db_version IS NULL`
	templateUpdateDomainWhereCurrentVersionIsIntSqlQuery = updateDomainPart +
		`db_version = :current_db_version`

	templateDeleteDomainByIdSqlQuery   = `DELETE FROM domains WHERE id = :id`
	templateDeleteDomainByNameSqlQuery = `DELETE FROM domains WHERE name = :name`
)

func (m *sqlMetadataManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func gobSerialize(x interface{}) ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(x)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func gobDeserialize(a []byte, x interface{}) error {
	b := bytes.NewBuffer(a)
	d := gob.NewDecoder(b)
	err := d.Decode(x)
	return err
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

		// Encode request.ReplicationConfig.Clusters
		clusters, err := gobSerialize(serializeClusterConfigs(request.ReplicationConfig.Clusters))
		if err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateDomain operation failed. Failed to encode ReplicationConfig.Clusters. Value: %v", request.ReplicationConfig.Clusters),
			}
		}

		if _, err := tx.NamedExec(templateCreateDomainSqlQuery, &FlatCreateDomainRequest{
			DomainInfo:   *(request.Info),
			DomainConfig: *(request.Config),
			// TODO Extracting the fields from DomainReplicationConfig since we don't currently support
			// TODO DomainReplicationConfig.Clusters

			//DomainReplicationConfig: *(request.ReplicationConfig),
			ActiveClusterName: request.ReplicationConfig.ActiveClusterName,
			Clusters:          clusters,

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
	// TODO cassandraMetaPersistence_test seems to lack a test case for when both ID and name are empty
	// TODO (even though the current Cassandra implementation does handle it)
	// TODO so for now we will not handle it either.

	// CAVEAT! The Cassandra persistence implementation relies on the following behaviour of gocql's scan:
	// if a column entry is NULL, the result of the scan is the type's default value
	// e.g. if dbVersion is NULL, we will get dbVersion 0
	// We have to explicitly implement this behaviour since the result of a scan
	// with go-sql-driver/mysql is that
	// the map[string]interface{} has, for its "dbVersion" key, an interface{}(nil) as the value

	var rows *sqlx.Rows
	var err error

	if len(request.Name) > 0 {
		if len(request.ID) > 0 {
			return nil, &workflow.BadRequestError{
				Message: "GetDomain operation failed.  Both ID and Name specified in request.",
			}
		} else {
			rows, err = m.db.NamedQuery(templateGetDomainByNameSqlQuery, request)
		}
	} else if len(request.ID) > 0 {
		rows, err = m.db.NamedQuery(templateGetDomainByIdSqlQuery, request)
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

		// Deserialize DomainReplicationConfig.Clusters
		var clusters []map[string]interface{}
		gobDeserialize(result["clusters"].([]byte), &clusters)

		// Set dbVersion to 0 if it is currently NULL
		// See caveat detailed above.
		if result["db_version"] == interface{}(nil) {
			result["db_version"] = int64(0)
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
					string(result["active_cluster_name"].([]byte))), // TODO TO BE IMPLEMENTED (get rid of "active" placeholder)
				Clusters: GetOrUseDefaultClusters("active", deserializeClusterConfigs(clusters)), // TODO same
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
	// Caveat. The Cassandra persistence implementation has the following logic for how to fill in
	// the "db version" bindings for the update query:
	// if request.DBVersion <= 0, then (currentVersion, nextVersion) = (nil, 1)
	// else, (currentVersion, nextVersion) = (request.DBVersion, request.DBVersion + 1)

	clusters, err := gobSerialize(serializeClusterConfigs(request.ReplicationConfig.Clusters))
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Failed to encode ReplicationConfig.Clusters. Value: %v", request.ReplicationConfig.Clusters),
		}
	}

	queryToUse := templateUpdateDomainWhereCurrentVersionIsNullSqlQuery
	if request.DBVersion > 0 {
		queryToUse = templateUpdateDomainWhereCurrentVersionIsIntSqlQuery
	}

	_, err = m.db.NamedExec(queryToUse, &FlatUpdateDomainRequest{
		DomainInfo:        *(request.Info),
		DomainConfig:      *(request.Config),
		ActiveClusterName: request.ReplicationConfig.ActiveClusterName,
		Clusters:          clusters,
		ConfigVersion:     request.ConfigVersion,
		FailoverVersion:   request.FailoverVersion,
		CurrentDBVersion:  request.DBVersion,
		NextDBVersion:     request.DBVersion + 1,
	})
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Error %v", err),
		}
	}

	return nil
}

// TODO Find a way to get rid of code repetition without using a type switch

func (m *sqlMetadataManager) DeleteDomain(request *DeleteDomainRequest) error {
	_, err := m.GetDomain(&GetDomainRequest{
		ID: request.ID,
	})
	switch err.(type) {
	case *workflow.EntityNotExistsError:
		return nil
	default:
		if _, err = m.db.NamedExec(templateDeleteDomainByIdSqlQuery, request); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("DeleteDomain operation failed. Error %v", err),
			}
		}
		return nil
	}
}

func (m *sqlMetadataManager) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	_, err := m.GetDomain(&GetDomainRequest{
		Name: request.Name,
	})
	switch err.(type) {
	case *workflow.EntityNotExistsError:
		return nil
	default:
		if _, err = m.db.NamedExec(templateDeleteDomainByNameSqlQuery, request); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("DeleteDomainByName operation failed. Error %v", err),
			}
		}
		return nil
	}
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
