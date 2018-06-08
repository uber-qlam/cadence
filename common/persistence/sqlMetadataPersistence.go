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
	"database/sql"
	"encoding/gob"
	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	sqlMetadataManager struct {
		db *sqlx.DB
	}

	// domainRow is a flattened version of CreateDomainRequest
	// but it also contains a DBVersion row.
	domainRow struct {
		DomainInfo
		DomainConfig
		ActiveClusterName string `db:"active_cluster_name"`
		Clusters          []byte `db:"clusters"`
		IsGlobalDomain    bool   `db:"is_global_domain"`
		ConfigVersion     int64  `db:"config_version"`
		FailoverVersion   int64  `db:"failover_version"`
		DBVersion         sql.NullInt64  `db:"db_version"`
	}

	// flatUpdateDomainRequest is a flattened version of UpdateDomainRequest
	flatUpdateDomainRequest struct {
		DomainInfo
		DomainConfig
		ActiveClusterName string `db:"active_cluster_name"`
		Clusters          []byte `db:"clusters"`
		ConfigVersion     int64  `db:"config_version"`
		FailoverVersion   int64  `db:"failover_version"`
		CurrentDBVersion  int64  `db:"current_db_version"`
		NextDBVersion     int64  `db:"next_db_version"`
	}
)

const (
	templateCreateDomainSqlQuery = `INSERT INTO domains (
		id,
		retention_days, 
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
		)
		VALUES(
		:id,
		:retention_days, 
		:emit_metric,
		:config_version,
		:name, 
		:status, 
		:description, 
		:owner_email,
		:failover_version, 
		:is_global_domain,
		:active_cluster_name, 
		:clusters,
:db_version
		)`

	getDomainPart = `SELECT
		id,
		retention_days, 
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
		id = :id,
		retention_days = :retention_days, 
		emit_metric = :emit_metric,
		config_version = :config_version,
		name = :name, 
		status = :status, 
		description = :description, 
		owner_email = :owner_email,
		failover_version = :failover_version, 
		active_cluster_name = :active_cluster_name,  
		clusters = :clusters,
		db_version = :next_db_version
WHERE
name = :name
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
	resp, err := m.GetDomain(&GetDomainRequest{Name: request.Info.Name})
	if err == nil {
		// The domain already exists.
		return nil, &workflow.DomainAlreadyExistsError{
			Message: fmt.Sprintf("Domain already exists.  DomainId: %v", resp.Info.ID),
		}
	}

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

		if _, err := tx.NamedExec(templateCreateDomainSqlQuery, &domainRow{
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

	var err error
	var stmt *sqlx.NamedStmt

	if len(request.Name) > 0 {
		if len(request.ID) > 0 {
			return nil, &workflow.BadRequestError{
				Message: "GetDomain operation failed.  Both ID and Name specified in request.",
			}
		} else {
			stmt, err = m.db.PrepareNamed(templateGetDomainByNameSqlQuery)
		}
	} else if len(request.ID) > 0 {
		stmt, err = m.db.PrepareNamed(templateGetDomainByIdSqlQuery)
	} else {
		return nil, &workflow.BadRequestError{
			Message: "GetDomain operation failed.  Both ID and Name are empty.",
		}
	}

	if err != nil {
		return nil, err
	}

	var result domainRow
	if err = stmt.Get(&result, request); err != nil {
		switch err {
		case sql.ErrNoRows:
			// We did not return in the above for-loop because there were no rows.
			identity := request.Name
			if len(request.ID) > 0 {
				identity = request.ID
			}

			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Domain %s does not exist.", identity),
			}
		default:
			panic(1)
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetDomain operation failed. Error %v", err),
			}

		}

	}

	var clusters []map[string]interface{}
	err = gobDeserialize(result.Clusters, &clusters)

	var dbVersion int64 = 0
	if result.DBVersion.Valid {
		dbVersion = result.DBVersion.Int64
	}

	return &GetDomainResponse{
		Info:   &result.DomainInfo,
		Config: &result.DomainConfig,
		ReplicationConfig: &DomainReplicationConfig{
			ActiveClusterName: GetOrUseDefaultActiveCluster("active",
				result.ActiveClusterName), // TODO TO BE IMPLEMENTED (get rid of "active" placeholder)
			Clusters: GetOrUseDefaultClusters("active", deserializeClusterConfigs(clusters)), // TODO same
		},
		IsGlobalDomain:  result.IsGlobalDomain,
		DBVersion:       dbVersion,
		FailoverVersion: result.FailoverVersion,
		ConfigVersion:   result.ConfigVersion,
	}, nil

	// int to bool conversions
	//int2bool := func(key string) bool {
	//	if result[key].(int64) == 1 {
	//		return true
	//	}
	//	return false
	//}

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

	_, err = m.db.NamedExec(queryToUse, &flatUpdateDomainRequest{
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
	if _, err := m.db.NamedExec(templateDeleteDomainByIdSqlQuery, request); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("DeleteDomain operation failed. Error %v", err),
			}
		}
		return nil
}

func (m *sqlMetadataManager) DeleteDomainByName(request *DeleteDomainByNameRequest) error {
	if _, err := m.db.NamedExec(templateDeleteDomainByNameSqlQuery, request); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("DeleteDomainByName operation failed. Error %v", err),
			}
		}
		return nil
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
