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

	FlatCommon struct {
		DomainInfo
		DomainConfig
		ActiveClusterName string `db:"active_cluster_name"`
		Clusters          []byte `db:"clusters"`
		ConfigVersion     int64  `db:"config_version"`
		FailoverVersion   int64  `db:"failover_version"`
	}

	FlatUpdateDomainRequest struct {
		FlatCommon
		FailoverNotificationVersion int64 `db:"failover_notification_version"`
		NotificationVersion         int64 `db:"notification_version"`
	}

	flatCreateDomainRequest struct {
		FlatCommon
		IsGlobalDomain bool `db:"is_global_domain"`
	}

	flatGetDomainResponse struct {
		FlatUpdateDomainRequest
		IsGlobalDomain bool `db:"is_global_domain"`
	}

	domainRow = flatGetDomainResponse
)

func (m *sqlMetadataManager) ListDomain(request *ListDomainRequest) (*ListDomainResponse, error) {

	var rows []*domainRow

	m.db.Select(&rows, templateListDomainSqlQuery)

	var domains []*GetDomainResponse
	for _, row := range rows {
		resp, err := domainRowToGetDomainResponse(row)
		if err != nil {
			return &ListDomainResponse{
				Domains: domains,
			}, err
		}
		domains = append(domains, resp)
	}

	return &ListDomainResponse{
		Domains: domains,
	}, nil
}

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
		notification_version,
		failover_notification_version
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
		:notification_version,
		:failover_notification_version
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
		notification_version,
		failover_notification_version
FROM domains
`
	templateGetDomainByIdSqlQuery = getDomainPart +
		`WHERE id = :id`
	templateGetDomainByNameSqlQuery = getDomainPart +
		`WHERE name = :name`

	templateUpdateDomainSqlQuery = `UPDATE domains
SET
		retention_days = :retention_days, 
		emit_metric = :emit_metric,
		config_version = :config_version,
		status = :status, 
		description = :description, 
		owner_email = :owner_email,
		failover_version = :failover_version, 
		active_cluster_name = :active_cluster_name,  
		clusters = :clusters,
		notification_version = :notification_version,
		failover_notification_version = :failover_notification_version
WHERE
name = :name AND
id = :id`

	templateDeleteDomainByIdSqlQuery   = `DELETE FROM domains WHERE id = :id`
	templateDeleteDomainByNameSqlQuery = `DELETE FROM domains WHERE name = :name`

	templateListDomainSqlQuery  = getDomainPart
	templateGetMetadataSqlQuery = `SELECT notification_version
FROM domains
WHERE name = :name`
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

		metadata, err := m.GetMetadata()
		if err != nil {
			return nil, err
		}

		if _, err := tx.NamedExec(templateCreateDomainSqlQuery, &domainRow{
			FlatUpdateDomainRequest: FlatUpdateDomainRequest{
				FlatCommon: FlatCommon{
					DomainInfo:   *(request.Info),
					DomainConfig: *(request.Config),
					// TODO Extracting the fields from DomainReplicationConfig since we don't currently support
					// TODO DomainReplicationConfig.Clusters

					//DomainReplicationConfig: *(request.ReplicationConfig),
					ActiveClusterName: request.ReplicationConfig.ActiveClusterName,
					Clusters:          clusters,

					// TODO

					ConfigVersion:   request.ConfigVersion,
					FailoverVersion: request.FailoverVersion,
				},

				NotificationVersion:         metadata.NotificationVersion,
				FailoverNotificationVersion: initialFailoverNotificationVersion,
			},

			IsGlobalDomain: request.IsGlobalDomain,
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
	var err error
	var stmt *sqlx.NamedStmt

	if len(request.Name) > 0 {
		if len(request.ID) > 0 {
			return nil, &workflow.BadRequestError{
				Message: "GetDomain operation failed.  Both ID and Name specified in request.",
			}
		}
		stmt, err = m.db.PrepareNamed(templateGetDomainByNameSqlQuery)
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
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("GetDomain operation failed. Error %v", err),
			}

		}
	}

	return domainRowToGetDomainResponse(&result)
}

func domainRowToGetDomainResponse(result *domainRow) (*GetDomainResponse, error) {
	var clusters []map[string]interface{}

	if err := gobDeserialize(result.Clusters, &clusters); err != nil {
		return nil, err
	}

	return &GetDomainResponse{
		Info:   &result.DomainInfo,
		Config: &result.DomainConfig,
		ReplicationConfig: &DomainReplicationConfig{
			ActiveClusterName: GetOrUseDefaultActiveCluster("active",
				result.ActiveClusterName), // TODO TO BE IMPLEMENTED (get rid of "active" placeholder)
			Clusters: GetOrUseDefaultClusters("active", deserializeClusterConfigs(clusters)), // TODO same
		},
		IsGlobalDomain:              result.IsGlobalDomain,
		FailoverVersion:             result.FailoverVersion,
		ConfigVersion:               result.ConfigVersion,
		NotificationVersion:         result.NotificationVersion,
		FailoverNotificationVersion: result.FailoverNotificationVersion,
	}, nil
}

func (m *sqlMetadataManager) UpdateDomain(request *UpdateDomainRequest) error {
	clusters, err := gobSerialize(serializeClusterConfigs(request.ReplicationConfig.Clusters))
	if err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("UpdateDomain operation failed. Failed to encode ReplicationConfig.Clusters. Value: %v", request.ReplicationConfig.Clusters),
		}
	}

	//queryToUse := templateUpdateDomainWhereCurrentVersionIsNullSqlQuery
	//if request.NotificationVersion > 0 {
	//	queryToUse = templateUpdateDomainWhereCurrentVersionIsIntSqlQuery
	//}

	_, err = m.db.NamedExec(templateUpdateDomainSqlQuery, &FlatUpdateDomainRequest{
		FlatCommon: FlatCommon{
			DomainInfo:   *(request.Info),
			DomainConfig: *(request.Config),

			ActiveClusterName: request.ReplicationConfig.ActiveClusterName,
			Clusters:          clusters,
			ConfigVersion:     request.ConfigVersion,
			FailoverVersion:   request.FailoverVersion,
		},
		FailoverNotificationVersion: request.FailoverNotificationVersion,
		NotificationVersion:         request.NotificationVersion,
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

func (m *sqlMetadataManager) GetMetadata() (*GetMetadataResponse, error) {
	stmt, err := m.db.PrepareNamed(templateGetMetadataSqlQuery)
	if err != nil {
		return nil, err
	}

	var notificationVersion int64
	var result struct {
		NotificationVersion int64 `db:"notification_version"`
	}
	err = stmt.Get(&result, struct {
		Name string `db:"name"`
	}{domainMetadataRecordName})
	if err != nil {
		if err == sql.ErrNoRows {
			return &GetMetadataResponse{NotificationVersion: 0}, nil
		}
		return nil, err
	}
	return &GetMetadataResponse{NotificationVersion: notificationVersion}, nil
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
