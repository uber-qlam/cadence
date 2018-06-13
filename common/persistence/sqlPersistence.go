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

	workflow "github.com/uber/cadence/.gen/go/shared"

	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type (
	sqlPersistence struct {
		db *sqlx.DB
	}

	FlatCreateWorkflowExecutionRequest struct {
		DomainId string `db:"domain_id"`
	}
)

const (
	templateInsertIntoExecutionsSqlQuery = `INSERT INTO executions (
domain_id
)
VALUES(
:domain_id
)
`

	templateSelectFromExecutionsSqlQuery = `SELECT (
domain_id
)
FROM
executions
`
)

func NewSqlWorkflowExecutionPersistence(sqlDsn string) (ExecutionManager, error) {
	db, err := sqlx.Connect("mysql", sqlDsn)
	if err != nil {
		return nil, err
	}

	return &sqlPersistence{
		db: db,
	}, nil
}

func (s *sqlPersistence) Close() {
	panic("implement me")
}

func (s *sqlPersistence) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	var row struct {
		DomainId string `db:"domain_id"`
	}
	err := s.db.Get(&row, templateSelectFromExecutionsSqlQuery)
	if err != nil {
		if err == sql.ErrNoRows {
			_, err := s.db.NamedExec(templateInsertIntoExecutionsSqlQuery,
				&FlatCreateWorkflowExecutionRequest{
					DomainId: request.DomainID,
				})
			if err != nil {
				return nil, &workflow.InternalServiceError{
					Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
				}
			}

			return &CreateWorkflowExecutionResponse{}, nil
		} else {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("CreateWorkflowExecution operation failed. Error: %v", err),
			}
		}
	} else {
		// The query returned a row, which means the execution already exists.
		return nil, &WorkflowExecutionAlreadyStartedError{
			Msg: fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v, rangeId: %v, columns: (%v)",
				"foo",
				"foo",
				"foo",
				"foo"),
		}
	}

}

func (s *sqlPersistence) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (s *sqlPersistence) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	panic("implement me")
}

func (s *sqlPersistence) ResetMutableState(request *ResetMutableStateRequest) error {
	panic("implement me")
}

func (s *sqlPersistence) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	panic("implement me")
}

func (s *sqlPersistence) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error) {
	panic("implement me")
}

func (s *sqlPersistence) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	return &GetTransferTasksResponse{}, nil
}

func (s *sqlPersistence) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	panic("implement me")
}

func (s *sqlPersistence) GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error) {
	return &GetReplicationTasksResponse{}, nil
}

func (s *sqlPersistence) CompleteReplicationTask(request *CompleteReplicationTaskRequest) error {
	panic("implement me")
}

func (s *sqlPersistence) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	panic("implement me")
}

func (s *sqlPersistence) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	panic("implement me")
}
