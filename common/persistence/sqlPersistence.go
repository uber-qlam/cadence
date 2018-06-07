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
	"github.com/jmoiron/sqlx"
	"fmt"
)

type(
	sqlPersistence struct {
		db *sqlx.DB
	}
)

const(
	templateInsertIntoExecutionSqlQuery = `INSERT INTO executions (
type
)
VALUES (
:type
)`
)

func NewSqlTaskPersistence(username, password, host, port, dbName string) (TaskManager, error) {
	db, err := sqlx.Connect("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", username, password, host, port, dbName))
	if err != nil {
		return nil, err
	}

	return &sqlPersistence{
		db: db,
	}, nil
}

func (*sqlPersistence) LeaseTaskList(request *LeaseTaskListRequest) (*LeaseTaskListResponse, error) {
	panic("implement me")
}

func (*sqlPersistence) UpdateTaskList(request *UpdateTaskListRequest) (*UpdateTaskListResponse, error) {
	panic("implement me")
}

func (*sqlPersistence) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	panic("implement me")
}

func (*sqlPersistence) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	panic("implement me")
}

func (*sqlPersistence) CompleteTask(request *CompleteTaskRequest) error {
	panic("implement me")
}

func NewSqlWorkflowExecutionPersistence() (ExecutionManager, error) {
	return &sqlPersistence{}, nil
}

func (p *sqlPersistence) Close() {
	if p.db != nil {
		p.Close()
	}
}

func (p *sqlPersistence) CreateWorkflowExecution(request *CreateWorkflowExecutionRequest) (*CreateWorkflowExecutionResponse, error) {
	_, err := p.db.NamedExec(``)

	return &CreateWorkflowExecutionResponse{}, nil
}

func (*sqlPersistence) GetWorkflowExecution(request *GetWorkflowExecutionRequest) (*GetWorkflowExecutionResponse, error) {
	panic("implement me")
}

func (*sqlPersistence) UpdateWorkflowExecution(request *UpdateWorkflowExecutionRequest) error {
	panic("implement me")
}

func (*sqlPersistence) ResetMutableState(request *ResetMutableStateRequest) error {
	panic("implement me")
}

func (*sqlPersistence) DeleteWorkflowExecution(request *DeleteWorkflowExecutionRequest) error {
	panic("implement me")
}

func (*sqlPersistence) GetCurrentExecution(request *GetCurrentExecutionRequest) (*GetCurrentExecutionResponse, error) {
	panic("implement me")
}

func (*sqlPersistence) GetTransferTasks(request *GetTransferTasksRequest) (*GetTransferTasksResponse, error) {
	return &GetTransferTasksResponse{}, nil
}

func (*sqlPersistence) CompleteTransferTask(request *CompleteTransferTaskRequest) error {
	panic("implement me")
}

func (*sqlPersistence) GetReplicationTasks(request *GetReplicationTasksRequest) (*GetReplicationTasksResponse, error) {
	return &GetReplicationTasksResponse{}, nil
}

func (*sqlPersistence) CompleteReplicationTask(request *CompleteReplicationTaskRequest) error {
	panic("implement me")
}

func (*sqlPersistence) GetTimerIndexTasks(request *GetTimerIndexTasksRequest) (*GetTimerIndexTasksResponse, error) {
	panic("implement me")
}

func (*sqlPersistence) CompleteTimerTask(request *CompleteTimerTaskRequest) error {
	panic("implement me")
}
