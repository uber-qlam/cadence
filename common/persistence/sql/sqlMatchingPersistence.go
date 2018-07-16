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

package sql

import (
	"database/sql"
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"

	"github.com/jmoiron/sqlx"
	"github.com/uber/cadence/common"
)

type (
	sqlMatchingManager struct {
		db *sqlx.DB
		shardID      int
	}

	FlatCreateWorkflowExecutionRequest struct {
		DomainID string `db:"domain_id"`
		WorkflowID string `db:"workflow_id"`
		RunID string `db:"run_id"`
		ParentDomainID *string `db:"parent_domain_id"`
		ParentWorkflowID *string`db:"parent_workflow_id"`
		ParentRunID *string `db:"parent_run_id"`
		InitiatedID *int64 `db:"initiated_id"`
		TaskList string `db:"task_list"`
		WorkflowTypeName string `db:"workflow_type_name"`
		WorkflowTimeoutSeconds int64 `db:"workflow_timeout_seconds"`
		DecisionTimeoutValue int64 `db:"decision_timeout_value"`
		ExecutionContext []byte `db:"execution_context"`
		NextEventID int64 `db:"next_event_id"`
		LastProcessedEvent int64 `db:"last_processed_event"`
		// maybe i don't need this.
	}

	executionRow struct {
		DomainID string `db:"domain_id"`
		WorkflowID string `db:"workflow_id"`
		RunID string `db:"run_id"`
		ParentDomainID *string `db:"parent_domain_id"`
		ParentWorkflowID *string`db:"parent_workflow_id"`
		ParentRunID *string `db:"parent_run_id"`
		InitiatedID *int64 `db:"initiated_id"`
		CompletionEvent *[]byte `db:"completion_event"`
		TaskList string `db:"task_list"`
		WorkflowTypeName string `db:"workflow_type_name"`
		WorkflowTimeoutSeconds int64 `db:"workflow_timeout_seconds"`
		DecisionTaskTimeoutMinutes int64 `db:"decision_task_timeout_minutes"`
		ExecutionContext []byte `db:"execution_context"`
		State int64 `db:"state"`
		CloseStatus int64 `db:"close_status"`
		StartVersion int64 `db:"start_version"`
		LastFirstEventId int64 `db:"last_first_event_id"`
		NextEventId int64 `db:"next_event_id"`
		LastProcessedEvent int64 `db:"last_processed_event"`
		StartTime time.Time `db:"start_time"`
		LastUpdatedTime time.Time `db:"last_updated_time"`
		CreateRequestId string `db:"create_request_id"`
		DecisionVersion int64 `db:"decision_version"`
		DecisionScheduleID int64 `db:"decision_schedule_id"`
		DecisionStartedID int64 `db:"decision_started_id"`
		DecisionRequestID string `db:"decision_request_id"`
		DecisionTimeout int64 `db:"decision_timeout"`
		DecisionAttempt int64 `db:"decision_attempt"`
		DecisionTimestamp int64 `db:"decision_timestamp"`
		CancelRequested *int64 `db:"cancel_requested"`
		CancelRequestID *string `db:"cancel_request_id"`
		StickyTaskList string `db:"sticky_task_list"`
		StickyScheduleToStartTimeout int64 `db:"sticky_schedule_to_start_timeout"`
		ClientLibraryVersion string `db:"client_library_version"`
		ClientFeatureVersion string `db:"client_feature_version"`
		ClientImpl string `db:"client_impl"`
		ShardID string `db:"shard_id"`
	}
)

const (
	executionsNonNullableColumns = `shard_id,
domain_id, 
workflow_id, 
run_id, 
task_list, 
workflow_type_name, 
workflow_timeout_seconds,
decision_task_timeout_minutes,
state,
close_status,
start_version,
last_first_event_id,
next_event_id,
last_processed_event,
start_time,
last_updated_time,
create_request_id,
decision_version,
decision_schedule_id,
decision_started_id,
decision_timeout,
decision_attempt,
decision_timestamp,
sticky_task_list,
sticky_schedule_to_start_timeout,
client_library_version,
client_feature_version,
client_impl`


	executionsNonNullableColumnsTags = `:shard_id,
:domain_id,
:workflow_id,
:run_id,
:task_list,
:workflow_type_name,
:workflow_timeout_seconds,
:decision_task_timeout_minutes,
:state,
:close_status,
:start_version,
:last_first_event_id,
:next_event_id,
:last_processed_event,
:start_time,
:last_updated_time,
:create_request_id,
:decision_version,
:decision_schedule_id,
:decision_started_id,
:decision_timeout,
:decision_attempt,
:decision_timestamp,
:sticky_task_list,
:sticky_schedule_to_start_timeout,
:client_library_version,
:client_feature_version,
:client_impl`

	executionsBlobColumns = `completion_event,
execution_context`

	executionsBlobColumnsTags = `:completion_event,
:execution_context`

// Excluding completion_event
	executionsNonblobParentColumns = `parent_domain_id,
parent_workflow_id,
parent_run_id,
initiated_id`

	executionsNonblobParentColumnsTags = `:parent_domain_id,
:parent_workflow_id,
:parent_run_id,
:initiated_id`

	executionsCancelColumns = `cancel_requested,
cancel_request_id`

	executionsCancelColumnsTags = `:cancel_requested,
:cancel_request_id`

	createExecutionWithNoParentSQLQuery = `INSERT INTO executions 
(` + executionsNonNullableColumns +
`,
execution_context,
cancel_requested,
cancel_request_id)
VALUES
(` + executionsNonNullableColumnsTags + `,
:execution_context,
:cancel_requested,
:cancel_request_id)
`

	getExecutionSQLQuery = `SELECT ` +
		executionsNonNullableColumns + "," +
		executionsBlobColumns + "," +
		executionsNonblobParentColumns + "," +
		executionsCancelColumns +
	 ` FROM executions WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id`

	getTransferTasksSQLQuery = `SELECT
domain_id,
workflow_id,
run_id,
task_id,
type,
target_domain_id,
target_workflow_id,
target_run_id,
target_child_workflow_only,
task_list,
schedule_id,
version
FROM transfer_tasks
WHERE
task_id >= :min_read_level AND
task_id <= :max_read_level
`

)

func (m *sqlMatchingManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func (m *sqlMatchingManager) CreateWorkflowExecution(request *persistence.CreateWorkflowExecutionRequest) (*persistence.CreateWorkflowExecutionResponse, error) {
	/*
	make a transaction
	check for a parent
	check for continueasnew, update the curret exec or create one for this new workflow
	create a workflow with/without cross dc
	 */

	 if resp, err := m.GetWorkflowExecution(&persistence.GetWorkflowExecutionRequest{request.DomainID,
	 request.Execution}); err == nil {
	 	// Workflow already exists.
	 	startVersion := common.EmptyVersion
	 	if resp.State.ReplicationState != nil {
	 		startVersion = resp.State.ReplicationState.StartVersion
		}

	 	return nil, &persistence.WorkflowExecutionAlreadyStartedError{
	 		Msg: fmt.Sprintf("Workflow execution already running. WorkflowId: %v, RunId: %v", resp.State.ExecutionInfo.WorkflowID, resp.State.ExecutionInfo.RunID),
	 		StartRequestID: resp.State.ExecutionInfo.CreateRequestID,
	 		RunID: resp.State.ExecutionInfo.RunID,
	 		State: resp.State.ExecutionInfo.State,
	 		CloseStatus: resp.State.ExecutionInfo.CloseStatus,
	 		StartVersion: startVersion,
		}
	 }

	tx, err := m.db.Beginx()
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution failed. Failed to start transaction. Error: %v", err),
		}
	}
	defer tx.Rollback()

	nowTimestamp := time.Now()
	_, err = m.db.NamedExec(createExecutionWithNoParentSQLQuery, executionRow{
		DomainID: request.DomainID,
		WorkflowID: *request.Execution.WorkflowId,
		RunID: *request.Execution.RunId,
		TaskList: request.TaskList,
		WorkflowTypeName: request.WorkflowTypeName,
		WorkflowTimeoutSeconds: int64(request.WorkflowTimeout),
		DecisionTaskTimeoutMinutes: int64(request.DecisionTimeoutValue),
		State: persistence.WorkflowStateRunning,
		CloseStatus: persistence.WorkflowCloseStatusNone,
		LastFirstEventId: common.FirstEventID,
		NextEventId: request.NextEventID,
		LastProcessedEvent: request.LastProcessedEvent,
		StartTime: nowTimestamp,
		LastUpdatedTime: nowTimestamp,
		CreateRequestId: request.RequestID,
		DecisionVersion: int64(request.DecisionVersion),
		DecisionScheduleID: int64(request.DecisionScheduleID),
		DecisionStartedID: int64(request.DecisionStartedID),
		DecisionTimeout: int64(request.DecisionStartToCloseTimeout),
		DecisionAttempt: 0,
		DecisionTimestamp: 0,
		StickyTaskList: "",
		StickyScheduleToStartTimeout: 0,
		ClientLibraryVersion: "",
		ClientFeatureVersion: "",
		ClientImpl: "",
	})
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution failed. Failed to insert into executions table. Error: %v", err),
		}
	}

	if err := lockShard(tx, m.shardID); err != nil {
		return nil, err
	}

	if err := updateShardLease(tx, m.shardID, request.RangeID); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("CreateWorkflowExecution failed. Failed to commit transaction. Error: %v", err),
		}
	}

	return &persistence.CreateWorkflowExecutionResponse{

	}, nil
}

func (m *sqlMatchingManager) GetWorkflowExecution(request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
	stmt, err := m.db.PrepareNamed(getExecutionSQLQuery)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution operation failed. Failed to prepare named statement. Error: %v", err),
		}
	}

	var result executionRow
	if err := stmt.Get(&result, struct{
		ShardId string `db:"shard_id"`
		DomainId string `db:"domain_id"`
		WorkflowId string `db:"workflow_id"`
		RunId string `db:"run_id"`
	}{"",
	request.DomainID,
	*request.Execution.WorkflowId,
	*request.Execution.RunId}); err != nil {
		if err == sql.ErrNoRows {
			return nil, &workflow.EntityNotExistsError{
				Message: fmt.Sprintf("Workflow execution not found.  WorkflowId: %v, RunId: %v",
					*request.Execution.WorkflowId,
					*request.Execution.RunId),
			}
		}
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecution failed. Error: %v", err),
		}
	}

	var state persistence.WorkflowMutableState
	state.ExecutionInfo = &persistence.WorkflowExecutionInfo{
		DomainID: result.DomainID,
		WorkflowID: result.WorkflowID,
		RunID: result.RunID,
		TaskList: result.TaskList,
		WorkflowTypeName: result.WorkflowTypeName,
		WorkflowTimeout: int32(result.WorkflowTimeoutSeconds),
		DecisionTimeoutValue: int32(result.DecisionTaskTimeoutMinutes),
		ExecutionContext: result.ExecutionContext,
		State                       : int(result.State),
		CloseStatus                  : int(result.CloseStatus),
		LastFirstEventID             : result.LastFirstEventId,
		NextEventID                  : result.NextEventId,
		LastProcessedEvent          : result.LastProcessedEvent,
		StartTimestamp               : result.StartTime,
		LastUpdatedTimestamp       : result.LastUpdatedTime,
		CreateRequestID              :result.CreateRequestId,
		DecisionVersion              :result.DecisionVersion,
		DecisionScheduleID           : result.DecisionScheduleID,
		DecisionStartedID            : result.DecisionStartedID,
		DecisionRequestID            : result.DecisionRequestID,
		DecisionTimeout              : int32(result.DecisionTimeout),
		DecisionAttempt              : result.DecisionAttempt,
		DecisionTimestamp            : result.DecisionTimestamp,
		StickyTaskList               : result.StickyTaskList,
		StickyScheduleToStartTimeout : int32(result.StickyScheduleToStartTimeout),
		ClientLibraryVersion         : result.ClientLibraryVersion,
		ClientFeatureVersion         : result.ClientFeatureVersion,
		ClientImpl                   : result.ClientImpl,
	}

	if result.ParentDomainID != nil {
		state.ExecutionInfo.ParentDomainID = *result.ParentDomainID
	state.ExecutionInfo.ParentWorkflowID = *result.ParentWorkflowID
		state.ExecutionInfo.ParentRunID = *result.ParentRunID
		state.ExecutionInfo.InitiatedID = *result.InitiatedID
		state.ExecutionInfo.CompletionEvent=  *result.CompletionEvent
	}

	if result.CancelRequested != nil && (*result.CancelRequested != 0){
		state.ExecutionInfo.CancelRequested = true
		state.ExecutionInfo.CancelRequestID = *result.CancelRequestID
	}


	return &persistence.GetWorkflowExecutionResponse{State: &state}, nil
}

func (*sqlMatchingManager) UpdateWorkflowExecution(request *persistence.UpdateWorkflowExecutionRequest) error {
	panic("implement me")
}

func (*sqlMatchingManager) ResetMutableState(request *persistence.ResetMutableStateRequest) error {
	panic("implement me")
}

func (*sqlMatchingManager) DeleteWorkflowExecution(request *persistence.DeleteWorkflowExecutionRequest) error {
	panic("implement me")
}

func (*sqlMatchingManager) GetCurrentExecution(request *persistence.GetCurrentExecutionRequest) (*persistence.GetCurrentExecutionResponse, error) {
	return &persistence.GetCurrentExecutionResponse{}, nil
}

func (m *sqlMatchingManager) GetTransferTasks(request *persistence.GetTransferTasksRequest) (*persistence.GetTransferTasksResponse, error) {
	//rows, err := m.db.Queryx(getTransferTasksSQLQuery)
	//if err != nil {
	//	return nil, &workflow.InternalServiceError{
	//		Message: fmt.Sprintf("GetTransferTasks operation failed. Error: %v", err),
	//	}
	//}
	//defer rows.Close()
	//
	//var resp persistence.GetTransferTasksResponse
	//
	//for rows.Next() {
	//	var task persistence.TransferTaskInfo
	//	if err := rows.StructScan(&task); err != nil {
	//		return nil, &workflow.InternalServiceError{
	//			Message: fmt.Sprintf("GetTransferTasks operation failed. Error: %v", err),
	//		}
	//	}
	//
	//	resp.Tasks = append(resp.Tasks, &task)
	//}
	//
	//if err := rows.Err(); err != nil {
	//	return nil, &workflow.InternalServiceError{
	//		Message: fmt.Sprintf("GetTransferTasks operation failed. Error: %v", err),
	//	}
	//}
	//
	//return &resp, nil
	return &persistence.GetTransferTasksResponse{}, nil
}

func (*sqlMatchingManager) CompleteTransferTask(request *persistence.CompleteTransferTaskRequest) error {
	panic("implement me")
}

func (*sqlMatchingManager) GetReplicationTasks(request *persistence.GetReplicationTasksRequest) (*persistence.GetReplicationTasksResponse, error) {
	return &persistence.GetReplicationTasksResponse{}, nil
}

func (*sqlMatchingManager) CompleteReplicationTask(request *persistence.CompleteReplicationTaskRequest) error {
	panic("implement me")
}

func (*sqlMatchingManager) GetTimerIndexTasks(request *persistence.GetTimerIndexTasksRequest) (*persistence.GetTimerIndexTasksResponse, error) {
	panic("implement me")
}

func (*sqlMatchingManager) CompleteTimerTask(request *persistence.CompleteTimerTaskRequest) error {
	panic("implement me")
}

func NewSqlMatchingPersistence(username, password, host, port, dbName string) (persistence.ExecutionManager, error) {
	var db, err = sqlx.Connect("mysql",
		fmt.Sprintf(Dsn, username, password, host, port, dbName))
	if err != nil {
		return nil, err
	}

	return &sqlMatchingManager{
		db: db,
	}, nil
}