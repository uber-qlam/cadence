package sql

import (
	"fmt"
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"

	"github.com/hmgle/sqlx"
	"strings"
)

/*
CRUD methods for the execution row's set/map/list objects.

You need to lock next_event_id before calling any of these.
*/

func stringMap(a []string, f func(string) string) []string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = f(v)
	}
	return b
}

func prependColons(a []string) []string {
	return stringMap(a, func(x string) string { return ":" + x })
}

func makeAssignmentsForUpdate(a []string) []string {
	return stringMap(a, func(x string) string { return x + " = :" + x })
}

const (
	deleteMapSQLQueryTemplate = `DELETE FROM %v
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id`

	// %[2]v is the columns of the value struct (i.e. no primary key columns), comma separated
	// %[3]v should be %[2]v with colons prepended.
	// i.e. %[3]v = ",".join(":" + s for s in %[2]v)
	// So that this query can be used with BindNamed
	// %[4]v should be the name of the key associated with the map
	// e.g. for ActivityInfo it is "schedule_id"
	setKeyInMapSQLQueryTemplate = `REPLACE INTO %[1]v
(shard_id, domain_id, workflow_id, run_id, %[4]v, %[2]v)
VALUES
(:shard_id, :domain_id, :workflow_id, :run_id, :%[4]v, %[3]v)`

	// %[2]v is the name of the key
	deleteKeyInMapSQLQueryTemplate = `DELETE FROM %[1]v
WHERE
shard_id = :shard_id AND
domain_id = :domain_id AND
workflow_id = :workflow_id AND
run_id = :run_id AND
%[2]v = :%[2]v`


	// %[1]v is the name of the table
	// %[2]v is the name of the key
	// %[3]v is the value columns, separated by commas
	getMapSQLQueryTemplate = `SELECT %[2]v, %[3]v FROM %[1]v
WHERE
shard_id = ? AND
domain_id = ? AND
workflow_id = ? AND
run_id = ?`
)

func makeDeleteMapSQLQuery(tableName string) string {
	return fmt.Sprintf(deleteMapSQLQueryTemplate, tableName)
}

func makeSetKeyInMapSQLQuery(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(setKeyInMapSQLQueryTemplate,
		tableName,
			strings.Join(nonPrimaryKeyColumns, ","),
				strings.Join(stringMap(nonPrimaryKeyColumns, func(x string) string {
					return ":" + x
				}), ","),
					mapKeyName)
}

func makeDeleteKeyInMapSQLQuery(tableName string, mapKeyName string) string {
	return fmt.Sprintf(deleteKeyInMapSQLQueryTemplate,
		tableName,
			mapKeyName)
}

func makeGetMapSQLQueryTemplate(tableName string, nonPrimaryKeyColumns []string, mapKeyName string) string {
	return fmt.Sprintf(getMapSQLQueryTemplate,
		tableName,
		mapKeyName,
			strings.Join(nonPrimaryKeyColumns, ","))
}

var (
	// Omit shard_id, run_id, domain_id, workflow_id, schedule_id since they're in the primary key
	activityInfoColumns = []string{
		"version",
		"scheduled_event",
		"scheduled_time",
		"started_id",
		"started_event",
		"started_time",
		"activity_id",
		"request_id",
		"details",
		"schedule_to_start_timeout",
		"schedule_to_close_timeout",
		"start_to_close_timeout",
		"heartbeat_timeout",
		"cancel_requested",
		"cancel_request_id",
		"last_heartbeat_updated_time",
		"timer_task_status",
		"attempt",
		"task_list",
		"started_identity",
		"has_retry_policy",
		"init_interval",
		"backoff_coefficient",
		"max_interval",
		"expiration_time",
		"max_attempts",
		"non_retriable_errors",
	}
	activityInfoTableName = "activity_info_maps"
	activityInfoKey = "schedule_id"

	deleteActivityInfoSQLQuery      = makeDeleteMapSQLQuery(activityInfoTableName)
	setKeyInActivityInfoMapSQLQuery = makeSetKeyInMapSQLQuery(activityInfoTableName, activityInfoColumns, activityInfoKey)
	deleteKeyInActivityInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(activityInfoTableName, activityInfoKey)
	getActivityInfoMapSQLQuery = makeGetMapSQLQueryTemplate(activityInfoTableName, activityInfoColumns, activityInfoKey)
)

type (
	activityInfoMapsPrimaryKey struct {
		ShardID    int64  `db:"shard_id"`
		DomainID   string `db:"domain_id"`
		WorkflowID string `db:"workflow_id"`
		RunID      string `db:"run_id"`
		ScheduleID int64  `db:"schedule_id"`
	}

	activityInfoMapsRow struct {
		activityInfoMapsPrimaryKey
		Version                  int64     `db:"version"`
		ScheduledEvent           []byte    `db:"scheduled_event"`
		ScheduledTime            time.Time `db:"scheduled_time"`
		StartedID                int64     `db:"started_id"`
		StartedEvent             []byte    `db:"started_event"`
		StartedTime              time.Time `db:"started_time"`
		ActivityID               string    `db:"activity_id"`
		RequestID                string    `db:"request_id"`
		Details                  *[]byte    `db:"details"`
		ScheduleToStartTimeout   int64     `db:"schedule_to_start_timeout"`
		ScheduleToCloseTimeout   int64     `db:"schedule_to_close_timeout"`
		StartToCloseTimeout      int64     `db:"start_to_close_timeout"`
		HeartbeatTimeout         int64     `db:"heartbeat_timeout"`
		CancelRequested          int64     `db:"cancel_requested"`
		CancelRequestID          int64     `db:"cancel_request_id"`
		LastHeartbeatUpdatedTime time.Time `db:"last_heartbeat_updated_time"`
		TimerTaskStatus          int64     `db:"timer_task_status"`
		Attempt                  int64     `db:"attempt"`
		TaskList                 string    `db:"task_list"`
		StartedIdentity          string    `db:"started_identity"`
		HasRetryPolicy           int64     `db:"has_retry_policy"`
		InitInterval             int64     `db:"init_interval"`
		BackoffCoefficient       float64   `db:"backoff_coefficient"`
		MaxInterval              int64     `db:"max_interval"`
		ExpirationTime           time.Time `db:"expiration_time"`
		MaxAttempts              int64     `db:"max_attempts"`
		NonRetriableErrors       *[]byte    `db:"non_retriable_errors"`
	}
)

func updateActivityInfos(tx *sqlx.Tx,
	activityInfos []*persistence.ActivityInfo,
	deleteInfos []int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {

	if len(activityInfos) > 0 {
		activityInfoMapsRows := make([]*activityInfoMapsRow, len(activityInfos))
		for i, v := range activityInfos {
			activityInfoMapsRows[i] = &activityInfoMapsRow{
				activityInfoMapsPrimaryKey: activityInfoMapsPrimaryKey{
					ShardID:    int64(shardID),
					DomainID:   domainID,
					WorkflowID: workflowID,
					RunID:      runID,
					ScheduleID: v.ScheduleID,
				},
				Version:                  v.Version,
				ScheduledEvent:           v.ScheduledEvent,
				ScheduledTime:            v.ScheduledTime,
				StartedID:                v.StartedID,
				StartedEvent:             v.StartedEvent,
				StartedTime:              v.StartedTime,
				ActivityID:               v.ActivityID,
				RequestID:                v.RequestID,
				ScheduleToStartTimeout:   int64(v.ScheduleToStartTimeout),
				ScheduleToCloseTimeout:   int64(v.ScheduleToCloseTimeout),
				StartToCloseTimeout:      int64(v.StartToCloseTimeout),
				HeartbeatTimeout:         int64(v.HeartbeatTimeout),
				CancelRequested:          boolToInt64(v.CancelRequested),
				CancelRequestID:          v.CancelRequestID,
				LastHeartbeatUpdatedTime: v.LastHeartBeatUpdatedTime,
				TimerTaskStatus:          int64(v.TimerTaskStatus),
				Attempt:                  int64(v.Attempt),
				TaskList:                 v.TaskList,
				StartedIdentity:          v.StartedIdentity,
				HasRetryPolicy:           boolToInt64(v.HasRetryPolicy),
				InitInterval:             int64(v.InitialInterval),
				BackoffCoefficient:       v.BackoffCoefficient,
				MaxInterval:              int64(v.MaximumInterval),
				ExpirationTime:           v.ExpirationTime,
				MaxAttempts:              int64(v.MaximumAttempts),
			}

			if v.Details != nil {
				activityInfoMapsRows[i].Details = &v.Details
			}

			if v.NonRetriableErrors != nil {
				nonRetriableErrors, err := gobSerialize(&v.NonRetriableErrors)
				if err != nil {
					return &workflow.InternalServiceError{
						Message: fmt.Sprintf("Failed to update activity info. Failed to serialize ActivityInfo.NonRetriableErrors. Error: %v", err),
					}
				}
				activityInfoMapsRows[i].NonRetriableErrors = &nonRetriableErrors
			}
		}

		query, args, err := tx.BindNamed(setKeyInActivityInfoMapSQLQuery, activityInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to bind query. Error: %v", err),
			}
		}

		if result, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to execute update query. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update activity info. Failed to verify number of rows updated. Error: %v", err),
				}
			}
			if int(rowsAffected) != len(activityInfos) {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update activity info. Touched %v rows instead of %v", len(activityInfos), rowsAffected),
				}
			}
		}

	}

	if len(deleteInfos) > 0 {
		activityInfoMapsPrimaryKeys := make([]*activityInfoMapsPrimaryKey, len(deleteInfos))
		for i, v := range deleteInfos {
			activityInfoMapsPrimaryKeys[i] = &activityInfoMapsPrimaryKey{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				ScheduleID: v,
			}
		}

		query, args, err := tx.BindNamed(deleteKeyInActivityInfoMapSQLQuery, activityInfoMapsPrimaryKeys)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to bind query. Error: %v", err),
			}
		}

		if result, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update activity info. Failed to execute delete query. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update activity info. Failed to verify number of rows deleted. Error: %v", err),
				}
			}
			if int(rowsAffected) != len(deleteInfos) {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update activity info. Deleted %v rows instead of %v", len(activityInfos), rowsAffected),
				}
			}
		}
	}

	return nil
}

func getActivityInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.ActivityInfo, error) {
		var activityInfoMapsRows []activityInfoMapsRow

		if err := tx.Select(&activityInfoMapsRows,
			getActivityInfoMapSQLQuery,
			shardID,
			domainID,
			 workflowID,
			runID); err != nil {
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to get activity info. Error: %v", err),
			}
		}

		ret := make(map[int64]*persistence.ActivityInfo)
		for _, v := range activityInfoMapsRows {
			ret[v.ScheduleID] = &persistence.ActivityInfo{
				Version: v.Version,
				ScheduleID: v.ScheduleID,
				ScheduledEvent: v.ScheduledEvent,
				ScheduledTime: v.ScheduledTime,
				StartedID: v.StartedID,
				StartedEvent: v.StartedEvent,
				StartedTime: v.StartedTime,
				ActivityID: v.ActivityID,
				RequestID: v.RequestID,
				ScheduleToStartTimeout: int32(v.ScheduleToStartTimeout),
				ScheduleToCloseTimeout: int32(v.ScheduleToCloseTimeout),
				StartToCloseTimeout: int32(v.StartToCloseTimeout),
				HeartbeatTimeout: int32(v.HeartbeatTimeout),
				CancelRequested: int64ToBool(v.CancelRequested),
				CancelRequestID: v.CancelRequestID,
				LastHeartBeatUpdatedTime: v.LastHeartbeatUpdatedTime,
				TimerTaskStatus: int32(v.TimerTaskStatus),
				Attempt: int32(v.Attempt),
				DomainID: v.DomainID,
				StartedIdentity: v.StartedIdentity,
				TaskList: v.TaskList,
				HasRetryPolicy: int64ToBool(v.HasRetryPolicy),
				InitialInterval: int32(v.InitInterval),
				BackoffCoefficient: v.BackoffCoefficient,
				MaximumInterval: int32(v.MaxInterval),
				ExpirationTime: v.ExpirationTime,
				MaximumAttempts: int32(v.MaxAttempts),
			}

			if v.Details != nil {
				ret[v.ScheduleID].Details = *v.Details
			}

			if v.NonRetriableErrors != nil {
				ret[v.ScheduleID].NonRetriableErrors = []string{}
				if err := gobDeserialize(*v.NonRetriableErrors, &ret[v.ScheduleID].NonRetriableErrors); err != nil {
					return nil, &workflow.InternalServiceError{
						Message: fmt.Sprintf("Failed to get activity info. Failed to deserialize ActivityInfo.NonRetriableErrors. %v", err),
					}
				}
			}
		}

		return ret, nil
}

var (
	timerInfoColumns = []string{
		"version",
		"started_id",
		"expiry_time",
		"task_id",
	}
	timerInfoTableName = "timer_info_maps"
	timerInfoKey = "timer_id"

	deleteTimerInfoSQLQuery      = makeDeleteMapSQLQuery(timerInfoTableName)
	setKeyInTimerInfoMapSQLQuery = makeSetKeyInMapSQLQuery(timerInfoTableName, timerInfoColumns, timerInfoKey)
	deleteKeyInTimerInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(timerInfoTableName, timerInfoKey)
	getTimerInfoMapSQLQuery = makeGetMapSQLQueryTemplate(timerInfoTableName, timerInfoColumns, timerInfoKey)
)

type (
	timerInfoMapsPrimaryKey struct {
		ShardID    int64  `db:"shard_id"`
		DomainID   string `db:"domain_id"`
		WorkflowID string `db:"workflow_id"`
		RunID      string `db:"run_id"`
		TimerID string  `db:"timer_id"`
	}

	timerInfoMapsRow struct {
		timerInfoMapsPrimaryKey
		Version int64 `db:"version"`
		StartedID int64 `db:"started_id"`
		ExpiryTime time.Time `db:"expiry_time"`
		TaskID int64 `db:"task_id"`
	}
)

func updateTimerInfos(tx *sqlx.Tx,
	timerInfos []*persistence.TimerInfo,
	deleteInfos []string,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(timerInfos) > 0 {
		timerInfoMapsRows := make([]*timerInfoMapsRow, len(timerInfos))
		for i, v := range timerInfos {
			timerInfoMapsRows[i] = &timerInfoMapsRow{
				timerInfoMapsPrimaryKey: timerInfoMapsPrimaryKey{
					ShardID: int64(shardID),
					DomainID: domainID,
					WorkflowID: workflowID,
					RunID: runID,
					TimerID: v.TimerID,
				},
				Version: v.Version,
				StartedID: v.StartedID,
				ExpiryTime: v.ExpiryTime,
				TaskID: v.TaskID,
			}
		}

		query, args, err := tx.BindNamed(setKeyInTimerInfoMapSQLQuery, timerInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to bind query. Error: %v", err),
			}
		}

		if result, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to execute update query. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update timer info. Failed to verify number of rows updated. Error: %v", err),
				}
			}
			if int(rowsAffected) != len(timerInfos) {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update timer info. Touched %v rows instead of %v", len(timerInfos), rowsAffected),
				}
			}
		}

	}
	if len(deleteInfos) > 0 {
		timerInfoMapsPrimaryKeys := make([]*timerInfoMapsPrimaryKey, len(deleteInfos))
		for i, v := range deleteInfos {
			timerInfoMapsPrimaryKeys[i] = &timerInfoMapsPrimaryKey{
				ShardID:    int64(shardID),
				DomainID:   domainID,
				WorkflowID: workflowID,
				RunID:      runID,
				TimerID: v,
			}
		}

		query, args, err := tx.BindNamed(deleteKeyInTimerInfoMapSQLQuery, timerInfoMapsPrimaryKeys)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to bind query. Error: %v", err),
			}
		}

		if result, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update timer info. Failed to execute delete query. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update timer info. Failed to verify number of rows deleted. Error: %v", err),
				}
			}
			if int(rowsAffected) != len(deleteInfos) {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update timer info. Deleted %v rows instead of %v", len(timerInfos), rowsAffected),
				}
			}
		}
	}

	return nil
}

func getTimerInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[string]*persistence.TimerInfo, error) {
	var timerInfoMapsRows []timerInfoMapsRow

	if err := tx.Select(&timerInfoMapsRows,
		getTimerInfoMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get timer info. Error: %v", err),
		}
	}

	ret := make(map[string]*persistence.TimerInfo)
	for _, v := range timerInfoMapsRows {
		ret[v.TimerID] = &persistence.TimerInfo{
			Version: v.Version,
			TimerID: v.TimerID,
			StartedID: v.StartedID,
			ExpiryTime: v.ExpiryTime,
			TaskID: v.TaskID,
		}
	}

	return ret, nil
}


var (
	childExecutionInfoColumns = []string{
		"version",
		"initiated_event",
		"started_id",
		"started_event",
		"create_request_id",
	}
	childExecutionInfoTableName = "child_execution_info_maps"
	childExecutionInfoKey = "initiated_id"

	deleteChildExecutionInfoSQLQuery      = makeDeleteMapSQLQuery(childExecutionInfoTableName)
	setKeyInChildExecutionInfoMapSQLQuery = makeSetKeyInMapSQLQuery(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
	deleteKeyInChildExecutionInfoMapSQLQuery = makeDeleteKeyInMapSQLQuery(childExecutionInfoTableName, childExecutionInfoKey)
	getChildExecutionInfoMapSQLQuery = makeGetMapSQLQueryTemplate(childExecutionInfoTableName, childExecutionInfoColumns, childExecutionInfoKey)
)

type (
	childExecutionInfoMapsPrimaryKey struct {
		ShardID    int64  `db:"shard_id"`
		DomainID   string `db:"domain_id"`
		WorkflowID string `db:"workflow_id"`
		RunID      string `db:"run_id"`
		InitiatedID int64  `db:"initiated_id"`
	}

	childExecutionInfoMapsRow struct {
		childExecutionInfoMapsPrimaryKey
		Version int64 `db:"version"`
		InitiatedEvent *[]byte `db:"initiated_event"`
		StartedID int64 `db:"started_id"`
		StartedEvent *[]byte `db:"started_event"`
		CreateRequestID string `db:"create_request_id"`
	}
)

func updateChildExecutionInfos(tx *sqlx.Tx,
	childExecutionInfos []*persistence.ChildExecutionInfo,
	deleteInfos *int64,
	shardID int,
	domainID,
	workflowID,
	runID string) error {
	if len(childExecutionInfos) > 0 {
		timerInfoMapsRows := make([]*childExecutionInfoMapsRow, len(childExecutionInfos))
		for i, v := range childExecutionInfos {
			timerInfoMapsRows[i] = &childExecutionInfoMapsRow{
				childExecutionInfoMapsPrimaryKey: childExecutionInfoMapsPrimaryKey{
					ShardID: int64(shardID),
					DomainID: domainID,
					WorkflowID: workflowID,
					RunID: runID,
					InitiatedID: v.InitiatedID,
				},
				Version: v.Version,
				InitiatedEvent: takeAddressIfNotNil(v.InitiatedEvent),
				StartedID: v.StartedID,
				StartedEvent: takeAddressIfNotNil(v.StartedEvent),
				CreateRequestID: v.CreateRequestID,
			}
		}

		query, args, err := tx.BindNamed(setKeyInChildExecutionInfoMapSQLQuery, timerInfoMapsRows)
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to bind query. Error: %v", err),
			}
		}

		if result, err := tx.Exec(query, args...); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to execute update query. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update child execution info. Failed to verify number of rows updated. Error: %v", err),
				}
			}
			if int(rowsAffected) != len(childExecutionInfos) {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update child execution info. Touched %v rows instead of %v", len(childExecutionInfos), rowsAffected),
				}
			}
		}

	}
	if deleteInfos != nil {
		if result, err := tx.NamedExec(deleteKeyInChildExecutionInfoMapSQLQuery, &childExecutionInfoMapsPrimaryKey{
			ShardID:    int64(shardID),
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			InitiatedID: *deleteInfos,
		}); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("Failed to update child execution info. Failed to execute delete query. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update child execution info. Failed to verify number of rows deleted. Error: %v", err),
				}
			}
			if int(rowsAffected) != 1 {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("Failed to update child execution info. Deleted %v rows instead of %v", len(childExecutionInfos), rowsAffected),
				}
			}
		}
	}

	return nil
}

func getChildExecutionInfoMap(tx *sqlx.Tx,
	shardID int,
	domainID,
	workflowID,
	runID string) (map[int64]*persistence.ChildExecutionInfo, error) {
	var childExecutionInfoMapsRows []childExecutionInfoMapsRow

	if err := tx.Select(&childExecutionInfoMapsRows,
		getChildExecutionInfoMapSQLQuery,
		shardID,
		domainID,
		workflowID,
		runID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to get timer info. Error: %v", err),
		}
	}

	ret := make(map[int64]*persistence.ChildExecutionInfo)
	for _, v := range childExecutionInfoMapsRows {
		ret[v.InitiatedID] = &persistence.ChildExecutionInfo{
			InitiatedID: v.InitiatedID,
			Version: v.Version,
			InitiatedEvent: dereferenceIfNotNil(v.InitiatedEvent),
			StartedID: v.StartedID,
			StartedEvent: dereferenceIfNotNil(v.StartedEvent),
			CreateRequestID: v.CreateRequestID,
		}

	}

	return ret, nil
}