package sql

import (
	"fmt"
	"strings"

	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"

	"github.com/hmgle/sqlx"
	"github.com/uber/cadence/common"
)

type (
	sqlHistoryManager struct {
		db      *sqlx.DB
		shardID int
		logger  bark.Logger
	}

	eventsRow struct {
		DomainID     string  `db:"domain_id"`
		WorkflowID   string  `db:"workflow_id"`
		RunID        string  `db:"run_id"`
		FirstEventID int64   `db:"first_event_id"`
		Data         *[]byte `db:"data"`
		DataEncoding string  `db:"data_encoding"`
		DataVersion  int64   `db:"data_version"`

		RangeID int64 `db:"range_id"`
		TxID    int64 `db:"tx_id"`
	}
)

const (
	appendHistorySQLQueryTemplate = `INSERT IGNORE INTO events (
%v
) VALUES (
%v
);
`

	overwriteHistorySQLQueryTemplate = `UPDATE events
SET
%v
WHERE
%v
`

	getHistorySQLQueryTemplate = `SELECT %v FROM events WHERE %v`

	lockRangeIDAndTxIDSQLQueryTemplate = `SELECT range_id, tx_id FROM events WHERE
%v
FOR UPDATE
`

	getWorkflowExecutionHistorySQLQuery = `SELECT first_event_id, data, data_encoding, data_version
FROM events
WHERE
domain_id = ? AND
workflow_id = ? AND
run_id = ? AND
first_event_id >= ? AND
first_event_id < ?`
)

func stringMap(a []string, f func(string) string) []string {
	b := make([]string, len(a))
	for i, v := range a {
		b[i] = f(v)
	}
	return b
}

func makeAppendHistorySQLQuery(fields []string) string {
	return fmt.Sprintf(appendHistorySQLQueryTemplate,
		strings.Join(fields, ","),
		strings.Join(stringMap(fields, func(x string) string {
			return ":" + x
		}), ","))
}

func makeOverwriteHistorySQLQuery(updateFields []string, whereFields []string) string {
	return fmt.Sprintf(overwriteHistorySQLQueryTemplate,
		strings.Join(stringMap(updateFields, func(x string) string {
			return x + "= :" + x
		}), ","),
		strings.Join(stringMap(whereFields, func(x string) string {
			return x + "= :" + x
		}), " AND "))
}

func makeGetHistorySQLQuery(selectFields []string, whereFields []string) string {
	return fmt.Sprintf(getHistorySQLQueryTemplate,
		strings.Join(selectFields, ","),
		strings.Join(stringMap(whereFields, func(x string) string {
			return x + "= :" + x
		}), " AND "))
}

func makeLockRangeIDAndTxIDSQLQuery(whereFields []string) string {
	return fmt.Sprintf(lockRangeIDAndTxIDSQLQueryTemplate,
		strings.Join(stringMap(whereFields, func(x string) string {
			return x + " = ?"
		}), " AND "))
}

func takeAddressIfNotNil(b []byte) *[]byte {
	if b != nil {
		return &b
	}
	return nil
}

var (
	eventsColumns           = []string{"domain_id", "workflow_id", "run_id", "first_event_id", "data", "data_encoding", "data_version"}
	eventsPrimaryKeyColumns = []string{"domain_id", "workflow_id", "run_id", "first_event_id"}

	appendHistorySQLQuery      = makeAppendHistorySQLQuery(eventsColumns)
	overwriteHistorySQLQuery   = makeOverwriteHistorySQLQuery(eventsColumns, eventsPrimaryKeyColumns)
	pollHistorySQLQuery = makeGetHistorySQLQuery([]string{"1"}, eventsPrimaryKeyColumns)
	lockRangeIDAndTxIDSQLQuery = makeLockRangeIDAndTxIDSQLQuery(eventsPrimaryKeyColumns)
)

func (m *sqlHistoryManager) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

func NewHistoryPersistence(username, password, host, port, dbName string, logger bark.Logger) (persistence.HistoryManager, error) {
	var db, err = sqlx.Connect("mysql",
		fmt.Sprintf(Dsn, username, password, host, port, dbName))
	if err != nil {
		return nil, err
	}

	return &sqlHistoryManager{
		db:     db,
		logger: logger,
	}, nil
}

func (m *sqlHistoryManager) AppendHistoryEvents(request *persistence.AppendHistoryEventsRequest) error {
	arg := &eventsRow{
		DomainID:     request.DomainID,
		WorkflowID:   *request.Execution.WorkflowId,
		RunID:        *request.Execution.RunId,
		FirstEventID: request.FirstEventID,
		Data:         takeAddressIfNotNil(request.Events.Data),
		DataEncoding: string(request.Events.EncodingType),
		DataVersion:  int64(request.Events.Version),
		RangeID:      request.RangeID,
		TxID:         request.TransactionID,
	}

	if request.Overwrite {
		tx, err := m.db.Beginx()
		if err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to begin transaction for overwrite. Error: %v", err),
			}
		}
		defer tx.Rollback()

		if err := lockAndCheckRangeIDAndTxID(tx,
			request.RangeID,
			request.TransactionID,
			request.DomainID,
			*request.Execution.WorkflowId,
			*request.Execution.RunId,
			request.FirstEventID); err != nil {
			switch err.(type) {
			case *persistence.ConditionFailedError:
				return &persistence.ConditionFailedError{
					Msg: fmt.Sprintf("AppendHistoryEvents operation failed. Overwrite failed. Error: %v", err),
				}
			default:
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to lock row for overwrite. Error: %v", err),
				}
			}
		}

		if result, err := tx.NamedExec(overwriteHistorySQLQuery, arg); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Update failed. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to check number of rows updated. Error: %v", err),
				}
			}
			if rowsAffected != 1 {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("AppendHistoryEvents operation failed. Updated %v rows instead of one.", rowsAffected),
				}
			}
		}

		if err := tx.Commit(); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to lock row. Error: %v", err),
			}
		}
	} else {
		if result, err := m.db.NamedExec(appendHistorySQLQuery, arg); err != nil {
			return &workflow.InternalServiceError{
				Message: fmt.Sprintf("AppendHistoryEvents operation failed. Insert failed. Error: %v", err),
			}
		} else {
			rowsAffected, err := result.RowsAffected()
			if err != nil {
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("AppendHistoryEvents operation failed. Failed to check number of rows inserted. Error: %v", err),
				}
			}
			switch {
			case rowsAffected == 0:
				return &persistence.ConditionFailedError{
					Msg: fmt.Sprintf("AppendHistoryEvents operation failed. No rows were inserted, because a row with the same primary key already existed (probably)."),
				}
			case rowsAffected > 1:
				return &workflow.InternalServiceError{
					Message: fmt.Sprintf("AppendHistoryEvents operation failed. Inserted %v rows instead of one.", rowsAffected),
				}
			}
		}
	}

	return nil
}

func (m *sqlHistoryManager) GetWorkflowExecutionHistory(request *persistence.GetWorkflowExecutionHistoryRequest) (*persistence.GetWorkflowExecutionHistoryResponse,
	error) {
	var rows []eventsRow
	if err := m.db.Select(&rows,
		getWorkflowExecutionHistorySQLQuery,
			request.DomainID,
				request.Execution.WorkflowId,
					request.Execution.RunId,
						request.FirstEventID,
							request.NextEventID); err != nil {
		return nil, &workflow.InternalServiceError{
			Message: fmt.Sprintf("GetWorkflowExecutionHistory operation failed. Select failed. Error: %v", err),
		}
	}

	events := make([]persistence.SerializedHistoryEventBatch, len(rows))
	for i, v := range rows {
		events[i].EncodingType = common.EncodingType(v.DataEncoding)
		events[i].Version = int(v.DataVersion)
		if v.Data != nil {
			events[i].Data = *v.Data
		}
	}

	return &persistence.GetWorkflowExecutionHistoryResponse{
		Events: events,
		NextPageToken: []byte{},
	}, nil
}

func (m *sqlHistoryManager) DeleteWorkflowExecutionHistory(request *persistence.DeleteWorkflowExecutionHistoryRequest) error {
	panic("implement me")
}

func lockAndCheckRangeIDAndTxID(tx *sqlx.Tx,
	maxRangeID int64,
	maxTxIDPlusOne int64,
	domainID string,
	workflowID string,
	runID string,
	firstEventID int64) error {
	var row eventsRow
	if err := tx.Get(&row,
		lockRangeIDAndTxIDSQLQuery,
		domainID,
		workflowID,
		runID,
		firstEventID); err != nil {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("Failed to lock range ID and tx ID. Get failed. Error: %v", err),
		}
	}
	if !(row.RangeID <= maxRangeID) {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to lock range ID and tx ID. %v should've been at most %v.", row.RangeID, maxRangeID),
		}
	} else if !(row.TxID < maxTxIDPlusOne) {
		return &persistence.ConditionFailedError{
			Msg: fmt.Sprintf("Failed to lock range ID and tx ID. %v should've been strictly less than %v.", row.TxID, maxTxIDPlusOne),
		}
	}
	return nil
}
