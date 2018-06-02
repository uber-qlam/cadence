// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"os"
	"testing"
	"time"

	"github.com/pborman/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
)

type (
	timerQueueProcessorSuite struct {
		suite.Suite
		TestBase       // TODO: this is a historyTestBase.TestBase, not (directly) a persistence.CassandraTestBase, and definitely not a persistence.TestBase
		engineImpl     *historyEngineImpl
		matchingClient matching.Client
		shardClosedCh  chan int
		logger         bark.Logger

		mockMetadataMgr   *mocks.MetadataManager
		mockVisibilityMgr *mocks.VisibilityManager
	}
)

func TestTimerQueueProcessorSuite(t *testing.T) {
	s := new(timerQueueProcessorSuite)
	suite.Run(t, s)
}

func (s *timerQueueProcessorSuite) SetupSuite() {
	if testing.Verbose() {
		log.SetOutput(os.Stdout)
	}

	s.SetupWorkflowStore()
	s.SetupDomains()

	log2 := log.New()
	log2.Level = log.DebugLevel
	s.logger = bark.NewLoggerFromLogrus(log2)

	// override config for testing
	s.ShardContext.config.ShardUpdateMinInterval = 0 * time.Second
	s.ShardContext.config.TransferProcessorUpdateShardTaskCount = 1
	s.ShardContext.config.TimerProcessorUpdateShardTaskCount = 1
	s.ShardContext.config.ReplicatorProcessorUpdateShardTaskCount = 1
	s.ShardContext.config.TransferProcessorCompleteTransferInterval = 100 * time.Millisecond
	s.ShardContext.config.TimerProcessorCompleteTimerInterval = 100 * time.Millisecond
	s.ShardContext.config.TransferProcessorUpdateAckInterval = 100 * time.Millisecond
	s.ShardContext.config.TimerProcessorUpdateAckInterval = 100 * time.Millisecond

	historyCache := newHistoryCache(s.ShardContext, s.logger)
	historyCache.disabled = true
	// set the standby cluster's timer ack level to max since we are not testing it
	// but we are testing the complete timer functionality
	s.ShardContext.UpdateTimerClusterAckLevel(cluster.TestAlternativeClusterName, timerQueueAckMgrMaxTimestamp)
	s.matchingClient = &mocks.MatchingClient{}
	s.engineImpl = &historyEngineImpl{
		currentClusterName: s.ShardContext.GetService().GetClusterMetadata().GetCurrentClusterName(),
		shard:              s.ShardContext,
		historyMgr:         s.HistoryMgr,
		historyCache:       historyCache,
		logger:             s.logger,
		tokenSerializer:    common.NewJSONTaskTokenSerializer(),
		hSerializerFactory: persistence.NewHistorySerializerFactory(),
		metricsClient:      metrics.NewClient(tally.NoopScope, metrics.History),
	}
	s.engineImpl.txProcessor = newTransferQueueProcessor(
		s.ShardContext, s.engineImpl, s.mockVisibilityMgr, &mocks.MatchingClient{}, &mocks.HistoryClient{}, s.logger,
	)
}

func (s *timerQueueProcessorSuite) TearDownSuite() {
	s.TeardownDomains()
	s.TearDownWorkflowStore()
}

func (s *timerQueueProcessorSuite) TearDownTest() {

}

func (s *timerQueueProcessorSuite) updateTimerSeqNumbers(timerTasks []persistence.Task) {
	for _, task := range timerTasks {
		taskID, err := s.ShardContext.GetNextTransferTaskID()
		if err != nil {
			panic(err)
		}
		task.SetTaskID(taskID)
		s.logger.Infof("%v: TestTimerQueueProcessorSuite: Assigning timer: %s",
			time.Now().UTC(), TimerSequenceID{VisibilityTimestamp: persistence.GetVisibilityTSFrom(task), TaskID: task.GetTaskID()})
	}
}

func (s *timerQueueProcessorSuite) createExecutionWithTimers(domainID string, we workflow.WorkflowExecution, tl,
	identity string, timeOuts []int32) (*persistence.WorkflowMutableState, []persistence.Task) {

	// Generate first decision task event.
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	addWorkflowExecutionStartedEvent(builder, we, "wType", tl, []byte("input"), 100, 200, identity)
	di := addDecisionTaskScheduledEvent(builder)

	createState := createMutableState(builder)
	info := createState.ExecutionInfo
	task0, err0 := s.CreateWorkflowExecution(domainID, we, tl, info.WorkflowTypeName, info.WorkflowTimeout, info.DecisionTimeoutValue,
		info.ExecutionContext, info.NextEventID, info.LastProcessedEvent, info.DecisionScheduleID, nil)
	s.Nil(err0, "No error expected.")
	s.NotEmpty(task0, "Expected non empty task identifier.")

	state0, err2 := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err2, "No error expected.")

	builder = newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state0)
	startedEvent := addDecisionTaskStartedEvent(builder, di.ScheduleID, tl, identity)
	addDecisionTaskCompletedEvent(builder, di.ScheduleID, *startedEvent.EventId, nil, identity)
	timerTasks := []persistence.Task{}
	timerInfos := []*persistence.TimerInfo{}
	decisionCompletedID := int64(4)
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})

	for _, timeOut := range timeOuts {
		_, ti := builder.AddTimerStartedEvent(decisionCompletedID,
			&workflow.StartTimerDecisionAttributes{
				TimerId:                   common.StringPtr(uuid.New()),
				StartToFireTimeoutSeconds: common.Int64Ptr(int64(timeOut)),
			})
		timerInfos = append(timerInfos, ti)
		tBuilder.AddUserTimer(ti, builder)
	}

	if t := tBuilder.GetUserTimerTaskIfNeeded(builder); t != nil {
		timerTasks = append(timerTasks, t)
	}

	s.updateTimerSeqNumbers(timerTasks)

	updatedState := createMutableState(builder)
	err3 := s.UpdateWorkflowExecution(updatedState.ExecutionInfo, nil, nil, int64(3), timerTasks, nil, nil, nil, timerInfos, nil)
	s.Nil(err3)

	return createMutableState(builder), timerTasks
}

func (s *timerQueueProcessorSuite) addDecisionTimer(domainID string, we workflow.WorkflowExecution, tb *timerBuilder) []persistence.Task {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)

	condition := state.ExecutionInfo.NextEventID
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)

	di := addDecisionTaskScheduledEvent(builder)
	startedEvent := addDecisionTaskStartedEvent(builder, di.ScheduleID, state.ExecutionInfo.TaskList, "identity")

	timeOutTask := tb.AddStartToCloseDecisionTimoutTask(di.ScheduleID, di.Attempt, 1)
	timerTasks := []persistence.Task{timeOutTask}

	s.updateTimerSeqNumbers(timerTasks)

	addDecisionTaskCompletedEvent(builder, di.ScheduleID, startedEvent.GetEventId(), nil, "identity")
	err2 := s.UpdateWorkflowExecution(state.ExecutionInfo, nil, nil, condition, timerTasks, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")
	return timerTasks
}

func (s *timerQueueProcessorSuite) addUserTimer(domainID string, we workflow.WorkflowExecution, timerID string, tb *timerBuilder) []persistence.Task {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// create a user timer
	_, ti := builder.AddTimerStartedEvent(common.EmptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr(timerID), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	tb.AddUserTimer(ti, builder)
	t := tb.GetUserTimerTaskIfNeeded(builder)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return timerTasks
}

func (s *timerQueueProcessorSuite) addHeartBeatTimer(domainID string,
	we workflow.WorkflowExecution, tb *timerBuilder) (*workflow.HistoryEvent, []persistence.Task) {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:              common.StringPtr("testID"),
			HeartbeatTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(ase)
	builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")

	// create a heart beat timeout
	tt := tb.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	return ase, timerTasks
}

func (s *timerQueueProcessorSuite) closeWorkflow(domainID string, we workflow.WorkflowExecution) {
	state, err := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err)

	state.ExecutionInfo.State = persistence.WorkflowStateCompleted

	err2 := s.UpdateWorkflowExecution(state.ExecutionInfo, nil, nil, state.ExecutionInfo.NextEventID, nil, nil, nil, nil, nil, nil)
	s.Nil(err2, "No error expected.")
}

func (s *timerQueueProcessorSuite) TestSingleTimerTask() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("single-timer-test"),
		RunId:      common.StringPtr(validRunID),
	}
	taskList := "single-timer-queue"
	identity := "testIdentity"
	_, tt := s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1})

	timerInfo, err := s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	processor.Start()
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), tt)

	for {
		timerInfo, err := s.GetTimerIndexTasks()
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))
}

func (s *timerQueueProcessorSuite) TestManyTimerTasks() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("multiple-timer-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "multiple-timer-queue"
	identity := "testIdentity"
	_, tt := s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{1, 2, 3})

	timerInfo, err := s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.NotEmpty(timerInfo, "Expected non empty timers list")
	s.Equal(1, len(timerInfo))

	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	processor.Start()
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), tt)

	for {
		timerInfo, err := s.GetTimerIndexTasks()
		s.logger.Infof("TestManyTimerTasks: GetTimerIndexTasks: Response Count: %d \n", len(timerInfo))
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			processor.Stop()
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

	timerInfo, err = s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(3), processor.getTimerFiredCount(cluster.TestCurrentClusterName))
}

func (s *timerQueueProcessorSuite) TestTimerTaskAfterProcessorStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("After-timer-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "After-timer-queue"
	identity := "testIdentity"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, identity, []int32{})

	timerInfo, err := s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Empty(timerInfo, "Expected empty timers list")

	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), tt)

	s.waitForTimerTasksToProcess(processor)

	timerInfo, err = s.GetTimerIndexTasks()
	s.Nil(err, "No error expected.")
	s.Equal(0, len(timerInfo))

	s.Equal(uint64(1), processor.getTimerFiredCount(cluster.TestCurrentClusterName))
}

func (s *timerQueueProcessorSuite) waitForTimerTasksToProcess(p timerQueueProcessor) {
	for {
		timerInfo, err := s.GetTimerIndexTasks()
		s.Nil(err, "No error expected.")
		if len(timerInfo) == 0 {
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	p.Stop()
}

func (s *timerQueueProcessorSuite) checkTimedOutEventFor(domainID string, we workflow.WorkflowExecution,
	scheduleID int64) bool {
	info, err1 := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err1)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(info)
	_, isRunning := builder.GetActivityInfo(scheduleID)

	return isRunning
}

func (s *timerQueueProcessorSuite) checkTimedOutEventForUserTimer(domainID string, we workflow.WorkflowExecution,
	timerID string) bool {
	info, err1 := s.GetWorkflowExecutionInfo(domainID, we)
	s.Nil(err1)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(info)

	isRunning, _ := builder.GetUserTimer(timerID)
	return isRunning
}

func (s *timerQueueProcessorSuite) updateHistoryAndTimers(ms *mutableStateBuilder, timerTasks []persistence.Task, condition int64) {
	updatedState := createMutableState(ms)

	actInfos := []*persistence.ActivityInfo{}
	for _, x := range updatedState.ActivitInfos {
		actInfos = append(actInfos, x)
	}
	timerInfos := []*persistence.TimerInfo{}
	for _, x := range updatedState.TimerInfos {
		timerInfos = append(timerInfos, x)
	}

	s.updateTimerSeqNumbers(timerTasks)
	err3 := s.UpdateWorkflowExecution(
		updatedState.ExecutionInfo, nil, nil, condition, timerTasks, nil, actInfos, nil, timerInfos, nil)
	s.Nil(err3)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_WithOutStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToStart - Without Start
	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	activityScheduledEvent, _ := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(2),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(activityScheduledEvent)

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *activityScheduledEvent.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_WithStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-Started-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToStart - With Start
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(ase)
	builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")

	// create a schedule to start timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToStart_MoreThanStartToClose() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_START-more-than-start2close"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToStart - Without Start
	processor := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	processor.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	activityScheduledEvent, _ := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(2),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(activityScheduledEvent)

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	s.Equal(workflow.TimeoutTypeScheduleToStart, workflow.TimeoutType(tt.(*persistence.ActivityTimeoutTask).TimeoutType))
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	processor.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(processor)
	s.Equal(uint64(1), processor.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *activityScheduledEvent.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskStartToClose_WithStart() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-START_TO_CLOSE-Started-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeStartToClose - Just start.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")

	// create a start to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskStartToClose_CompletedActivity() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-START_TO_CLOSE-Completed-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeStartToClose - Start and Completed activity.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	aste := builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	builder.AddActivityTaskCompletedEvent(*ase.EventId, *aste.EventId, &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result:   []byte("result"),
	})

	// create a start to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddStartToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_JustScheduled() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Scheduled-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Just Scheduled.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, _ := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(ase)

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_Started() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Started-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Scheduled and started.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(ase)
	builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	tt := tBuilder.GetActivityTimerTaskIfNeeded(builder)
	s.NotNil(tt)
	timerTasks := []persistence.Task{tt}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskScheduleToClose_Completed() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-SCHEDULE_TO_CLOSE-Completed-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Scheduled, started, completed.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase, ai := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(ase)
	aste := builder.AddActivityTaskStartedEvent(ai, *ase.EventId, uuid.New(), "")
	builder.AddActivityTaskCompletedEvent(*ase.EventId, *aste.EventId, &workflow.RespondActivityTaskCompletedRequest{
		Identity: common.StringPtr("test-id"),
		Result:   []byte("result"),
	})

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTaskHeartBeat_JustStarted() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("activity-timer-hb-started-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "activity-timer-queue"

	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeHeartbeat - Scheduled, started.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	ase, timerTasks := s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)
	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase.EventId)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerActivityTask_SameExpiry() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr("activity-timer-same-expiry-test"),
		RunId:      common.StringPtr(validRunID),
	}

	taskList := "activity-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// TimeoutTypeScheduleToClose - Scheduled, started, completed.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	ase1, ai1 := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID-1"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(ase1)
	ase2, ai2 := builder.AddActivityTaskScheduledEvent(common.EmptyEventID,
		&workflow.ScheduleActivityTaskDecisionAttributes{
			ActivityId:                    common.StringPtr("testID-2"),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(1),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(1),
			TaskList:                      &workflow.TaskList{Name: common.StringPtr("task-list")},
		})
	s.NotNil(ase2)

	// create a schedule to close timeout
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	t, err := tBuilder.AddScheduleToCloseActivityTimeout(ai1)
	s.NoError(err)
	s.NotNil(t)
	t, err = tBuilder.AddScheduleToCloseActivityTimeout(ai2)
	s.NoError(err)
	s.NotNil(t)
	timerTasks := []persistence.Task{t}

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventFor(domainID, workflowExecution, *ase1.EventId)
	s.False(running)
	running = s.checkTimedOutEventFor(domainID, workflowExecution, *ase2.EventId)
	s.False(running)

	// assert activity infos are deleted
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder = newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	s.Equal(0, len(builder.pendingActivityInfoIDs))
}

func (s *timerQueueProcessorSuite) TestTimerUserTimers() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "user-timer-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// Single timer.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})
	timerID := "tid1"
	timerTasks := s.addUserTimer(domainID, workflowExecution, timerID, tBuilder)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)
	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(1), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventForUserTimer(domainID, workflowExecution, timerID)
	s.False(running)
}

func (s *timerQueueProcessorSuite) TestTimerUserTimers_SameExpiry() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("user-timer-same-expiry-test"),
		RunId: common.StringPtr(validRunID)}

	taskList := "user-timer-same-expiry-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	// Two timers.
	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	state, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder := newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	condition := state.ExecutionInfo.NextEventID

	// load any timers.
	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now().Add(-1 * time.Second)})
	timerTasks := []persistence.Task{}

	// create two user timers.
	_, ti := builder.AddTimerStartedEvent(common.EmptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid1"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})
	_, ti2 := builder.AddTimerStartedEvent(common.EmptyEventID,
		&workflow.StartTimerDecisionAttributes{TimerId: common.StringPtr("tid2"), StartToFireTimeoutSeconds: common.Int64Ptr(1)})

	tBuilder.AddUserTimer(ti, builder)
	tBuilder.AddUserTimer(ti2, builder)
	t := tBuilder.GetUserTimerTaskIfNeeded(builder)
	timerTasks = append(timerTasks, t)

	s.updateHistoryAndTimers(builder, timerTasks, condition)
	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), timerTasks)

	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(len(timerTasks)), p.getTimerFiredCount(cluster.TestCurrentClusterName))
	running := s.checkTimedOutEventForUserTimer(domainID, workflowExecution, ti.TimerID)
	s.False(running)
	running = s.checkTimedOutEventForUserTimer(domainID, workflowExecution, ti2.TimerID)
	s.False(running)

	// assert user timer infos are deleted
	state, err = s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	builder = newMutableStateBuilder(s.ShardContext.GetConfig(), s.logger)
	builder.Load(state)
	s.Equal(0, len(builder.pendingTimerInfoIDs))
}

func (s *timerQueueProcessorSuite) TestTimersOnClosedWorkflow() {
	domainID := testDomainActiveID
	workflowExecution := workflow.WorkflowExecution{WorkflowId: common.StringPtr("closed-workflow-test-desicion-timer"),
		RunId: common.StringPtr(validRunID)}

	taskList := "closed-workflow-queue"
	s.createExecutionWithTimers(domainID, workflowExecution, taskList, "identity", []int32{})

	p := newTimerQueueProcessor(s.ShardContext, s.engineImpl, s.matchingClient, s.logger).(*timerQueueProcessorImpl)
	p.Start()

	tBuilder := newTimerBuilder(s.ShardContext.GetConfig(), s.logger, &mockTimeSource{currTime: time.Now()})

	// Start of one of each timers each
	s.addDecisionTimer(domainID, workflowExecution, tBuilder)
	tt := s.addUserTimer(domainID, workflowExecution, "tid1", tBuilder)
	s.addHeartBeatTimer(domainID, workflowExecution, tBuilder)

	// GEt current state of workflow.
	state0, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)

	// close workflow
	s.closeWorkflow(domainID, workflowExecution)

	p.NotifyNewTimers(cluster.TestCurrentClusterName, s.ShardContext.GetCurrentTime(cluster.TestCurrentClusterName), tt)
	s.waitForTimerTasksToProcess(p)
	s.Equal(uint64(3), p.getTimerFiredCount(cluster.TestCurrentClusterName))

	// Verify that no new events are added to workflow.
	state1, err := s.GetWorkflowExecutionInfo(domainID, workflowExecution)
	s.Nil(err)
	s.Equal(state0.ExecutionInfo.NextEventID, state1.ExecutionInfo.NextEventID)
}

func (s *timerQueueProcessorSuite) printHistory(builder *mutableStateBuilder) string {
	history, err := builder.hBuilder.Serialize()
	if err != nil {
		s.logger.Errorf("Error serializing history: %v", err)
		return ""
	}

	//s.logger.Info(string(history))
	return history.String()
}
