package swf_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

const (
	ttDomain   = "dom"
	ttWorkflow = "wf"
	ttTaskList = "tl"
)

func ttEventAttrs(t *testing.T, events []swf.HistoryEvent, eventType, key string) map[string]any {
	t.Helper()

	for _, e := range events {
		if e.EventType == eventType {
			attrs, ok := e.Attributes[key].(map[string]any)
			require.True(t, ok, "event %s has no %s", eventType, key)

			return attrs
		}
	}

	return nil
}

// ttStart registers the types, starts an execution, and returns it with its first decision task
// already polled.
func ttStart(t *testing.T, b *swf.InMemoryBackend, taskTimeout string, act swf.ActivityTypeDefaults) (string, string) {
	t.Helper()

	require.NoError(t, b.RegisterDomain(ttDomain, "", "1"))
	require.NoError(t, b.RegisterWorkflowType(ttDomain, "wt", "1", "", swf.WorkflowTypeDefaults{}))
	require.NoError(t, b.RegisterActivityType(ttDomain, "at", "1", "", act))

	exec, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: ttDomain, WorkflowID: ttWorkflow, WorkflowTypeName: "wt", WorkflowTypeVersion: "1",
		TaskList: ttTaskList, TaskStartToCloseTimeout: taskTimeout,
	})
	require.NoError(t, err)

	task := b.PollForDecisionTask(ttDomain, ttTaskList, 0, "", false)
	require.NotNil(t, task)

	return exec.RunID, task.TaskToken
}

func TestActivityTaskTimeouts(t *testing.T) {
	t.Parallel()

	type step struct {
		advance   time.Duration
		heartbeat bool
	}

	tests := []struct {
		attrs       swf.ScheduleActivityTaskDecisionAttrs
		defaults    swf.ActivityTypeDefaults
		name        string
		wantType    string
		wantDetails string
		steps       []step
		poll        bool
		wantStarted bool
	}{
		{
			name:     "schedule_to_start",
			attrs:    swf.ScheduleActivityTaskDecisionAttrs{ScheduleToStartTimeout: "10"},
			steps:    []step{{advance: 11 * time.Second}},
			wantType: "SCHEDULE_TO_START",
		},
		{
			name:     "start_to_close",
			attrs:    swf.ScheduleActivityTaskDecisionAttrs{StartToCloseTimeout: "10"},
			poll:     true,
			steps:    []step{{advance: 11 * time.Second}},
			wantType: "START_TO_CLOSE", wantStarted: true,
		},
		{
			name:     "schedule_to_close",
			attrs:    swf.ScheduleActivityTaskDecisionAttrs{ScheduleToCloseTimeout: "10", StartToCloseTimeout: "100"},
			poll:     true,
			steps:    []step{{advance: 11 * time.Second}},
			wantType: "SCHEDULE_TO_CLOSE", wantStarted: true,
		},
		{
			name:  "heartbeat_extends",
			attrs: swf.ScheduleActivityTaskDecisionAttrs{HeartbeatTimeout: "5", StartToCloseTimeout: "100"},
			poll:  true,
			steps: []step{
				{advance: 3 * time.Second, heartbeat: true},
				{advance: 3 * time.Second},
				{advance: 3 * time.Second},
			},
			wantType: "HEARTBEAT", wantStarted: true, wantDetails: "progress",
		},
		{
			name:     "type_default_applies",
			attrs:    swf.ScheduleActivityTaskDecisionAttrs{},
			defaults: swf.ActivityTypeDefaults{DefaultTaskScheduleToStartTimeout: "5"},
			steps:    []step{{advance: 6 * time.Second}},
			wantType: "SCHEDULE_TO_START",
		},
		{
			name:  "within_timeout",
			attrs: swf.ScheduleActivityTaskDecisionAttrs{ScheduleToStartTimeout: "10"},
			steps: []step{{advance: 9 * time.Second}},
		},
		{
			name:  "none_never_expires",
			attrs: swf.ScheduleActivityTaskDecisionAttrs{ScheduleToStartTimeout: "NONE"},
			steps: []step{{advance: 24 * time.Hour}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := swf.NewInMemoryBackend()
				_, token := ttStart(t, b, "", tt.defaults)

				attrs := tt.attrs
				attrs.ActivityType = swf.ActivityTaskActivityType{Name: "at", Version: "1"}
				attrs.ActivityID = "a1"
				attrs.TaskList = ttTaskList
				require.NoError(t, b.RespondDecisionTaskCompleted(token, "", []swf.Decision{
					{DecisionType: "ScheduleActivityTask", ScheduleActivityTaskAttrs: &attrs},
				}))

				var activityToken string
				if tt.poll {
					task := b.PollForActivityTask(ttDomain, ttTaskList)
					require.NotNil(t, task)
					activityToken = task.TaskToken
				}

				for _, s := range tt.steps {
					time.Sleep(s.advance)

					if s.heartbeat {
						_, err := b.RecordActivityTaskHeartbeat(activityToken, "progress")
						require.NoError(t, err)
					}
				}

				events, _ := b.GetWorkflowExecutionHistory(ttDomain, ttWorkflow, "", 0, "", false)
				got := ttEventAttrs(t, events, "ActivityTaskTimedOut", "activityTaskTimedOutEventAttributes")

				if tt.wantType == "" {
					assert.Nil(t, got)

					return
				}

				require.NotNil(t, got)
				assert.Equal(t, tt.wantType, got["timeoutType"])
				assert.Equal(t, tt.wantStarted, got["startedEventId"] != int64(0))
				if tt.wantDetails != "" {
					assert.Equal(t, tt.wantDetails, got["details"])
				}
				assert.Equal(t, 1, b.CountPendingDecisionTasks(ttDomain, ttTaskList))

				if tt.poll {
					err := b.RespondActivityTaskCompleted(activityToken, "late")
					require.ErrorIs(t, err, swf.ErrNotFound)
					_, err = b.RecordActivityTaskHeartbeat(activityToken, "")
					require.ErrorIs(t, err, swf.ErrNotFound)
				} else {
					assert.Zero(t, b.CountPendingActivityTasks(ttDomain, ttTaskList))
				}
			})
		})
	}
}

func TestDecisionTaskTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sticky       string
		stickyTO     string
		wantList     string
		advance      time.Duration
		pollSticky   bool
		wantTimedOut bool
	}{
		{
			name:         "sticky_unstarted_reverts",
			sticky:       "sticky",
			stickyTO:     "5",
			advance:      6 * time.Second,
			wantTimedOut: true,
			wantList:     ttTaskList,
		},
		{name: "sticky_within_timeout", sticky: "sticky", stickyTO: "5", advance: 4 * time.Second, wantList: "sticky"},
		{name: "sticky_permanent", sticky: "sticky", advance: 24 * time.Hour, wantList: "sticky"},
		{
			name: "sticky_started_not_completed", sticky: "sticky", stickyTO: "100", pollSticky: true,
			advance: 11 * time.Second, wantTimedOut: true, wantList: ttTaskList,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := swf.NewInMemoryBackend()
				runID, token := ttStart(t, b, "10", swf.ActivityTypeDefaults{})

				require.NoError(t, b.RespondDecisionTaskCompleted(
					token, "", nil, swf.WithStickyTaskList(tt.sticky, tt.stickyTO),
				))
				require.NoError(t, b.SignalWorkflowExecution(ttDomain, ttWorkflow, runID, "sig", ""))
				require.Equal(t, 1, b.CountPendingDecisionTasks(ttDomain, tt.sticky))

				if tt.pollSticky {
					require.NotNil(t, b.PollForDecisionTask(ttDomain, tt.sticky, 0, "", false))
				}

				time.Sleep(tt.advance)

				events, _ := b.GetWorkflowExecutionHistory(ttDomain, ttWorkflow, runID, 0, "", false)
				got := ttEventAttrs(t, events, "DecisionTaskTimedOut", "decisionTaskTimedOutEventAttributes")
				assert.Equal(t, tt.wantTimedOut, got != nil)
				assert.Equal(t, 1, b.CountPendingDecisionTasks(ttDomain, tt.wantList))
			})
		})
	}
}

func TestDecisionTaskStartToCloseTimeout(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := swf.NewInMemoryBackend()
		runID, token := ttStart(t, b, "10", swf.ActivityTypeDefaults{})

		time.Sleep(11 * time.Second)

		events, _ := b.GetWorkflowExecutionHistory(ttDomain, ttWorkflow, runID, 0, "", false)
		got := ttEventAttrs(t, events, "DecisionTaskTimedOut", "decisionTaskTimedOutEventAttributes")
		require.NotNil(t, got)
		assert.Equal(t, "START_TO_CLOSE", got["timeoutType"])
		assert.Equal(t, 1, b.CountPendingDecisionTasks(ttDomain, ttTaskList))
		require.ErrorIs(t, b.RespondDecisionTaskCompleted(token, "", nil), swf.ErrNotFound)
	})
}

func TestPendingTaskQueuesPersist(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.EnqueueActivityTaskInternal(ttDomain, ttTaskList, "a1", "at", "1", "", "w", "r")
	b.EnqueueActivityTaskInternal(ttDomain, ttTaskList, "a2", "at", "1", "", "w", "r")
	b.EnqueueDecisionTaskInternal(ttDomain, ttTaskList, "w1", "r1")
	b.EnqueueDecisionTaskInternal(ttDomain, ttTaskList, "w2", "r2")

	fresh := swf.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), b.Snapshot(t.Context())))

	assert.Equal(t, "a1", fresh.PollForActivityTask(ttDomain, ttTaskList).ActivityID)
	assert.Equal(t, "a2", fresh.PollForActivityTask(ttDomain, ttTaskList).ActivityID)
	assert.Equal(t, "w1", fresh.PollForDecisionTask(ttDomain, ttTaskList, 0, "", false).WorkflowID)
	assert.Equal(t, "w2", fresh.PollForDecisionTask(ttDomain, ttTaskList, 0, "", false).WorkflowID)
}
