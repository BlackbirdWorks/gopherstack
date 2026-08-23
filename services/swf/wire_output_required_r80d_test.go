package swf_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// newTestSWFSDKClient stands up the real aws-sdk-go-v2 swf client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.

// TestPollAndRespondDecisionTaskCompleted_ScheduledStartedEventIds proves the
// required DecisionTaskCompletedEventAttributes.scheduledEventId/startedEventId
// (swf@v1.37.4 types/types.go's DecisionTaskCompletedEventAttributes, "This
// member is required." on both) and PollForDecisionTaskOutput.StartedEventId
// (api_op_PollForDecisionTask.go, also required) survive a real SDK client
// round trip.
//
// Before the fix, enqueueDecisionTaskLocked never recorded a
// DecisionTaskScheduled history event and PollForDecisionTask never recorded
// DecisionTaskStarted, so RespondDecisionTaskCompleted's "DecisionTaskCompleted"
// event carried only executionContext -- scheduledEventId/startedEventId had no
// struct field in the map literal at all (dropped entirely, on literally every
// decision task completion), and PollForDecisionTaskOutput.StartedEventId
// stayed at its Go zero value (0) forever, a value no real event ID can ever
// take (real AWS event IDs start at 1) -- gopherstack-r80d batch 17.
func TestPollAndRespondDecisionTaskCompleted_ScheduledStartedEventIds(t *testing.T) {
	t.Parallel()

	backend := swf.NewInMemoryBackend()
	client := newTestSWFSDKClient(t, swf.NewHandler(backend))
	ctx := t.Context()

	_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
		Name:                                   aws.String("dom"),
		WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
		Domain:  aws.String("dom"),
		Name:    aws.String("wt"),
		Version: aws.String("1.0"),
	})
	require.NoError(t, err)

	started, err := client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
		Domain:     aws.String("dom"),
		WorkflowId: aws.String("wf-1"),
		WorkflowType: &swftypes.WorkflowType{
			Name:    aws.String("wt"),
			Version: aws.String("1.0"),
		},
		TaskList: &swftypes.TaskList{Name: aws.String("default")},
	})
	require.NoError(t, err)

	task, err := client.PollForDecisionTask(ctx, &swfsdk.PollForDecisionTaskInput{
		Domain:   aws.String("dom"),
		TaskList: &swftypes.TaskList{Name: aws.String("default")},
	})
	require.NoError(t, err)
	require.NotNil(t, task.TaskToken)
	require.NotZero(
		t, task.StartedEventId,
		"PollForDecisionTaskOutput.StartedEventId is required and must reference a real event",
	)

	_, err = client.RespondDecisionTaskCompleted(ctx, &swfsdk.RespondDecisionTaskCompletedInput{
		TaskToken: task.TaskToken,
	})
	require.NoError(t, err)

	hist, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
		Domain: aws.String("dom"),
		Execution: &swftypes.WorkflowExecution{
			WorkflowId: aws.String("wf-1"),
			RunId:      started.RunId,
		},
	})
	require.NoError(t, err)

	var completed *swftypes.HistoryEvent
	for i := range hist.Events {
		if hist.Events[i].EventType == swftypes.EventTypeDecisionTaskCompleted {
			completed = &hist.Events[i]
		}
	}
	require.NotNil(t, completed, "expected a DecisionTaskCompleted event")
	require.NotNil(t, completed.DecisionTaskCompletedEventAttributes)
	require.NotZero(t, completed.DecisionTaskCompletedEventAttributes.ScheduledEventId)
	require.NotZero(t, completed.DecisionTaskCompletedEventAttributes.StartedEventId)
}

// TestCancelTimer_StartedEventId proves TimerCanceledEventAttributes.startedEventId
// (swf@v1.37.4 types/types.go, "This member is required.") survives a real SDK
// client round trip after a StartTimer decision is later canceled by a
// CancelTimer decision on the same execution.
//
// Before the fix, handleCancelTimerDecision's "TimerCanceled" event map carried
// only decisionTaskCompletedEventId/timerId -- startedEventId (the TimerStarted
// event this cancellation refers to) had no struct field anywhere on
// WorkflowExecution to source it from, so the key was always absent.
// WorkflowExecution.TimerStartedEventIDs now tracks each open timer's
// TimerStarted event ID, populated in handleStartTimerDecision and consumed
// (then cleared) in handleCancelTimerDecision -- gopherstack-r80d batch 17.
func TestCancelTimer_StartedEventId(t *testing.T) {
	t.Parallel()

	backend := swf.NewInMemoryBackend()
	client := newTestSWFSDKClient(t, swf.NewHandler(backend))
	ctx := t.Context()

	_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
		Name:                                   aws.String("dom"),
		WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
		Domain:  aws.String("dom"),
		Name:    aws.String("wt"),
		Version: aws.String("1.0"),
	})
	require.NoError(t, err)

	started, err := client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
		Domain:     aws.String("dom"),
		WorkflowId: aws.String("wf-timer"),
		WorkflowType: &swftypes.WorkflowType{
			Name:    aws.String("wt"),
			Version: aws.String("1.0"),
		},
		TaskList: &swftypes.TaskList{Name: aws.String("default")},
	})
	require.NoError(t, err)

	task, err := client.PollForDecisionTask(ctx, &swfsdk.PollForDecisionTaskInput{
		Domain:   aws.String("dom"),
		TaskList: &swftypes.TaskList{Name: aws.String("default")},
	})
	require.NoError(t, err)

	// Both decisions ride in one RespondDecisionTaskCompleted call: real AWS
	// (and this backend, see decision_lifecycle_test.go) rejects CancelTimer
	// against a timer that isn't open, and StartTimer alone doesn't enqueue a
	// follow-up decision task -- only activity completion/signals do.
	_, err = client.RespondDecisionTaskCompleted(ctx, &swfsdk.RespondDecisionTaskCompletedInput{
		TaskToken: task.TaskToken,
		Decisions: []swftypes.Decision{
			{
				DecisionType: swftypes.DecisionTypeStartTimer,
				StartTimerDecisionAttributes: &swftypes.StartTimerDecisionAttributes{
					TimerId:            aws.String("timer-1"),
					StartToFireTimeout: aws.String("60"),
				},
			},
			{
				DecisionType: swftypes.DecisionTypeCancelTimer,
				CancelTimerDecisionAttributes: &swftypes.CancelTimerDecisionAttributes{
					TimerId: aws.String("timer-1"),
				},
			},
		},
	})
	require.NoError(t, err)

	hist, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
		Domain: aws.String("dom"),
		Execution: &swftypes.WorkflowExecution{
			WorkflowId: aws.String("wf-timer"),
			RunId:      started.RunId,
		},
	})
	require.NoError(t, err)

	var canceled *swftypes.HistoryEvent
	for i := range hist.Events {
		if hist.Events[i].EventType == swftypes.EventTypeTimerCanceled {
			canceled = &hist.Events[i]
		}
	}
	require.NotNil(t, canceled, "expected a TimerCanceled event")
	require.NotNil(t, canceled.TimerCanceledEventAttributes)
	require.NotZero(t, canceled.TimerCanceledEventAttributes.StartedEventId)
}
