package swf_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestTimerFired_SDKRoundTrip proves that a StartTimer decision's
// StartToFireTimeout actually fires: real SWF appends a TimerFired history
// event (types.TimerFiredEventAttributes requires startedEventId/timerId,
// confirmed against aws-sdk-go-v2/service/swf@v1.37.4/types/types.go) once
// the timeout elapses, and enqueues a fresh decision task -- StartTimer was
// previously purely decision-driven with no autonomous firing at all (see
// PARITY.md items_still_open). Uses StartToFireTimeout "0" so the timer's
// deadline is already in the past by the time any later call runs the lazy
// sweep (timeout_sweep.go), matching the same clock-free technique already
// used for ChildWorkflowExecutionTimedOut/EXECUTION_START_TO_CLOSE tests.
func TestTimerFired_SDKRoundTrip(t *testing.T) {
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
		WorkflowId: aws.String("wf-timer-fired"),
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

	_, err = client.RespondDecisionTaskCompleted(ctx, &swfsdk.RespondDecisionTaskCompletedInput{
		TaskToken: task.TaskToken,
		Decisions: []swftypes.Decision{
			{
				DecisionType: swftypes.DecisionTypeStartTimer,
				StartTimerDecisionAttributes: &swftypes.StartTimerDecisionAttributes{
					TimerId:            aws.String("timer-1"),
					StartToFireTimeout: aws.String("0"),
				},
			},
		},
	})
	require.NoError(t, err)

	// Any later op runs the lazy sweep -- GetWorkflowExecutionHistory here,
	// same technique the existing EXECUTION_START_TO_CLOSE tests use.
	hist, err := client.GetWorkflowExecutionHistory(ctx, &swfsdk.GetWorkflowExecutionHistoryInput{
		Domain: aws.String("dom"),
		Execution: &swftypes.WorkflowExecution{
			WorkflowId: aws.String("wf-timer-fired"),
			RunId:      started.RunId,
		},
	})
	require.NoError(t, err)

	var (
		started2 *swftypes.HistoryEvent
		fired    *swftypes.HistoryEvent
	)

	for i := range hist.Events {
		if hist.Events[i].EventType == swftypes.EventTypeTimerStarted {
			started2 = &hist.Events[i]
		}

		if hist.Events[i].EventType == swftypes.EventTypeTimerFired {
			fired = &hist.Events[i]
		}
	}

	require.NotNil(t, started2, "expected a TimerStarted event")
	require.NotNil(t, fired, "expected a TimerFired event once StartToFireTimeout elapses")
	require.NotNil(t, fired.TimerFiredEventAttributes)
	require.Equal(t, "timer-1", aws.ToString(fired.TimerFiredEventAttributes.TimerId))
	require.Equal(t, started2.EventId, fired.TimerFiredEventAttributes.StartedEventId)

	// Firing a timer gives the execution a fresh decision task.
	task2, err := client.PollForDecisionTask(ctx, &swfsdk.PollForDecisionTaskInput{
		Domain:   aws.String("dom"),
		TaskList: &swftypes.TaskList{Name: aws.String("default")},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(task2.TaskToken))
}
