package swf_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// PollForDecisionTaskInput.StartAtPreviousStartedEvent (swf@v1.37.4: "returns
// the events with eventTimestamp greater than or equal to eventTimestamp of the
// most recent DecisionTaskStarted event") was parsed nowhere -- every poll
// returned the execution's entire history regardless of the request.
func TestPollForDecisionTask_StartAtPreviousStartedEvent_RealClient(t *testing.T) {
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

	_, err = client.StartWorkflowExecution(ctx, &swfsdk.StartWorkflowExecutionInput{
		Domain:     aws.String("dom"),
		WorkflowId: aws.String("wf-1"),
		WorkflowType: &swftypes.WorkflowType{
			Name:    aws.String("wt"),
			Version: aws.String("1.0"),
		},
		TaskList: &swftypes.TaskList{Name: aws.String("default")},
	})
	require.NoError(t, err)

	// Events so far: 1 WorkflowExecutionStarted, 2 DecisionTaskScheduled.

	// Decision task 1: no prior DecisionTaskStarted event exists yet.
	task1 := pollDecisionTaskSDK(ctx, t, client, false)
	require.Zero(t, task1.PreviousStartedEventId)
	respondDecisionTaskCompleted(ctx, t, client, task1.TaskToken)
	// Events: 3 DecisionTaskStarted(task1), 4 DecisionTaskCompleted.

	signalWorkflow(ctx, t, client, "wake-up-1")
	// Events: 5 WorkflowExecutionSignaled, 6 DecisionTaskScheduled.

	// Decision task 2, full history requested (the default).
	task2 := pollDecisionTaskSDK(ctx, t, client, false)
	require.Equal(t, task1.StartedEventId, task2.PreviousStartedEventId)
	require.Len(t, task2.Events, 7, "no StartAtPreviousStartedEvent must return the entire history")
	respondDecisionTaskCompleted(ctx, t, client, task2.TaskToken)
	// Events: 7 DecisionTaskStarted(task2), 8 DecisionTaskCompleted.

	signalWorkflow(ctx, t, client, "wake-up-2")
	// Events: 9 WorkflowExecutionSignaled, 10 DecisionTaskScheduled.

	// Decision task 3, trimmed to events since task2's DecisionTaskStarted.
	task3 := pollDecisionTaskSDK(ctx, t, client, true)
	require.Equal(t, task2.StartedEventId, task3.PreviousStartedEventId)
	require.Len(t, task3.Events, 5,
		"StartAtPreviousStartedEvent must trim events before the previous DecisionTaskStarted event")
	require.Equal(t, swftypes.EventTypeDecisionTaskStarted, task3.Events[0].EventType)
	require.Equal(t, task2.StartedEventId, task3.Events[0].EventId)
}

func pollDecisionTaskSDK(
	ctx context.Context,
	t *testing.T,
	client *swfsdk.Client,
	startAtPreviousStartedEvent bool,
) *swfsdk.PollForDecisionTaskOutput {
	t.Helper()

	out, err := client.PollForDecisionTask(ctx, &swfsdk.PollForDecisionTaskInput{
		Domain:                      aws.String("dom"),
		TaskList:                    &swftypes.TaskList{Name: aws.String("default")},
		StartAtPreviousStartedEvent: startAtPreviousStartedEvent,
	})
	require.NoError(t, err)
	require.NotNil(t, out.TaskToken)

	return out
}

func respondDecisionTaskCompleted(ctx context.Context, t *testing.T, client *swfsdk.Client, taskToken *string) {
	t.Helper()

	_, err := client.RespondDecisionTaskCompleted(ctx, &swfsdk.RespondDecisionTaskCompletedInput{
		TaskToken: taskToken,
	})
	require.NoError(t, err)
}

func signalWorkflow(ctx context.Context, t *testing.T, client *swfsdk.Client, signalName string) {
	t.Helper()

	_, err := client.SignalWorkflowExecution(ctx, &swfsdk.SignalWorkflowExecutionInput{
		Domain:     aws.String("dom"),
		WorkflowId: aws.String("wf-1"),
		SignalName: aws.String(signalName),
	})
	require.NoError(t, err)
}
