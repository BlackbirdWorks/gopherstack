package stepfunctions_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// gopherstack-r80d batch 10 (required-output-member sweep). Four bugs found
// by reading stepfunctions's nested domain structs against sfn@v1.45.4's
// types.go directly -- the flat op-level scan (23 ops, 54 fields) only
// covers each op's own top-level Output struct and misses required members
// on nested list-item/detail types like HistoryEvent's *EventDetails family
// and MapRun.

// Test_SDKRoundTrip_TaskEventDetails_ResourceFields proves TaskScheduled/
// TaskSucceeded history event details carry their required resource/region/
// parameters fields end to end through the real SDK client's deserializer.
// Before the fix: TaskScheduledEventDetails.Region/Parameters
// (types.go:1311-1339, both "This member is required.") were never set at
// all by execution_history.go's RecordTaskScheduled, and
// TaskSucceededEventDetails.Resource/ResourceType (types.go:1431-1450, both
// required) were never set by RecordTaskSucceeded -- both genuinely
// reachable on every successful Task-state execution, not an edge case.
func Test_SDKRoundTrip_TaskEventDetails_ResourceFields(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	backend.SetLambdaInvoker(&mockLambdaForBackend{})
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("r80d-task-scheduled-sm"),
		Definition: aws.String(taskLambdaDefinition),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"in":1}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: startOut.ExecutionArn,
		})

		return descErr == nil && desc.Status != sfntypes.ExecutionStatusRunning
	}, 5*time.Second, 50*time.Millisecond)

	histOut, err := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)

	var sawScheduled, sawSucceeded bool

	for _, ev := range histOut.Events {
		switch ev.Type {
		case sfntypes.HistoryEventTypeTaskScheduled:
			require.NotNil(t, ev.TaskScheduledEventDetails)
			assert.Equal(t, "us-east-1", aws.ToString(ev.TaskScheduledEventDetails.Region))
			assert.Contains(t, aws.ToString(ev.TaskScheduledEventDetails.Parameters), `"in":1`)
			assert.Equal(t,
				"arn:aws:lambda:us-east-1:000000000000:function:fn",
				aws.ToString(ev.TaskScheduledEventDetails.Resource),
			)
			assert.Equal(t, "lambda", aws.ToString(ev.TaskScheduledEventDetails.ResourceType))
			sawScheduled = true
		case sfntypes.HistoryEventTypeTaskSucceeded:
			require.NotNil(t, ev.TaskSucceededEventDetails)
			assert.Equal(t,
				"arn:aws:lambda:us-east-1:000000000000:function:fn",
				aws.ToString(ev.TaskSucceededEventDetails.Resource),
			)
			assert.Equal(t, "lambda", aws.ToString(ev.TaskSucceededEventDetails.ResourceType))
			sawSucceeded = true
		default:
		}
	}

	assert.True(t, sawScheduled, "expected a TaskScheduled event")
	assert.True(t, sawSucceeded, "expected a TaskSucceeded event")
}

// Test_SDKRoundTrip_TaskFailedEventDetails_ResourceFields proves
// TaskFailedEventDetails.Resource/ResourceType (types.go:1289-1307, both
// required) decode as non-empty through the real SDK client on a failed
// Task state -- previously RecordTaskFailed never set either field.
func Test_SDKRoundTrip_TaskFailedEventDetails_ResourceFields(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	backend.SetLambdaInvoker(&mockLambdaForBackend{returnErr: assert.AnError})
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("r80d-task-failed-sm"),
		Definition: aws.String(taskLambdaDefinition),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: startOut.ExecutionArn,
		})

		return descErr == nil && desc.Status != sfntypes.ExecutionStatusRunning
	}, 5*time.Second, 50*time.Millisecond)

	histOut, err := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)

	var sawFailed bool

	for _, ev := range histOut.Events {
		if ev.Type == sfntypes.HistoryEventTypeTaskFailed {
			require.NotNil(t, ev.TaskFailedEventDetails)
			assert.Equal(t,
				"arn:aws:lambda:us-east-1:000000000000:function:fn",
				aws.ToString(ev.TaskFailedEventDetails.Resource),
			)
			assert.Equal(t, "lambda", aws.ToString(ev.TaskFailedEventDetails.ResourceType))
			sawFailed = true
		}
	}

	assert.True(t, sawFailed, "expected a TaskFailed event")
}

// Test_SDKRoundTrip_DescribeMapRun_ExecutionCounts proves DescribeMapRun's
// required executionCounts (api_op_DescribeMapRun.go:57, "This member is
// required.", types.MapRunExecutionCounts at types.go:841-906) decodes as a
// non-nil pointer, not silently dropped. Before the fix, MapRun had no
// ExecutionCounts field at all, so the key never appeared on the wire and
// the real SDK client's *types.MapRunExecutionCounts pointer stayed nil --
// a client dereferencing out.ExecutionCounts.Total would nil-pointer panic.
// The counts are genuinely zero: this backend never spawns separate child
// workflow executions per Map item (see PARITY.md), so zero is the honest
// value, not a fabricated one.
func Test_SDKRoundTrip_DescribeMapRun_ExecutionCounts(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("r80d-maprun-sm"),
		Definition: aws.String(mapIterStateDef),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`[1,2,3]`),
	})
	require.NoError(t, err)

	var mapRunArn string

	require.Eventually(t, func() bool {
		listOut, listErr := client.ListMapRuns(ctx, &sfnsdk.ListMapRunsInput{
			ExecutionArn: startOut.ExecutionArn,
		})
		if listErr != nil || len(listOut.MapRuns) == 0 {
			return false
		}

		mapRunArn = aws.ToString(listOut.MapRuns[0].MapRunArn)

		return true
	}, 5*time.Second, 50*time.Millisecond)

	descOut, err := client.DescribeMapRun(ctx, &sfnsdk.DescribeMapRunInput{
		MapRunArn: aws.String(mapRunArn),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.ExecutionCounts, "executionCounts must decode as present, not a dropped required field")
	assert.Equal(t, int64(0), descOut.ExecutionCounts.Total)
	require.NotNil(t, descOut.ItemCounts)
}

// Test_SDKRoundTrip_StopExecution_StopDate_AfterSnapshotTimeout proves
// StopExecutionOutput.StopDate (required, no omitempty) decodes as non-nil
// through the real SDK client even for an execution a persistence
// Snapshot/Restore round trip promoted from RUNNING to TIMED_OUT.
//
// PARITY.md's "Reviewed, not a bug" note for StopDate claimed every place
// exec.Status transitions off RUNNING (finalizeExecutionRecordLocked,
// StopExecution itself) sets StopDate in the same statement, so
// StopExecution's already-terminal no-op branch could never observe a nil
// StopDate. That trace missed a third transition: Snapshot (persistence.go)
// promotes any execution still RUNNING at snapshot time to TIMED_OUT (so
// Restore never has to resurrect a non-terminal execution with no running
// goroutine) but, before this fix, left StopDate untouched -- nil, since the
// execution never actually stopped. A client calling StopExecution on such
// an execution after restore hit the no-op branch and got a required field
// back as JSON null.
func Test_SDKRoundTrip_StopExecution_StopDate_AfterSnapshotTimeout(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name: aws.String("r80d-snapshot-timeout-sm"),
		Definition: aws.String(
			`{"StartAt":"W","States":{"W":{"Type":"Wait","Seconds":3600,"End":true}}}`,
		),
		RoleArn: aws.String("arn:aws:iam::000000000000:role/sfn-role"),
		Type:    sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: startOut.ExecutionArn,
		})

		return descErr == nil && desc.Status == sfntypes.ExecutionStatusRunning
	}, 5*time.Second, 50*time.Millisecond)

	// Round-trip through Snapshot/Restore while the execution is still
	// RUNNING -- this is what promotes it to TIMED_OUT.
	snap := backend.Snapshot(ctx)
	require.NotNil(t, snap)

	// The original backend's Wait-state goroutine is still blocked sleeping
	// (3600s); stop it now that the snapshot has been taken so it doesn't
	// leak past the end of the test.
	t.Cleanup(func() { _ = backend.StopExecution(*startOut.ExecutionArn, "", "") })

	restored := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(ctx, snap))

	restoredHandler := stepfunctions.NewHandler(restored)
	restoredClient := newSFNSDKClient(t, restoredHandler)

	desc, err := restoredClient.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	require.Equal(t, sfntypes.ExecutionStatusTimedOut, desc.Status)

	stopOut, err := restoredClient.StopExecution(ctx, &sfnsdk.StopExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	require.NotNil(t, stopOut.StopDate, "StopDate must decode as present, not a dropped required field")
	assert.False(t, stopOut.StopDate.IsZero())
}

// Test_SDKRoundTrip_ValidateStateMachineDefinition_Severity proves a FAIL
// diagnostic carries the required severity field
// (types.go:1559-1586, "This member is required.") through the real SDK
// client. Before the fix, the handler's diagnostic map only ever set
// "message" and "code", leaving Severity as the zero-value empty string
// client-side.
func Test_SDKRoundTrip_ValidateStateMachineDefinition_Severity(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	out, err := client.ValidateStateMachineDefinition(ctx, &sfnsdk.ValidateStateMachineDefinitionInput{
		Definition: aws.String(`{not valid json`),
	})
	require.NoError(t, err)
	require.Equal(t, sfntypes.ValidateStateMachineDefinitionResultCodeFail, out.Result)
	require.NotEmpty(t, out.Diagnostics)
	assert.Equal(t, sfntypes.ValidateStateMachineDefinitionSeverityError, out.Diagnostics[0].Severity)
}
