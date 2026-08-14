package integration_test

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// passThroughDefinition is a minimal ASL state machine that passes input through.
const passThroughDefinition = `{
	"StartAt": "Pass",
	"States": {
		"Pass": {
			"Type": "Pass",
			"End": true
		}
	}
}`

// choiceDefinition is a state machine that branches based on a Choice.
const choiceDefinition = `{
	"StartAt": "Check",
	"States": {
		"Check": {
			"Type": "Choice",
			"Choices": [
				{
					"Variable": "$.status",
					"StringEquals": "active",
					"Next": "Active"
				}
			],
			"Default": "Inactive"
		},
		"Active": {
			"Type": "Pass",
			"End": true,
			"Result": {"branch": "active"}
		},
		"Inactive": {
			"Type": "Pass",
			"End": true,
			"Result": {"branch": "inactive"}
		}
	}
}`

func TestIntegration_StepFunctions_ASL_PassState(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createStepFunctionsClient(t)
	ctx := t.Context()

	smName := "asl-pass-" + uuid.NewString()[:8]

	// Create state machine with valid ASL.
	smOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(passThroughDefinition),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/test"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStateMachine(cleanupCtx, &sfnsdk.DeleteStateMachineInput{
			StateMachineArn: smOut.StateMachineArn,
		})
	})

	// Start execution.
	execName := "exec-" + uuid.NewString()[:8]
	execOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: smOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(`{"key": "value"}`),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, execOut.ExecutionArn)

	// Wait for completion.
	require.Eventually(t, func() bool {
		descOut, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: execOut.ExecutionArn,
		})
		if descErr != nil {
			return false
		}

		return string(descOut.Status) == "SUCCEEDED"
	}, 10*time.Second, 200*time.Millisecond, "execution should succeed")

	// Verify output.
	descOut, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descOut.Output), "key")
}

func TestIntegration_StepFunctions_ASL_Choice(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createStepFunctionsClient(t)
	ctx := t.Context()

	smName := "asl-choice-" + uuid.NewString()[:8]

	smOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(choiceDefinition),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/test"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStateMachine(cleanupCtx, &sfnsdk.DeleteStateMachineInput{
			StateMachineArn: smOut.StateMachineArn,
		})
	})

	// Execute with "active" status - should take the Active branch.
	execOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: smOut.StateMachineArn,
		Name:            aws.String("exec-active-" + uuid.NewString()[:8]),
		Input:           aws.String(`{"status": "active"}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: execOut.ExecutionArn,
		})
		if descErr != nil {
			return false
		}

		return string(desc.Status) == "SUCCEEDED"
	}, 10*time.Second, 200*time.Millisecond)

	descOut, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descOut.Output), "active")

	// Execute with "inactive" status - should take the Default branch.
	execOut2, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: smOut.StateMachineArn,
		Name:            aws.String("exec-inactive-" + uuid.NewString()[:8]),
		Input:           aws.String(`{"status": "idle"}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: execOut2.ExecutionArn,
		})
		if descErr != nil {
			return false
		}

		return string(desc.Status) == "SUCCEEDED"
	}, 10*time.Second, 200*time.Millisecond)

	descOut2, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: execOut2.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(descOut2.Output), "inactive")
}

func TestIntegration_StepFunctions_ASL_Fail(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createStepFunctionsClient(t)
	ctx := t.Context()

	failDef := `{
		"StartAt": "F",
		"States": {
			"F": {
				"Type": "Fail",
				"Error": "TestError",
				"Cause": "intentional failure"
			}
		}
	}`

	smName := "asl-fail-" + uuid.NewString()[:8]

	smOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(failDef),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/test"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStateMachine(cleanupCtx, &sfnsdk.DeleteStateMachineInput{
			StateMachineArn: smOut.StateMachineArn,
		})
	})

	execOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: smOut.StateMachineArn,
		Name:            aws.String("exec-fail-" + uuid.NewString()[:8]),
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: execOut.ExecutionArn,
		})
		if descErr != nil {
			return false
		}

		return string(desc.Status) == "FAILED"
	}, 10*time.Second, 200*time.Millisecond)

	descOut, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "FAILED", string(descOut.Status))
}

// TestIntegration_StepFunctions_InvalidDefinition verifies that CreateStateMachine
// rejects an invalid (empty) ASL definition with an error.
func TestIntegration_StepFunctions_InvalidDefinition(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createStepFunctionsClient(t)
	ctx := t.Context()

	_, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("invalid-sm-" + uuid.NewString()[:8]),
		Definition: aws.String(`{}`),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/test"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.Error(t, err, "creating a state machine with an invalid definition should fail")
}

// TestIntegration_StepFunctions_FullExecution exercises the complete execution lifecycle:
// create, execute, wait for completion, verify output, and inspect execution history
// with correct per-state-type event names.
func TestIntegration_StepFunctions_FullExecution(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createStepFunctionsClient(t)
	ctx := t.Context()

	// A state machine that enriches the input with a constant field via a Pass Result.
	definition := `{
		"StartAt": "Enrich",
		"States": {
			"Enrich": {
				"Type": "Pass",
				"Result": {"enriched": true},
				"ResultPath": "$.meta",
				"End": true
			}
		}
	}`

	smName := "full-exec-" + uuid.NewString()[:8]

	smOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(definition),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/test"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStateMachine(cleanupCtx, &sfnsdk.DeleteStateMachineInput{
			StateMachineArn: smOut.StateMachineArn,
		})
	})

	// Start execution with input.
	execName := "full-exec-" + uuid.NewString()[:8]
	execOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: smOut.StateMachineArn,
		Name:            aws.String(execName),
		Input:           aws.String(`{"original": "data"}`),
	})
	require.NoError(t, err)
	require.NotEmpty(t, execOut.ExecutionArn)

	// Wait for SUCCEEDED.
	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: execOut.ExecutionArn,
		})
		if descErr != nil {
			return false
		}

		return string(desc.Status) == "SUCCEEDED"
	}, 10*time.Second, 200*time.Millisecond, "execution should succeed")

	// Verify final output contains both the original field and the enriched meta.
	descOut, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", string(descOut.Status))

	var output map[string]any
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(descOut.Output)), &output))
	require.Contains(t, output, "original", "output should contain the original input field")
	assert.Equal(t, "data", output["original"])
	meta, ok := output["meta"].(map[string]any)
	require.True(t, ok, "output.meta should be an object")
	require.Contains(t, meta, "enriched", "meta should contain enriched field")
	assert.Equal(t, true, meta["enriched"])

	// Inspect execution history — expect Pass state entry/exit events.
	histOut, err := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, histOut.Events)

	// Collect event types in order.
	eventTypes := make([]string, 0, len(histOut.Events))
	for _, ev := range histOut.Events {
		eventTypes = append(eventTypes, string(ev.Type))
	}

	// History must contain the bookend events.
	assert.Contains(t, eventTypes, "ExecutionStarted")
	assert.Contains(t, eventTypes, "ExecutionSucceeded")

	// Pass state should produce PassStateEntered and PassStateExited — not TaskState* names.
	assert.Contains(t, eventTypes, "PassStateEntered", "Pass state should emit PassStateEntered events")
	assert.Contains(t, eventTypes, "PassStateExited", "Pass state should emit PassStateExited events")
	assert.NotContains(t, eventTypes, "TaskStateEntered", "Pass state must not emit TaskStateEntered")
	assert.NotContains(t, eventTypes, "TaskStateExited", "Pass state must not emit TaskStateExited")

	// Verify the state name in StateEnteredEventDetails is just the state name (not "Enrich(Pass)").
	for _, ev := range histOut.Events {
		if string(ev.Type) == "PassStateEntered" {
			require.NotNil(t, ev.StateEnteredEventDetails)
			assert.Equal(t, "Enrich", aws.ToString(ev.StateEnteredEventDetails.Name),
				"StateEnteredEventDetails.Name should be the state name only, not 'Enrich(Pass)'")
		}
	}
}

// TestIntegration_StepFunctions_ActivityTaskRoundTrip exercises the
// activity-worker path end to end: CreateActivity, a Task state whose
// Resource is the activity ARN, StartExecution, GetActivityTask (the real
// long-poll dequeue), SendTaskSuccess, then verifies both DescribeExecution's
// output and the TaskSucceeded history event carry the worker's real output —
// not just a 200. This path is never exercised by a dispatch-level sweep
// because it requires a worker to actually poll and respond.
func TestIntegration_StepFunctions_ActivityTaskRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createStepFunctionsClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]

	actOut, err := client.CreateActivity(ctx, &sfnsdk.CreateActivityInput{
		Name: aws.String("worker-activity-" + suffix),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(actOut.ActivityArn))
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteActivity(cleanupCtx, &sfnsdk.DeleteActivityInput{
			ActivityArn: actOut.ActivityArn,
		})
	})

	definition := fmt.Sprintf(`{
		"StartAt": "DoWork",
		"States": {
			"DoWork": {
				"Type": "Task",
				"Resource": %q,
				"End": true
			}
		}
	}`, aws.ToString(actOut.ActivityArn))

	smOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("activity-sm-" + suffix),
		Definition: aws.String(definition),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/test"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStateMachine(cleanupCtx, &sfnsdk.DeleteStateMachineInput{
			StateMachineArn: smOut.StateMachineArn,
		})
	})

	execOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: smOut.StateMachineArn,
		Name:            aws.String("exec-" + suffix),
		Input:           aws.String(`{"work":"item-42"}`),
	})
	require.NoError(t, err)

	taskOut, err := client.GetActivityTask(ctx, &sfnsdk.GetActivityTaskInput{
		ActivityArn: actOut.ActivityArn,
		WorkerName:  aws.String("test-worker"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(taskOut.TaskToken))
	assert.JSONEq(t, `{"work":"item-42"}`, aws.ToString(taskOut.Input))

	_, err = client.SendTaskSuccess(ctx, &sfnsdk.SendTaskSuccessInput{
		TaskToken: taskOut.TaskToken,
		Output:    aws.String(`{"result":"done-42"}`),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: execOut.ExecutionArn,
		})
		if descErr != nil {
			return false
		}

		return string(desc.Status) == "SUCCEEDED"
	}, 10*time.Second, 200*time.Millisecond, "execution should succeed after SendTaskSuccess")

	descOut, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.JSONEq(t, `{"result":"done-42"}`, aws.ToString(descOut.Output))

	histOut, err := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)

	var foundTaskSucceeded bool

	for _, ev := range histOut.Events {
		if ev.Type == sfntypes.HistoryEventTypeTaskSucceeded {
			require.NotNil(t, ev.TaskSucceededEventDetails)
			assert.JSONEq(t, `{"result":"done-42"}`, aws.ToString(ev.TaskSucceededEventDetails.Output))

			foundTaskSucceeded = true
		}
	}

	assert.True(t, foundTaskSucceeded, "expected a TaskSucceeded history event carrying the worker's output")
}
