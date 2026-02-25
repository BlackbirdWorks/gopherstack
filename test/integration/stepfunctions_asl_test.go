package integration_test

import (
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
		_, _ = client.DeleteStateMachine(ctx, &sfnsdk.DeleteStateMachineInput{
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
		_, _ = client.DeleteStateMachine(ctx, &sfnsdk.DeleteStateMachineInput{
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
		_, _ = client.DeleteStateMachine(ctx, &sfnsdk.DeleteStateMachineInput{
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
