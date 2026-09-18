package stepfunctions_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// findStateEnteredEvent returns the StateEntered event's details, or nil if
// none is present.
func findStateEnteredEvent(events []sfntypes.HistoryEvent) *sfntypes.StateEnteredEventDetails {
	for _, e := range events {
		if e.StateEnteredEventDetails != nil {
			return e.StateEnteredEventDetails
		}
	}

	return nil
}

// findStateExitedEvent returns the StateExited event's details, or nil if
// none is present.
func findStateExitedEvent(events []sfntypes.HistoryEvent) *sfntypes.StateExitedEventDetails {
	for _, e := range events {
		if e.StateExitedEventDetails != nil {
			return e.StateExitedEventDetails
		}
	}

	return nil
}

// TestGetExecutionHistory_IncludeExecutionData covers gopherstack-xhu2t:
// includeExecutionData was previously dropped entirely (real, documented
// default true -- api_op_GetExecutionHistory.go), so a caller asking to
// omit event input/output payloads with includeExecutionData=false always
// got them back anyway.
func TestGetExecutionHistory_IncludeExecutionData(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("include-data-" + uuid.NewString()[:8]),
		Definition: aws.String(validPassDef),
		RoleArn:    aws.String(validRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{"foo":"bar"}`),
	})
	require.NoError(t, err)

	waitTerminal(ctx, t, client, aws.ToString(startOut.ExecutionArn))

	t.Run("default includes event data", func(t *testing.T) {
		t.Parallel()

		histOut, histErr := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{
			ExecutionArn: startOut.ExecutionArn,
		})
		require.NoError(t, histErr)

		entered := findStateEnteredEvent(histOut.Events)
		require.NotNil(t, entered)
		assert.Contains(t, aws.ToString(entered.Input), "foo")

		exited := findStateExitedEvent(histOut.Events)
		require.NotNil(t, exited)
		assert.NotEmpty(t, aws.ToString(exited.Output))
	})

	t.Run("explicit false omits event data", func(t *testing.T) {
		t.Parallel()

		histOut, histErr := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{
			ExecutionArn:         startOut.ExecutionArn,
			IncludeExecutionData: aws.Bool(false),
		})
		require.NoError(t, histErr)

		entered := findStateEnteredEvent(histOut.Events)
		require.NotNil(t, entered)
		assert.Nil(t, entered.Input)

		exited := findStateExitedEvent(histOut.Events)
		require.NotNil(t, exited)
		assert.Nil(t, exited.Output)
	})
}
