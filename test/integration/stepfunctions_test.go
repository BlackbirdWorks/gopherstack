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

func TestIntegration_StepFunctions_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createStepFunctionsClient(t)
	ctx := t.Context()

	smName := "test-sm-" + uuid.NewString()[:8]
	definition := `{"Comment":"Test","StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}`
	roleArn := "arn:aws:iam::000000000000:role/sfn-role"

	// CreateStateMachine
	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(definition),
		RoleArn:    aws.String(roleArn),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	smArn := *createOut.StateMachineArn

	// StartExecution
	execOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: aws.String(smArn),
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)
	execArn := *execOut.ExecutionArn

	// Wait a moment then DescribeExecution
	time.Sleep(200 * time.Millisecond)

	descOut, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{ExecutionArn: aws.String(execArn)})
	require.NoError(t, err)
	assert.Equal(t, execArn, *descOut.ExecutionArn)

	// GetExecutionHistory
	histOut, err := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{ExecutionArn: aws.String(execArn)})
	require.NoError(t, err)
	assert.NotEmpty(t, histOut.Events)

	// StopExecution (may fail if already completed, ignore)
	_, _ = client.StopExecution(ctx, &sfnsdk.StopExecutionInput{ExecutionArn: aws.String(execArn)})

	// DeleteStateMachine
	_, err = client.DeleteStateMachine(ctx, &sfnsdk.DeleteStateMachineInput{StateMachineArn: aws.String(smArn)})
	require.NoError(t, err)
}
