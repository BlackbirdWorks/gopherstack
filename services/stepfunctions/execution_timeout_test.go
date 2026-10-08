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

func TestExecutionTopLevelTimeoutSeconds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		definition string
		wantStatus sfntypes.ExecutionStatus
		wantError  string
		wantEvent  sfntypes.HistoryEventType
	}{
		{
			name: "times out",
			definition: `{"TimeoutSeconds":1,"StartAt":"W","States":{` +
				`"W":{"Type":"Wait","Seconds":60,"End":true}}}`,
			wantStatus: sfntypes.ExecutionStatusTimedOut,
			wantError:  "States.Timeout",
			wantEvent:  sfntypes.HistoryEventTypeExecutionTimedOut,
		},
		{
			name: "finishes in time",
			definition: `{"TimeoutSeconds":30,"StartAt":"P","States":{` +
				`"P":{"Type":"Pass","End":true}}}`,
			wantStatus: sfntypes.ExecutionStatusSucceeded,
			wantEvent:  sfntypes.HistoryEventTypeExecutionSucceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))
			ctx := t.Context()

			sm, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("timeout-sm"),
				Definition: aws.String(tt.definition),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
				Type:       sfntypes.StateMachineTypeStandard,
			})
			require.NoError(t, err)

			start, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{StateMachineArn: sm.StateMachineArn})
			require.NoError(t, err)

			var desc *sfnsdk.DescribeExecutionOutput

			require.Eventually(t, func() bool {
				desc, err = client.DescribeExecution(
					ctx,
					&sfnsdk.DescribeExecutionInput{ExecutionArn: start.ExecutionArn},
				)

				return err == nil && desc.Status != sfntypes.ExecutionStatusRunning
			}, 10*time.Second, 50*time.Millisecond)

			assert.Equal(t, tt.wantStatus, desc.Status)
			assert.Equal(t, tt.wantError, aws.ToString(desc.Error))

			hist, err := client.GetExecutionHistory(
				ctx,
				&sfnsdk.GetExecutionHistoryInput{ExecutionArn: start.ExecutionArn},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEvent, hist.Events[len(hist.Events)-1].Type)
		})
	}
}

func TestCreateStateMachineRejectsBadTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout string
	}{
		{name: "negative", timeout: "-1"},
		{name: "string", timeout: `"soon"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			client := newSFNSDKClient(t, stepfunctions.NewHandler(backend))

			_, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name: aws.String("bad-timeout"),
				Definition: aws.String(
					`{"TimeoutSeconds":` + tt.timeout + `,"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`,
				),
				RoleArn: aws.String("arn:aws:iam::000000000000:role/sfn-role"),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "InvalidDefinition")
		})
	}
}
