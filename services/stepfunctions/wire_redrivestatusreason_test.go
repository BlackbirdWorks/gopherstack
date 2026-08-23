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

// Test_SDKRoundTrip_RedriveStatusReason proves DescribeExecutionOutput's
// redriveStatusReason (real, populated only when redriveStatus is
// NOT_REDRIVABLE -- aws-sdk-go-v2/service/sfn@v1.45.4
// api_op_DescribeExecution.go) is set to one of AWS's documented reason
// strings when NOT_REDRIVABLE, and left empty when REDRIVABLE. Before this
// fix, models.go declared the field but executions.go never assigned it, so
// a real client always decoded an empty string regardless of redriveStatus.
func Test_SDKRoundTrip_RedriveStatusReason(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		definition  string
		wantStatus  sfntypes.ExecutionStatus
		wantRedrive sfntypes.ExecutionRedriveStatus
		wantReason  string
	}{
		"succeeded": {
			definition:  `{"StartAt": "S", "States": {"S": {"Type": "Pass", "End": true}}}`,
			wantStatus:  sfntypes.ExecutionStatusSucceeded,
			wantRedrive: sfntypes.ExecutionRedriveStatusNotRedrivable,
			wantReason:  "Execution is SUCCEEDED and cannot be redriven.",
		},
		"failed": {
			definition:  `{"StartAt": "S", "States": {"S": {"Type": "Fail"}}}`,
			wantStatus:  sfntypes.ExecutionStatusFailed,
			wantRedrive: sfntypes.ExecutionRedriveStatusRedrivable,
			wantReason:  "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			h := stepfunctions.NewHandler(backend)
			client := newSFNSDKClient(t, h)
			ctx := t.Context()

			createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("redrive-reason-" + name),
				Definition: aws.String(tc.definition),
				RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
				Type:       sfntypes.StateMachineTypeStandard,
			})
			require.NoError(t, err)

			startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
				StateMachineArn: createOut.StateMachineArn,
			})
			require.NoError(t, err)

			var desc *sfnsdk.DescribeExecutionOutput

			require.Eventually(t, func() bool {
				var descErr error

				desc, descErr = client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
					ExecutionArn: startOut.ExecutionArn,
				})

				return descErr == nil && desc.Status != sfntypes.ExecutionStatusRunning
			}, 5*time.Second, 50*time.Millisecond)

			assert.Equal(t, tc.wantStatus, desc.Status)
			assert.Equal(t, tc.wantRedrive, desc.RedriveStatus)
			assert.Equal(t, tc.wantReason, aws.ToString(desc.RedriveStatusReason))
		})
	}
}
