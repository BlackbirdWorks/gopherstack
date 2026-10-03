package stepfunctions_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

type countingSDK struct {
	calls atomic.Int64
}

func (c *countingSDK) SFNCallSDK(_ context.Context, call asl.SDKCall) (any, error) {
	c.calls.Add(1)

	return map[string]any{"real": call.Service + ":" + call.Action}, nil
}

func TestSDKIntegration_MockTakesPrecedence(t *testing.T) {
	t.Parallel()

	const cfgJSON = `{
  "StateMachines": {"sdksm": {"TestCases": {"Happy": {"Call": "Ok"}}}},
  "MockedResponses": {"Ok": {"0": {"Return": {"mocked": true}}}}
}`

	tests := []struct {
		name      string
		resource  string
		testCase  string
		wantOut   string
		wantCalls int64
	}{
		{"sdk_resource_mocked", "arn:aws:states:::aws-sdk:sqs:sendMessage", "#Happy", `{"mocked":true}`, 0},
		{"optimized_resource_mocked", "arn:aws:states:::dynamodb:putItem", "#Happy", `{"mocked":true}`, 0},
		{"sdk_resource_unmocked", "arn:aws:states:::aws-sdk:sqs:sendMessage", "", `{"real":"sqs:sendMessage"}`, 1},
		{"optimized_sync_resource_unmocked", "arn:aws:states:::states:startExecution.sync:2", "",
			`{"real":"sfn:startExecution"}`, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := asl.ParseMockConfig([]byte(cfgJSON))
			require.NoError(t, err)

			sdk := &countingSDK{}
			b := stepfunctions.NewInMemoryBackend()
			b.SetMockConfig(cfg)
			b.SetSDKIntegration(sdk)

			client := newJSONataClient(t, stepfunctions.NewHandler(b))
			created, err := client.CreateStateMachine(t.Context(), &sfnsdk.CreateStateMachineInput{
				Name: aws.String("sdksm"),
				Definition: aws.String(
					`{"StartAt":"Call","States":{"Call":{"Type":"Task","Resource":"` + tt.resource + `","End":true}}}`,
				),
				RoleArn: aws.String(testRoleArn),
				Type:    sfntypes.StateMachineTypeExpress,
			})
			require.NoError(t, err)

			out, err := client.StartSyncExecution(t.Context(), &sfnsdk.StartSyncExecutionInput{
				StateMachineArn: aws.String(aws.ToString(created.StateMachineArn) + tt.testCase),
				Input:           aws.String(`{}`),
			})
			require.NoError(t, err)
			assert.Equal(t, sfntypes.SyncExecutionStatusSucceeded, out.Status)
			assert.JSONEq(t, tt.wantOut, aws.ToString(out.Output))
			assert.Equal(t, tt.wantCalls, sdk.calls.Load())
		})
	}
}
