package asl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSDKCallFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resource string
		want     SDKCall
		wantOK   bool
	}{
		{"sdk", "arn:aws:states:::aws-sdk:s3:putObject", SDKCall{Service: "s3", Action: "putObject"}, true},
		{"sdk_pattern", "arn:aws:states:::aws-sdk:sqs:sendMessage.waitForTaskToken",
			SDKCall{Service: "sqs", Action: "sendMessage", Pattern: "waitForTaskToken"}, true},
		{"optimized_events", "arn:aws:states:::events:putEvents",
			SDKCall{Service: "eventbridge", Action: "putEvents", Optimized: true}, true},
		{"optimized_states_v2", "arn:aws:states:::states:startExecution.sync:2",
			SDKCall{Service: "sfn", Action: "startExecution", Pattern: "sync:2", Optimized: true}, true},
		{"optimized_athena_sync", "arn:aws:states:::athena:startQueryExecution.sync",
			SDKCall{Service: "athena", Action: "startQueryExecution", Pattern: "sync", Optimized: true}, true},
		{"optimized_ecs_sync", "arn:aws:states:::ecs:runTask.sync",
			SDKCall{Service: "ecs", Action: "runTask", Pattern: "sync", Optimized: true}, true},
		{"optimized_glue_sync", "arn:aws:states:::glue:startJobRun.sync",
			SDKCall{Service: "glue", Action: "startJobRun", Pattern: "sync", Optimized: true}, true},
		{"http_invoke", "arn:aws:states:::http:invoke",
			SDKCall{Service: "http", Action: "invoke", Optimized: true}, true},
		{"lambda_arn", "arn:aws:lambda:us-east-1:000000000000:function:f", SDKCall{}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := sdkCallFor(tt.resource)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLambdaExecutedVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   string
		want string
	}{
		{"bare", "fn", "$LATEST"},
		{"qualified_name", "fn:prod", "prod"},
		{"arn", "arn:aws:lambda:us-east-1:000000000000:function:fn", "$LATEST"},
		{"arn_version", "arn:aws:lambda:us-east-1:000000000000:function:fn:3", "3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, lambdaExecutedVersion(tt.fn))
		})
	}
}
