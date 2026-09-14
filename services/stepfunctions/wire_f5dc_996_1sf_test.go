package stepfunctions_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// waitTerminal blocks until execArn leaves RUNNING, via require.Eventually
// rather than a fixed sleep.
func waitTerminal(ctx context.Context, t *testing.T, client *sfnsdk.Client, execArn string) {
	t.Helper()

	require.Eventually(t, func() bool {
		desc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
			ExecutionArn: aws.String(execArn),
		})

		return err == nil && desc.Status != sfntypes.ExecutionStatusRunning
	}, 5*time.Second, 20*time.Millisecond)
}

// Test_SDKRoundTrip_InputOutputDetails_Included proves DescribeExecutionOutput's
// InputDetails/OutputDetails decode Included=true through the real SDK
// client. CloudWatchEventsExecutionDataDetails's only member is Included,
// not Truncated (sfn@v1.49.0 types.go:159-165: "Indicates whether input or
// output was included in the response. Always true for API calls."); before
// this fix, models.go's struct wire-tagged the field "truncated", so the
// real deserializer (which only recognizes "included") never populated it
// and this always decoded false regardless of emulator behavior.
func Test_SDKRoundTrip_InputOutputDetails_Included(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("f5dc-included-" + uuid.NewString()[:8]),
		Definition: aws.String(`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`),
		RoleArn:    aws.String(validRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	waitTerminal(ctx, t, client, aws.ToString(startOut.ExecutionArn))

	desc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.InputDetails)
	assert.True(t, desc.InputDetails.Included)
	require.NotNil(t, desc.OutputDetails)
	assert.True(t, desc.OutputDetails.Included)
}

// Test_SDKRoundTrip_TraceHeader_Echoed proves StartExecutionInput.TraceHeader
// (api_op_StartExecution.go: "Passes the X-Ray trace header ... can also be
// passed in the request payload") is parsed and echoed back on
// DescribeExecutionOutput.TraceHeader.
func Test_SDKRoundTrip_TraceHeader_Echoed(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("f5dc-trace-" + uuid.NewString()[:8]),
		Definition: aws.String(`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`),
		RoleArn:    aws.String(validRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	const trace = "Root=1-5759e988-bd862e3fe1be46a994272793"

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createOut.StateMachineArn,
		Input:           aws.String(`{}`),
		TraceHeader:     aws.String(trace),
	})
	require.NoError(t, err)

	desc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t, trace, aws.ToString(desc.TraceHeader))
}

// Test_SDKRoundTrip_DistributedMapChild_MapRunArn confirms MapRunArn is
// carried on a Distributed Map child's own DescribeExecution response and
// absent on the parent's -- gopherstack-8j8/zov6 (e042fa9c0), predates this
// session; kept here as a regression guard alongside the sibling fixes in
// this file.
func Test_SDKRoundTrip_DistributedMapChild_MapRunArn(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	execArn, _ := runMapExecution(ctx, t, client, distributedMapDef, "f5dc-maprunarn")

	parentDesc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: aws.String(execArn),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(parentDesc.MapRunArn), "a top-level execution must have no MapRunArn")

	listMR, err := client.ListMapRuns(ctx, &sfnsdk.ListMapRunsInput{ExecutionArn: aws.String(execArn)})
	require.NoError(t, err)
	require.Len(t, listMR.MapRuns, 1)

	listExec, err := client.ListExecutions(ctx, &sfnsdk.ListExecutionsInput{
		MapRunArn: listMR.MapRuns[0].MapRunArn,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listExec.Executions)

	childDesc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: listExec.Executions[0].ExecutionArn,
	})
	require.NoError(t, err)
	assert.Equal(t,
		aws.ToString(listMR.MapRuns[0].MapRunArn),
		aws.ToString(childDesc.MapRunArn),
		"a Distributed Map child execution must carry its MapRunArn",
	)
}

// lambdaInvokeTaskDefinition is a Task state using the States service-
// integration ARN form ("arn:aws:states:::lambda:invoke"), distinct from
// taskLambdaDefinition's direct Lambda function ARN form -- the two carry
// different TaskScheduledEventDetails.Resource semantics (split action vs.
// full ARN, see historyResourceValue).
const lambdaInvokeTaskDefinition = `{
"StartAt": "Invoke",
"States": {
"Invoke": {
	"Type": "Task",
	"Resource": "arn:aws:states:::lambda:invoke",
	"Parameters": {"FunctionName": "fn", "Payload": {"x": 1}},
	"TimeoutSeconds": 30,
	"HeartbeatSeconds": 5,
	"End": true
}
}
}`

// lambdaInvokeTaskDefinitionNoTimeouts is lambdaInvokeTaskDefinition without
// TimeoutSeconds/HeartbeatSeconds, to confirm they stay omitted (nil) rather
// than a fabricated 0 when the Task state never set them.
const lambdaInvokeTaskDefinitionNoTimeouts = `{
"StartAt": "Invoke",
"States": {
"Invoke": {
	"Type": "Task",
	"Resource": "arn:aws:states:::lambda:invoke",
	"Parameters": {"FunctionName": "fn", "Payload": {"x": 1}},
	"End": true
}
}
}`

// Test_SDKRoundTrip_TaskScheduled_ResourceSplitAndTimeouts proves
// TaskScheduledEventDetails.Resource is the split action ("invoke"), not
// the raw service-integration ARN, for a "arn:aws:states:::lambda:invoke"
// Task state (sfn@v1.49.0 types.go:1311-1339's Resource: "The action of the
// resource called by a task state."), and that TimeoutInSeconds/
// HeartbeatInSeconds reflect the Task state's own TimeoutSeconds/
// HeartbeatSeconds when set, nil otherwise.
func Test_SDKRoundTrip_TaskScheduled_ResourceSplitAndTimeouts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTimeout   *int64
		wantHeartbeat *int64
		name          string
		definition    string
	}{
		{
			name:          "timeout and heartbeat set",
			definition:    lambdaInvokeTaskDefinition,
			wantTimeout:   aws.Int64(30),
			wantHeartbeat: aws.Int64(5),
		},
		{
			name:       "timeout and heartbeat unset",
			definition: lambdaInvokeTaskDefinitionNoTimeouts,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			backend.SetLambdaInvoker(&mockLambdaForBackend{})
			h := stepfunctions.NewHandler(backend)
			client := newSFNSDKClient(t, h)
			ctx := t.Context()

			createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
				Name:       aws.String("f5dc996-" + uuid.NewString()[:8]),
				Definition: aws.String(tt.definition),
				RoleArn:    aws.String(validRoleARN),
				Type:       sfntypes.StateMachineTypeStandard,
			})
			require.NoError(t, err)

			startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
				StateMachineArn: createOut.StateMachineArn,
				Input:           aws.String(`{}`),
			})
			require.NoError(t, err)

			waitTerminal(ctx, t, client, aws.ToString(startOut.ExecutionArn))

			histOut, err := client.GetExecutionHistory(ctx, &sfnsdk.GetExecutionHistoryInput{
				ExecutionArn: startOut.ExecutionArn,
			})
			require.NoError(t, err)

			var details *sfntypes.TaskScheduledEventDetails
			for _, ev := range histOut.Events {
				if ev.Type == sfntypes.HistoryEventTypeTaskScheduled {
					details = ev.TaskScheduledEventDetails
				}
			}
			require.NotNil(t, details, "expected a TaskScheduled event")

			assert.Equal(t, "invoke", aws.ToString(details.Resource))
			assert.Equal(t, "lambda", aws.ToString(details.ResourceType))
			assert.Equal(t, "us-east-1", aws.ToString(details.Region))
			assert.Contains(t, aws.ToString(details.Parameters), `"FunctionName":"fn"`)
			assert.Equal(t, tt.wantTimeout, details.TimeoutInSeconds)
			assert.Equal(t, tt.wantHeartbeat, details.HeartbeatInSeconds)
		})
	}
}

// blockingLambda blocks InvokeFunction on release, keeping an execution
// RUNNING for as long as a test needs to observe that state deterministically
// (no time.Sleep in either the test or the SUT's own scheduling).
type blockingLambda struct {
	release chan struct{}
}

func (m *blockingLambda) InvokeFunction(
	ctx context.Context, _, _ string, _ []byte,
) ([]byte, int, error) {
	select {
	case <-m.release:
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}

	return []byte(`{"ok":true}`), 200, nil
}

// Test_StartExecution_NameReuseSemantics covers gopherstack-1sf: EXPRESS
// names may be reused immediately, STANDARD reuse with the same input on a
// still-RUNNING execution returns that same execution (StartExecution is
// idempotent for STANDARD -- api_op_StartExecution.go doc on Name), and
// STANDARD reuse with a different input raises ExecutionAlreadyExists.
func Test_StartExecution_NameReuseSemantics(t *testing.T) {
	t.Parallel()

	t.Run("express reuse succeeds immediately", func(t *testing.T) {
		t.Parallel()

		backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
		h := stepfunctions.NewHandler(backend)
		client := newSFNSDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
			Name:       aws.String("1sf-express-" + uuid.NewString()[:8]),
			Definition: aws.String(`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`),
			RoleArn:    aws.String(validRoleARN),
			Type:       sfntypes.StateMachineTypeExpress,
		})
		require.NoError(t, err)

		const name = "reused-express-name"

		first, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Name:            aws.String(name),
			Input:           aws.String(`{"n":1}`),
		})
		require.NoError(t, err)

		second, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Name:            aws.String(name),
			Input:           aws.String(`{"n":2}`),
		})
		require.NoError(t, err, "EXPRESS must allow immediate name reuse, unlike STANDARD")
		assert.NotEmpty(t, aws.ToString(second.ExecutionArn))
		assert.Equal(t, aws.ToString(first.ExecutionArn), aws.ToString(second.ExecutionArn),
			"execution ARNs are deterministic from name -- EXPRESS reuse targets the same ARN")
	})

	t.Run("standard reuse same input returns existing execution", func(t *testing.T) {
		t.Parallel()

		backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
		lambda := &blockingLambda{release: make(chan struct{})}
		backend.SetLambdaInvoker(lambda)
		h := stepfunctions.NewHandler(backend)
		client := newSFNSDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
			Name:       aws.String("1sf-standard-same-" + uuid.NewString()[:8]),
			Definition: aws.String(taskLambdaDefinition),
			RoleArn:    aws.String(validRoleARN),
			Type:       sfntypes.StateMachineTypeStandard,
		})
		require.NoError(t, err)

		const name = "reused-standard-name"

		first, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Name:            aws.String(name),
			Input:           aws.String(`{"in":1}`),
		})
		require.NoError(t, err)

		second, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Name:            aws.String(name),
			Input:           aws.String(`{"in":1}`),
		})
		require.NoError(t, err, "same name+input against a RUNNING STANDARD execution must be idempotent")
		assert.Equal(t, aws.ToString(first.ExecutionArn), aws.ToString(second.ExecutionArn))

		close(lambda.release)
		waitTerminal(ctx, t, client, aws.ToString(first.ExecutionArn))
	})

	t.Run("standard reuse different input conflicts", func(t *testing.T) {
		t.Parallel()

		backend := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
		lambda := &blockingLambda{release: make(chan struct{})}
		backend.SetLambdaInvoker(lambda)
		h := stepfunctions.NewHandler(backend)
		client := newSFNSDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
			Name:       aws.String("1sf-standard-diff-" + uuid.NewString()[:8]),
			Definition: aws.String(taskLambdaDefinition),
			RoleArn:    aws.String(validRoleARN),
			Type:       sfntypes.StateMachineTypeStandard,
		})
		require.NoError(t, err)

		const name = "reused-standard-name-conflict"

		first, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Name:            aws.String(name),
			Input:           aws.String(`{"in":1}`),
		}, func(o *sfnsdk.Options) { o.RetryMaxAttempts = 1 })
		require.NoError(t, err)

		_, err = client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
			StateMachineArn: createOut.StateMachineArn,
			Name:            aws.String(name),
			Input:           aws.String(`{"in":2}`),
		}, func(o *sfnsdk.Options) { o.RetryMaxAttempts = 1 })
		require.Error(t, err)

		var alreadyExists *sfntypes.ExecutionAlreadyExists
		require.ErrorAs(t, err, &alreadyExists,
			"expected a real ExecutionAlreadyExists from the SDK deserializer, got: %v", err)

		close(lambda.release)
		waitTerminal(ctx, t, client, aws.ToString(first.ExecutionArn))
	})
}
