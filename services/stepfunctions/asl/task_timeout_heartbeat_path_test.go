package asl_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// TestExecutor_Task_TimeoutSecondsPath covers Task.TimeoutSecondsPath
// (gopherstack-vkrn): the parser had no field for it, so encoding/json
// silently discarded it and the Task never timed out. synctest gives exact
// virtual timing, so elapsed duration proves the resolved value -- not just
// that a Catch fired -- actually gated the deadline.
func TestExecutor_Task_TimeoutSecondsPath(t *testing.T) {
	t.Parallel()

	t.Run("timeout_seconds_path_times_out_task", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			def := `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"TimeoutSecondsPath": "$.timeout",
						"Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "TimedOut"}],
						"End": true
					},
					"TimedOut": {"Type": "Pass", "End": true, "Result": "timeout"}
				}
			}`

			sm, err := asl.Parse(def)
			require.NoError(t, err)

			lambda := &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
				<-ctx.Done()

				return nil, 0, ctx.Err()
			}}

			exec := asl.NewExecutor(sm, lambda, nil)

			start := time.Now()
			result, execErr := exec.Execute(t.Context(), "test", `{"timeout": 1}`)
			elapsed := time.Since(start)

			require.NoError(t, execErr)
			require.Empty(t, result.Error)
			assert.Equal(t, "timeout", result.Output)
			assert.Equal(t, time.Second, elapsed)
		})
	})

	t.Run("timeout_seconds_path_wins_over_literal", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			def := `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"TimeoutSeconds": 100,
						"TimeoutSecondsPath": "$.timeout",
						"Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "TimedOut"}],
						"End": true
					},
					"TimedOut": {"Type": "Pass", "End": true, "Result": "timeout"}
				}
			}`

			sm, err := asl.Parse(def)
			require.NoError(t, err)

			lambda := &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
				<-ctx.Done()

				return nil, 0, ctx.Err()
			}}

			exec := asl.NewExecutor(sm, lambda, nil)

			start := time.Now()
			result, execErr := exec.Execute(t.Context(), "test", `{"timeout": 1}`)
			elapsed := time.Since(start)

			require.NoError(t, execErr)
			require.Empty(t, result.Error)
			assert.Equal(t, "timeout", result.Output)
			assert.Equal(
				t,
				time.Second,
				elapsed,
				"TimeoutSecondsPath (1s) must win over the literal TimeoutSeconds (100s)",
			)
		})
	})

	t.Run("timeout_seconds_path_invalid_value_errors", func(t *testing.T) {
		t.Parallel()

		def := `{
			"StartAt": "T",
			"States": {
				"T": {
					"Type": "Task",
					"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
					"TimeoutSecondsPath": "$.timeout",
					"End": true
				}
			}
		}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)

		lambda := &mockLambdaFnCtx{fn: func(_ context.Context) ([]byte, int, error) {
			return []byte(`{}`), 200, nil
		}}

		exec := asl.NewExecutor(sm, lambda, nil)
		_, execErr := exec.Execute(t.Context(), "test", `{"timeout": "oops"}`)
		require.Error(t, execErr)
	})

	t.Run("timeout_seconds_path_resets_per_retry_attempt", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			def := `{
				"StartAt": "T",
				"States": {
					"T": {
						"Type": "Task",
						"Resource": "arn:aws:lambda:us-east-1:000000000000:function:fn",
						"TimeoutSecondsPath": "$.timeout",
						"Retry": [{"ErrorEquals": ["States.ALL"], "MaxAttempts": 2, "IntervalSeconds": 0}],
						"Catch": [{"ErrorEquals": ["States.Timeout"], "Next": "TimedOut"}],
						"End": true
					},
					"TimedOut": {"Type": "Pass", "End": true, "Result": "timeout"}
				}
			}`

			sm, err := asl.Parse(def)
			require.NoError(t, err)

			var calls int

			lambda := &mockLambdaFnCtx{fn: func(ctx context.Context) ([]byte, int, error) {
				calls++
				<-ctx.Done()

				return nil, 0, ctx.Err()
			}}

			exec := asl.NewExecutor(sm, lambda, nil)

			start := time.Now()
			result, execErr := exec.Execute(t.Context(), "test", `{"timeout": 1}`)
			elapsed := time.Since(start)

			require.NoError(t, execErr)
			require.Empty(t, result.Error)
			assert.Equal(t, "timeout", result.Output)
			assert.Equal(t, 3, calls, "initial attempt plus 2 retries")
			// 3 attempts x 1s TimeoutSecondsPath each, 0s between retries: if the
			// deadline were shared across attempts instead of reset per attempt,
			// only the first attempt would ever run and elapsed would be ~1s.
			assert.Equal(t, 3*time.Second, elapsed)
		})
	})
}

// fakeHeartbeatWait mimics InMemoryBackend.WaitForTaskToken's heartbeat-timer
// algorithm (services/stepfunctions/activities.go) without a real activity
// worker: it never simulates a heartbeat/completion arriving, so it always
// times out after heartbeatSeconds, or when ctx is otherwise cancelled.
type fakeHeartbeatWait struct{}

var errFakeHeartbeatTimeout = errors.New("fake States.HeartbeatTimeout")

func (fakeHeartbeatWait) WaitForTaskToken(ctx context.Context, _ string, heartbeatSeconds int) (string, error) {
	if heartbeatSeconds <= 0 {
		<-ctx.Done()

		return "", ctx.Err()
	}

	timer := time.NewTimer(time.Duration(heartbeatSeconds) * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		return "", errFakeHeartbeatTimeout
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// TestExecutor_Task_HeartbeatSecondsPath covers Task.HeartbeatSecondsPath
// (gopherstack-vkrn): same unmodelled-field defect as TimeoutSecondsPath.
// fakeHeartbeatWait enforces a real heartbeat-timeout window rather than
// merely recording the resolved value, so exact elapsed virtual time proves
// the path's value actually gated when the task gave up.
func TestExecutor_Task_HeartbeatSecondsPath(t *testing.T) {
	t.Parallel()

	t.Run("heartbeat_seconds_path_times_out_task", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			def := `{
				"StartAt": "Send",
				"States": {
					"Send": {
						"Type": "Task",
						"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
						"HeartbeatSecondsPath": "$.heartbeat",
						"Catch": [{"ErrorEquals": ["States.ALL"], "Next": "TimedOut"}],
						"End": true
					},
					"TimedOut": {"Type": "Pass", "End": true, "Result": "heartbeat-timeout"}
				}
			}`

			sm, err := asl.Parse(def)
			require.NoError(t, err)

			exec := asl.NewExecutor(sm, nil, nil)
			exec.SetSQSIntegration(&mockSQS{returnMsgID: "msg-id", returnMD5: "md5"})
			exec.SetTaskTokenCallbackInvoker(fakeHeartbeatWait{})

			input := `{
				"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
				"MessageBody": "hello",
				"heartbeat": 1
			}`

			start := time.Now()
			result, execErr := exec.Execute(t.Context(), "test", input)
			elapsed := time.Since(start)

			require.NoError(t, execErr)
			require.Empty(t, result.Error)
			assert.Equal(t, "heartbeat-timeout", result.Output)
			assert.Equal(t, time.Second, elapsed)
		})
	})

	t.Run("heartbeat_seconds_path_resolves_from_input", func(t *testing.T) {
		t.Parallel()

		def := `{
			"StartAt": "Send",
			"States": {
				"Send": {
					"Type": "Task",
					"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
					"HeartbeatSecondsPath": "$.heartbeat",
					"End": true
				}
			}
		}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)

		exec := asl.NewExecutor(sm, nil, nil)
		exec.SetSQSIntegration(&mockSQS{returnMsgID: "msg-id", returnMD5: "md5"})

		callback := &mockTaskTokenCallback{returnOutput: `{"callback":"ok"}`}
		exec.SetTaskTokenCallbackInvoker(callback)

		input := `{
			"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
			"MessageBody": "hello",
			"heartbeat": 42
		}`

		result, execErr := exec.Execute(t.Context(), "test", input)
		require.NoError(t, execErr)
		require.Empty(t, result.Error)
		assert.Equal(t, 42, callback.lastHeartbeat)
	})

	t.Run("heartbeat_seconds_path_invalid_value_errors", func(t *testing.T) {
		t.Parallel()

		def := `{
			"StartAt": "Send",
			"States": {
				"Send": {
					"Type": "Task",
					"Resource": "arn:aws:states:::sqs:sendMessage.waitForTaskToken",
					"HeartbeatSecondsPath": "$.heartbeat",
					"End": true
				}
			}
		}`

		sm, err := asl.Parse(def)
		require.NoError(t, err)

		exec := asl.NewExecutor(sm, nil, nil)
		exec.SetSQSIntegration(&mockSQS{returnMsgID: "msg-id", returnMD5: "md5"})
		exec.SetTaskTokenCallbackInvoker(&mockTaskTokenCallback{})

		input := `{
			"QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/myqueue",
			"MessageBody": "hello",
			"heartbeat": "oops"
		}`

		_, execErr := exec.Execute(t.Context(), "test", input)
		require.Error(t, execErr)
	})
}
