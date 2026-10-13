package swf_test

import (
	"context"
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

type fakeLambda struct {
	err           error
	gotName       string
	gotPayload    string
	result        string
	functionError string
}

func (f *fakeLambda) InvokeLambda(ctx context.Context, name string, payload []byte) ([]byte, string, error) {
	f.gotName, f.gotPayload = name, string(payload)

	if f.err != nil {
		return nil, "", f.err
	}

	return []byte(f.result), f.functionError, ctx.Err()
}

func TestScheduleLambdaFunctionDecision(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lambda      *fakeLambda
		wantAttrs   map[string]any
		name        string
		wantEvent   string
		wantAttrKey string
		noInvoker   bool
	}{
		{
			name: "completed", lambda: &fakeLambda{result: `"ok"`}, wantEvent: "LambdaFunctionCompleted",
			wantAttrKey: "lambdaFunctionCompletedEventAttributes", wantAttrs: map[string]any{"result": `"ok"`},
		},
		{
			name:      "function_error",
			lambda:    &fakeLambda{result: `{"errorType":"Boom","errorMessage":"x"}`, functionError: "Unhandled"},
			wantEvent: "LambdaFunctionFailed", wantAttrKey: "lambdaFunctionFailedEventAttributes",
			wantAttrs: map[string]any{"reason": "Boom"},
		},
		{
			name: "invoke_error", lambda: &fakeLambda{err: fs.ErrNotExist},
			wantEvent: "LambdaFunctionFailed", wantAttrKey: "lambdaFunctionFailedEventAttributes",
			wantAttrs: map[string]any{"details": fs.ErrNotExist.Error()},
		},
		{
			name: "timed_out", lambda: &fakeLambda{err: context.DeadlineExceeded},
			wantEvent: "LambdaFunctionTimedOut", wantAttrKey: "lambdaFunctionTimedOutEventAttributes",
			wantAttrs: map[string]any{"timeoutType": "START_TO_CLOSE"},
		},
		{
			name: "no_invoker", noInvoker: true, wantEvent: "ScheduleLambdaFunctionFailed",
			wantAttrKey: "scheduleLambdaFunctionFailedEventAttributes",
			wantAttrs:   map[string]any{"cause": "LAMBDA_SERVICE_NOT_AVAILABLE_IN_REGION"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			if !tt.noInvoker {
				b.SetLambdaInvoker(tt.lambda)
			}

			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
			})
			require.NoError(t, err)

			token := pollDecisionTask(t, b, "dom", "tasks")
			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", []swf.Decision{{
				DecisionType: "ScheduleLambdaFunction",
				ScheduleLambdaFunctionAttrs: &swf.ScheduleLambdaFunctionDecisionAttrs{
					ID: "l1", Name: "my-fn", Input: `{"a":1}`, StartToCloseTimeout: "30",
				},
			}}))

			var got *swf.HistoryEvent

			require.Eventually(t, func() bool {
				events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
				for i := range events {
					if events[i].EventType == tt.wantEvent {
						got = &events[i]

						return true
					}
				}

				return false
			}, 5*1e9, 5*1e6)

			attrs, ok := got.Attributes[tt.wantAttrKey].(map[string]any)
			require.True(t, ok)

			for k, v := range tt.wantAttrs {
				assert.Equal(t, v, attrs[k])
			}

			if tt.noInvoker {
				return
			}

			assert.Equal(t, "my-fn", tt.lambda.gotName)
			assert.JSONEq(t, `{"a":1}`, tt.lambda.gotPayload)

			// A follow-up decision task is scheduled so the decider sees the outcome.
			assert.NotEmpty(t, pollDecisionTask(t, b, "dom", "tasks"))
		})
	}
}

func TestScheduleLambdaFunctionDecision_IDInUse(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	block := make(chan struct{})
	b.SetLambdaInvoker(blockingLambda(block))
	t.Cleanup(func() { close(block) })

	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(
		swf.StartWorkflowExecutionInput{Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks"},
	)
	require.NoError(t, err)

	d := swf.Decision{
		DecisionType:                "ScheduleLambdaFunction",
		ScheduleLambdaFunctionAttrs: &swf.ScheduleLambdaFunctionDecisionAttrs{ID: "l1", Name: "fn"},
	}
	require.NoError(t, b.RespondDecisionTaskCompleted(pollDecisionTask(t, b, "dom", "tasks"), "", []swf.Decision{d, d}))

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)

	var scheduled int
	var failed *swf.HistoryEvent

	for i := range events {
		switch events[i].EventType {
		case "LambdaFunctionScheduled":
			scheduled++
		case "ScheduleLambdaFunctionFailed":
			failed = &events[i]
		}
	}

	assert.Equal(t, 1, scheduled)
	require.NotNil(t, failed)
	attrs, ok := failed.Attributes["scheduleLambdaFunctionFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ID_ALREADY_IN_USE", attrs["cause"])
}

type blockingLambda chan struct{}

func (c blockingLambda) InvokeLambda(ctx context.Context, _ string, _ []byte) ([]byte, string, error) {
	select {
	case <-c:
	case <-ctx.Done():
	}

	return nil, "", nil
}
