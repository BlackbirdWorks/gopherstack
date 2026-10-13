package stepfunctions_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

type callCountingLambda struct {
	calls   map[string]int
	failing map[string]bool
	mu      sync.Mutex
}

func (l *callCountingLambda) InvokeFunction(_ context.Context, name, _ string, _ []byte) ([]byte, int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	name = name[strings.LastIndex(name, ":")+1:]
	l.calls[name]++
	if l.failing[name] {
		return nil, 500, assert.AnError
	}

	return []byte(`{"from":"` + name + `"}`), 200, nil
}

func TestRedriveExecution_ResumesAtFailedState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantCalls  map[string]int
		name       string
		definition string
		failFn     string
	}{
		{
			name: "second task",
			definition: `{"StartAt":"A","States":{` +
				`"A":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:a","Next":"B"},` +
				`"B":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:b","End":true}}}`,
			failFn:    "b",
			wantCalls: map[string]int{"b": 1},
		},
		{
			name: "first task",
			definition: `{"StartAt":"A","States":{` +
				`"A":{"Type":"Task","Resource":"arn:aws:lambda:us-east-1:123456789012:function:a","End":true}}}`,
			failFn:    "a",
			wantCalls: map[string]int{"a": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				lam := &callCountingLambda{calls: map[string]int{}, failing: map[string]bool{tt.failFn: true}}
				b := stepfunctions.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
				b.SetLambdaInvoker(lam)

				sm, err := b.CreateStateMachine(context.Background(), "sm", tt.definition, "arn:role", "STANDARD")
				require.NoError(t, err)
				exec, err := b.StartExecution(sm.StateMachineArn, "e1", `{"in":1}`)
				require.NoError(t, err)
				synctest.Wait()

				got, err := b.DescribeExecution(exec.ExecutionArn)
				require.NoError(t, err)
				require.Equal(t, "FAILED", got.Status)

				lam.mu.Lock()
				lam.failing[tt.failFn] = false
				lam.calls = map[string]int{}
				lam.mu.Unlock()

				before, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
				require.NoError(t, err)

				_, err = b.RedriveExecution(exec.ExecutionArn)
				require.NoError(t, err)
				synctest.Wait()

				got, err = b.DescribeExecution(exec.ExecutionArn)
				require.NoError(t, err)
				assert.Equal(t, "SUCCEEDED", got.Status)
				assert.Equal(t, tt.wantCalls, lam.calls)

				after, _, err := b.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
				require.NoError(t, err)
				assert.Greater(t, len(after), len(before), "history must be kept across redrive")

				var types []string
				for _, ev := range after {
					types = append(types, ev.Type)
				}

				assert.Contains(t, types, "ExecutionRedriven")
				assert.Equal(t, "ExecutionSucceeded", after[len(after)-1].Type)
				require.NotNil(t, before[len(before)-1].ExecutionFailedEventDetails)
				require.NotNil(t, before[0].ExecutionStartedEventDetails)
				assert.JSONEq(t, `{"in":1}`, before[0].ExecutionStartedEventDetails.Input)
			})
		})
	}
}
