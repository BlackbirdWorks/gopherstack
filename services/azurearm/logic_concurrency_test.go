package azurearm_test

import (
	"context"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func concurrencyDef(trigger map[string]any) map[string]any {
	return logicDef(map[string]any{"manual": trigger}, la{
		"Pause": la{"type": "Wait", "runAfter": la{}, "inputs": la{"interval": la{"count": 5, "unit": "Minute"}}},
	})
}

func concurrentTrigger(extra map[string]any) map[string]any {
	t := map[string]any{"type": "Request", "kind": "Http", "inputs": map[string]any{"method": "POST"}}
	maps.Copy(t, extra)

	return t
}

func runStatuses(t *testing.T, s *logicStack) map[string]int {
	t.Helper()

	out := map[string]int{}
	pager := s.factory.NewWorkflowRunsClient().NewListPager(logicRG, "wf", nil)

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		for _, r := range page.Value {
			out[string(*r.Properties.Status)]++
		}
	}

	return out
}

func waitTimers(t *testing.T, s *logicStack, n int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	for s.clock.pending(true) < n {
		select {
		case <-s.clock.changed:
		case <-ctx.Done():
			require.Failf(t, "timers", "want %d armed timers, have %d", n, s.clock.pending(true))
		}
	}
}

func TestLogicTriggerConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		trigger     map[string]any
		name        string
		wantCodes   []int
		posts       int
		wantRunning int
		wantWaiting int
	}{
		{
			name: "runs limit queues", posts: 3, wantCodes: []int{202, 202, 202}, wantRunning: 1, wantWaiting: 2,
			trigger: concurrentTrigger(map[string]any{"runtimeConfiguration": map[string]any{
				"concurrency": map[string]any{"runs": 1, "maximumWaitingRuns": 5},
			}}),
		},
		{
			name: "waiting limit rejects", posts: 3, wantCodes: []int{202, 202, 429}, wantRunning: 1, wantWaiting: 1,
			trigger: concurrentTrigger(map[string]any{"runtimeConfiguration": map[string]any{
				"concurrency": map[string]any{"runs": 1, "maximumWaitingRuns": 1},
			}}),
		},
		{
			name: "single instance", posts: 3, wantCodes: []int{202, 202, 202}, wantRunning: 1, wantWaiting: 2,
			trigger: concurrentTrigger(map[string]any{"operationOptions": "SingleInstance"}),
		},
		{
			name: "two concurrent", posts: 3, wantCodes: []int{202, 202, 202}, wantRunning: 2, wantWaiting: 1,
			trigger: concurrentTrigger(map[string]any{"runtimeConfiguration": map[string]any{
				"concurrency": map[string]any{"runs": 2},
			}}),
		},
		{
			name: "no concurrency runs all", posts: 3, wantCodes: []int{202, 202, 202}, wantRunning: 3,
			trigger: concurrentTrigger(nil),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			s.create(t, "wf", concurrencyDef(tc.trigger))

			cb := s.callback(t, "wf")

			for i := range tc.posts {
				code, _, _ := s.post(t, cb, "{}")
				assert.Equal(t, tc.wantCodes[i], code, "post %d", i)
			}

			waitTimers(t, s, tc.wantRunning)

			st := runStatuses(t, s)
			assert.Equal(t, tc.wantRunning, st[string(armlogic.WorkflowStatusRunning)])
			assert.Equal(t, tc.wantWaiting, st[string(armlogic.WorkflowStatusWaiting)])
		})
	}
}

func TestLogicTriggerConcurrencyDrainsQueue(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "wf", concurrencyDef(concurrentTrigger(map[string]any{"operationOptions": "SingleInstance"})))

	cb := s.callback(t, "wf")

	for range 2 {
		code, _, _ := s.post(t, cb, "{}")
		require.Equal(t, http.StatusAccepted, code)
	}

	s.clock.waitTimer(t)
	s.clock.Advance(5 * time.Minute)
	s.clock.waitTimer(t)

	st := runStatuses(t, s)
	assert.Equal(t, 1, st[string(armlogic.WorkflowStatusSucceeded)])
	assert.Equal(t, 1, st[string(armlogic.WorkflowStatusRunning)])
	assert.Zero(t, st[string(armlogic.WorkflowStatusWaiting)])

	s.clock.Advance(5 * time.Minute)
	s.waitRuns(t)

	assert.Equal(t, 2, runStatuses(t, s)[string(armlogic.WorkflowStatusSucceeded)])
}

func TestLogicTriggerConcurrencyCancelWaiting(t *testing.T) {
	t.Parallel()

	s := newLogicStack(t)
	s.create(t, "wf", concurrencyDef(concurrentTrigger(map[string]any{"operationOptions": "SingleInstance"})))

	cb := s.callback(t, "wf")

	for range 2 {
		s.post(t, cb, "{}")
	}

	s.clock.waitTimer(t)

	pager := s.factory.NewWorkflowRunsClient().NewListPager(logicRG, "wf", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)

	waiting := ""

	for _, r := range page.Value {
		if *r.Properties.Status == armlogic.WorkflowStatusWaiting {
			waiting = *r.Name
		}
	}

	require.NotEmpty(t, waiting)

	_, err = s.factory.NewWorkflowRunsClient().Cancel(t.Context(), logicRG, "wf", waiting, nil)
	require.NoError(t, err)

	got, err := s.factory.NewWorkflowRunsClient().Get(t.Context(), logicRG, "wf", waiting, nil)
	require.NoError(t, err)
	assert.Equal(t, armlogic.WorkflowStatusCancelled, *got.Properties.Status)

	s.clock.Advance(5 * time.Minute)
	s.waitRuns(t)

	st := runStatuses(t, s)
	assert.Equal(t, 1, st[string(armlogic.WorkflowStatusSucceeded)])
	assert.Equal(t, 1, st[string(armlogic.WorkflowStatusCancelled)])
}
