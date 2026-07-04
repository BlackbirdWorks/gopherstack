package stepfunctions_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *stepfunctions.InMemoryBackend) string
		verify func(t *testing.T, b *stepfunctions.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *stepfunctions.InMemoryBackend) string {
				sm, err := b.CreateStateMachine(
					context.Background(),
					"test-sm",
					`{"Comment":"test","StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
					"arn:aws:iam::000000000000:role/test",
					"STANDARD",
				)
				if err != nil {
					return ""
				}

				return sm.StateMachineArn
			},
			verify: func(t *testing.T, b *stepfunctions.InMemoryBackend, id string) {
				t.Helper()

				sm, err := b.DescribeStateMachine(id)
				require.NoError(t, err)
				assert.Equal(t, "test-sm", sm.Name)
				assert.Equal(t, id, sm.StateMachineArn)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *stepfunctions.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *stepfunctions.InMemoryBackend, _ string) {
				t.Helper()

				sms, _, err := b.ListStateMachines(context.Background(), "", 0)
				require.NoError(t, err)
				assert.Empty(t, sms)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestRestore_RebuildsStatusIndex verifies that smExecsByStatus is correctly rebuilt
// from restored executions so that ListExecutions with a status filter works after Restore.
func TestRestore_RebuildsStatusIndex(t *testing.T) {
	t.Parallel()

	const def = `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	const role = "arn:aws:iam::000000000000:role/test"

	original := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	ctx := t.Context()

	sm, err := original.CreateStateMachine(ctx, "index-sm", def, role, "STANDARD")
	require.NoError(t, err)
	smARN := sm.StateMachineArn

	// Manually inject a SUCCEEDED execution via snapshot-level approach:
	// start, wait for completion.
	exec, err := original.StartExecution(smARN, "exec-a", `{}`)
	require.NoError(t, err)
	execARN := exec.ExecutionArn

	// Wait for Pass state to complete.
	require.Eventually(t, func() bool {
		e, _ := original.DescribeExecution(execARN)

		return e != nil && e.Status != "RUNNING"
	}, 5*time.Second, 10*time.Millisecond)

	// Verify status before snapshot.
	e, err := original.DescribeExecution(execARN)
	require.NoError(t, err)
	wantStatus := e.Status

	// Snapshot → restore.
	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	// Status bucket should be populated for the terminal status.
	count := fresh.SMExecsByStatusCountForTest(smARN, wantStatus)
	assert.Equal(t, 1, count, "smExecsByStatus[%s][%s] should have 1 entry after Restore", smARN, wantStatus)

	// ListExecutions with status filter should return the execution.
	execs, _, listErr := fresh.ListExecutions(smARN, wantStatus, "", 0)
	require.NoError(t, listErr)
	require.Len(t, execs, 1)
	assert.Equal(t, execARN, execs[0].ExecutionArn)

	// ListExecutions with a non-matching status filter should return nothing.
	execs2, _, listErr2 := fresh.ListExecutions(smARN, "FAILED", "", 0)
	require.NoError(t, listErr2)
	assert.Empty(t, execs2)
}
