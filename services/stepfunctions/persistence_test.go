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

// TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip exercises a full
// Snapshot->Restore round trip across every resource type backendSnapshot
// persists (state machines, activities, and executions), including the
// execution's inline history (Phase 3.3 moved history from a separate
// backend map onto Execution itself -- see persistence.go's
// executionSnapshot DTO). It guards against a regression where the DTO
// round trip silently drops a field.
func TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	const def = `{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`
	const role = "arn:aws:iam::000000000000:role/test"

	ctx := t.Context()
	original := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	sm, err := original.CreateStateMachine(ctx, "full-state-sm", def, role, "STANDARD")
	require.NoError(t, err)

	act, err := original.CreateActivity(ctx, "full-state-activity")
	require.NoError(t, err)

	v, err := original.PublishStateMachineVersion(sm.StateMachineArn, "v1", "")
	require.NoError(t, err)

	// Started via the version-qualified ARN so StateMachineVersionArn is
	// non-empty on the Execution record, exercising the persistence DTO
	// field added alongside qualified-ARN execution resolution.
	exec, err := original.StartExecution(v.StateMachineVersionArn, "full-state-exec", `{"k":"v"}`)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		e, describeErr := original.DescribeExecution(exec.ExecutionArn)

		return describeErr == nil && e.Status != "RUNNING"
	}, 5*time.Second, 10*time.Millisecond)

	wantHistory, _, err := original.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
	require.NoError(t, err)
	require.NotEmpty(t, wantHistory, "execution should have recorded at least one history event")

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := stepfunctions.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(ctx, snap))

	// State machine survives the round trip.
	restoredSM, err := fresh.DescribeStateMachine(sm.StateMachineArn)
	require.NoError(t, err)
	assert.Equal(t, sm.Name, restoredSM.Name)
	assert.Equal(t, sm.Definition, restoredSM.Definition)
	assert.Equal(t, sm.RoleArn, restoredSM.RoleArn)

	// Activity survives the round trip.
	restoredAct, err := fresh.DescribeActivity(act.ActivityArn)
	require.NoError(t, err)
	assert.Equal(t, act.Name, restoredAct.Name)

	// Execution survives the round trip, including its inline history.
	restoredExec, err := fresh.DescribeExecution(exec.ExecutionArn)
	require.NoError(t, err)
	assert.Equal(t, exec.ExecutionArn, restoredExec.ExecutionArn)
	assert.Equal(t, sm.StateMachineArn, restoredExec.StateMachineArn)
	assert.Equal(t, v.StateMachineVersionArn, restoredExec.StateMachineVersionArn,
		"StateMachineVersionArn must survive the snapshot/restore round trip")
	assert.JSONEq(t, `{"k":"v"}`, restoredExec.Input)

	gotHistory, _, err := fresh.GetExecutionHistory(exec.ExecutionArn, "", 0, false)
	require.NoError(t, err)
	require.Len(t, gotHistory, len(wantHistory))

	for i, wantEvent := range wantHistory {
		assert.Equal(t, wantEvent.Type, gotHistory[i].Type, "history event %d type mismatch after restore", i)
		assert.Equal(t, wantEvent.ID, gotHistory[i].ID, "history event %d ID mismatch after restore", i)
	}
}

func TestSFNHandler_SnapshotRestore_Delegation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *stepfunctions.InMemoryBackend)
		check func(t *testing.T, b *stepfunctions.InMemoryBackend)
		name  string
	}{
		{
			name: "with_state_machine",
			setup: func(b *stepfunctions.InMemoryBackend) {
				_, _ = b.CreateStateMachine(context.Background(), "snap-sm", sfnPassDefinition, "arn:role", "STANDARD")
			},
			check: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
				t.Helper()

				sms, _, err := b.ListStateMachines(context.Background(), "", 0)
				require.NoError(t, err)
				assert.Len(t, sms, 1)
				assert.Equal(t, "snap-sm", sms[0].Name)
			},
		},
		{
			name:  "empty_backend",
			setup: func(_ *stepfunctions.InMemoryBackend) {},
			check: func(t *testing.T, b *stepfunctions.InMemoryBackend) {
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

			origBackend := stepfunctions.NewInMemoryBackend()
			tt.setup(origBackend)

			h, _ := newSFNHandler(t)
			// Create a handler wrapping origBackend
			origH := stepfunctions.NewHandler(origBackend)

			snap := origH.Snapshot(t.Context())
			require.NotNil(t, snap)

			freshBackend := stepfunctions.NewInMemoryBackend()
			freshH := stepfunctions.NewHandler(freshBackend)
			require.NoError(t, freshH.Restore(t.Context(), snap))

			tt.check(t, freshBackend)

			_ = h
		})
	}
}

// ---- GetExecutionHistory: reverse order ----
