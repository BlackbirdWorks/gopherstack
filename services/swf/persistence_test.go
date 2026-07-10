package swf_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *swf.InMemoryBackend) string
		verify func(t *testing.T, b *swf.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *swf.InMemoryBackend) string {
				err := b.RegisterDomain("test-domain", "test description", "30")
				if err != nil {
					return ""
				}

				return "test-domain"
			},
			verify: func(t *testing.T, b *swf.InMemoryBackend, id string) {
				t.Helper()

				domain, err := b.DescribeDomain(id)
				require.NoError(t, err)
				assert.Equal(t, id, domain.Name)
				assert.Equal(t, "test description", domain.Description)
				assert.Equal(t, "30", domain.WorkflowExecutionRetentionPeriodInDays)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *swf.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *swf.InMemoryBackend, _ string) {
				t.Helper()

				domains, err := b.ListDomains("REGISTERED")
				require.NoError(t, err)
				assert.Empty(t, domains)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := swf.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := swf.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns no
// error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("seed-domain", "", "NONE"))

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	domains, err := b.ListDomains("")
	require.NoError(t, err)
	assert.Empty(t, domains)

	_, err = b.DescribeDomain("seed-domain")
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches swfSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied. This also covers the
// pre-Phase-3.3 on-disk shape (flat maps keyed by hand-built composite
// strings), which lacks "version"/"tables" entirely.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("seed-domain", "", "NONE"))

	// The pre-refactor shape: flat maps, no version/tables keys.
	oldShape := `{"domains":{"seed-domain":{"name":"seed-domain","status":"REGISTERED"}},` +
		`"workflows":{},"activities":{},"executions":{},"executionOrder":[]}`
	err := b.Restore(t.Context(), []byte(oldShape))
	require.NoError(t, err)

	domains, err := b.ListDomains("")
	require.NoError(t, err)
	assert.Empty(t, domains)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (domains, workflows, activities, executions -- all
// "clean" tables with a companion byDomain index on the latter three -- plus
// the "dirty" activeActivityTasks/activeDecisionTasks tables, each carrying a
// hidden taskToken field through a DTO) and every raw map that was already
// part of the pre-Phase-3.3 snapshot (history, tags, executionOrder).
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	const (
		domainName   = "domain-1"
		domainARN    = "arn:aws:swf:us-east-1:123456789012:/domain/" + domainName
		workflowID   = "wf-1"
		taskListName = "list-1"
	)

	original := swf.NewInMemoryBackend()
	ctx := t.Context()

	require.NoError(t, original.RegisterDomain(domainName, "a domain", "30"))

	require.NoError(t, original.RegisterWorkflowType(
		domainName, "wf-type", "1.0", "a workflow type", swf.WorkflowTypeDefaults{},
	))

	require.NoError(t, original.RegisterActivityType(
		domainName, "act-type", "1.0", "an activity type", swf.ActivityTypeDefaults{},
	))

	exec, err := original.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:              domainName,
		WorkflowID:          workflowID,
		WorkflowTypeName:    "wf-type",
		WorkflowTypeVersion: "1.0",
		TaskList:            taskListName,
	})
	require.NoError(t, err)

	require.NoError(t, original.TagResource(domainARN, map[string]string{"env": "test"}))

	original.EnqueueActivityTaskInternal(
		domainName, taskListName, "act-1", "MyActivity", "1.0", "payload", workflowID, exec.RunID,
	)
	activityTask := original.PollForActivityTask(domainName, taskListName)
	require.NotNil(t, activityTask)

	original.EnqueueDecisionTaskInternal(domainName, taskListName, workflowID, exec.RunID)
	decisionTask := original.PollForDecisionTask(domainName, taskListName, 0, "")
	require.NotNil(t, decisionTask)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := swf.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(ctx, snap))

	verifyDomainRestored(t, fresh, domainName)
	verifyWorkflowTypeRestored(t, fresh, domainName)
	verifyActivityTypeRestored(t, fresh, domainName)
	verifyExecutionAndHistoryRestored(t, fresh, domainName, workflowID, exec.RunID)
	verifyTagsRestored(t, fresh, domainARN)
	verifyActiveTasksRestored(t, fresh, activityTask.TaskToken, decisionTask.TaskToken)

	// activityQueues/decisionQueues stay ephemeral -- neither was ever part of
	// backendSnapshot pre-Phase-3.3, and that omission is preserved unchanged
	// by this conversion (see persistence.go's restoreDirtyTablesLocked doc).
	assert.Equal(t, 0, fresh.CountPendingActivityTasks(domainName, taskListName))
	assert.Equal(t, 0, fresh.CountPendingDecisionTasks(domainName, taskListName))
}

// verifyDomainRestored checks the "clean" domains table survived the round trip.
func verifyDomainRestored(t *testing.T, b *swf.InMemoryBackend, domainName string) {
	t.Helper()

	gotDomain, err := b.DescribeDomain(domainName)
	require.NoError(t, err)
	assert.Equal(t, "a domain", gotDomain.Description)
	assert.Equal(t, "30", gotDomain.WorkflowExecutionRetentionPeriodInDays)
}

// verifyWorkflowTypeRestored checks the "clean" workflows table and its
// byDomain index both survived the round trip.
func verifyWorkflowTypeRestored(t *testing.T, b *swf.InMemoryBackend, domainName string) {
	t.Helper()

	gotWT, err := b.DescribeWorkflowType(domainName, "wf-type", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "a workflow type", gotWT.Description)

	wts, err := b.ListWorkflowTypes(domainName, "")
	require.NoError(t, err)
	assert.Len(t, wts, 1)
}

// verifyActivityTypeRestored checks the "clean" activities table and its
// byDomain index both survived the round trip.
func verifyActivityTypeRestored(t *testing.T, b *swf.InMemoryBackend, domainName string) {
	t.Helper()

	gotAT, err := b.DescribeActivityType(domainName, "act-type", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "an activity type", gotAT.Description)

	ats, err := b.ListActivityTypes(domainName, "")
	require.NoError(t, err)
	assert.Len(t, ats, 1)
}

// verifyExecutionAndHistoryRestored checks the "clean" executions table, its
// byDomain index, and the raw (order-sensitive) history map all survived the
// round trip.
func verifyExecutionAndHistoryRestored(t *testing.T, b *swf.InMemoryBackend, domainName, workflowID, runID string) {
	t.Helper()

	gotExec, err := b.DescribeWorkflowExecution(domainName, workflowID)
	require.NoError(t, err)
	assert.Equal(t, runID, gotExec.RunID)

	execs := b.ListOpenWorkflowExecutions(domainName, swf.ExecutionFilter{})
	assert.Len(t, execs, 1)

	events, _ := b.GetWorkflowExecutionHistory(domainName, workflowID, 0, "", false)
	require.NotEmpty(t, events)
	assert.Equal(t, "WorkflowExecutionStarted", events[0].EventType)
}

// verifyTagsRestored checks the raw tags map (keyed by resource ARN) survived
// the round trip.
func verifyTagsRestored(t *testing.T, b *swf.InMemoryBackend, resourceARN string) {
	t.Helper()

	gotTags, err := b.ListTagsForResource(resourceARN)
	require.NoError(t, err)
	assert.Equal(t, "test", gotTags["env"])
}

// verifyActiveTasksRestored checks the "dirty" activeActivityTasks/
// activeDecisionTasks tables survived the round trip: RecordActivityTaskHeartbeat
// and RespondDecisionTaskCompleted only succeed if their taskToken -- and, for
// the heartbeat, the execution it references -- came back intact.
func verifyActiveTasksRestored(t *testing.T, b *swf.InMemoryBackend, activityTaskToken, decisionTaskToken string) {
	t.Helper()

	cancelRequested, err := b.RecordActivityTaskHeartbeat(activityTaskToken)
	require.NoError(t, err)
	assert.False(t, cancelRequested)

	require.NoError(t, b.RespondDecisionTaskCompleted(decisionTaskToken, "", nil))
}
