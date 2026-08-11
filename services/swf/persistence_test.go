package swf_test

import (
	"encoding/json"
	"net/http"
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

	gotExec, err := b.DescribeWorkflowExecution(domainName, workflowID, "")
	require.NoError(t, err)
	assert.Equal(t, runID, gotExec.RunID)

	execs := b.ListOpenWorkflowExecutions(domainName, swf.ExecutionFilter{})
	assert.Len(t, execs, 1)

	events, _ := b.GetWorkflowExecutionHistory(domainName, workflowID, "", 0, "", false)
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

// TestSnapshotRestore_ActivityTypes verifies activities survive snapshot/restore.
func TestSnapshotRestore_ActivityTypes(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterActivityType("dom", "act", "1.0", "persisted", swf.ActivityTypeDefaults{}))

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := swf.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	at, err := b2.DescribeActivityType("dom", "act", "1.0")
	require.NoError(t, err)
	assert.Equal(t, "persisted", at.Description)
}

// TestSnapshotRestore_WorkflowTypes verifies workflow types survive snapshot/restore.
func TestSnapshotRestore_WorkflowTypes(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "wf", "2.0", "wf desc", swf.WorkflowTypeDefaults{}))
	require.NoError(t, b.DeprecateWorkflowType("dom", "wf", "2.0"))

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := swf.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	wt, err := b2.DescribeWorkflowType("dom", "wf", "2.0")
	require.NoError(t, err)
	assert.Equal(t, "DEPRECATED", wt.Status)
	assert.Equal(t, "wf desc", wt.Description)
}

// TestSnapshotRestore_ChildLinkAndOpenTimers verifies the fields added to
// WorkflowExecution for the StartChildWorkflowExecution/parent-closure-
// propagation and openTimers work (ParentWorkflowID/ParentRunID/
// ParentInitiatedEventID/ParentStartedEventID/OpenTimerIDs) survive
// snapshot/restore -- executions is a "clean" store.Table (see store.go), so
// these need no special persistence.go wiring, but a restart losing a
// child's parent link or its open timers would silently break
// openChildWorkflowExecutions/openTimers and child-closure propagation after
// every restart, so this is worth locking down explicitly.
func TestSnapshotRestore_ChildLinkAndOpenTimers(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "childType", "1.0", "", swf.WorkflowTypeDefaults{}))

	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "parent-1", TaskList: "parent-tasks",
	})
	require.NoError(t, err)

	parentToken := pollDecisionTask(t, b, "dom", "parent-tasks")
	require.NoError(t, b.RespondDecisionTaskCompleted(parentToken, "", []swf.Decision{
		{
			DecisionType: "StartChildWorkflowExecution",
			StartChildWorkflowExecutionAttrs: &swf.StartChildWorkflowExecutionDecisionAttrs{
				WorkflowID:   "child-1",
				WorkflowType: swf.WorkflowTypeRef{Name: "childType", Version: "1.0"},
				TaskList:     "child-tasks",
			},
		},
		{
			DecisionType:    "StartTimer",
			StartTimerAttrs: &swf.StartTimerDecisionAttrs{TimerID: "t1", StartToFireTimeout: "60"},
		},
	}))

	data := b.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := swf.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	child, err := b2.DescribeWorkflowExecution("dom", "child-1", "")
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", child.Status)

	// openChildWorkflowExecutions/openTimers must reflect the restored
	// ParentWorkflowID/ParentRunID and OpenTimerIDs, not 0 -- these come from
	// the "clean" executions table (see store.go), which round-trips through
	// backendSnapshot automatically, unlike decisionQueues/activityQueues
	// (deliberately ephemeral, see the FullState test above), so this checks
	// state directly rather than through a queue.
	h2 := swf.NewHandler(b2)
	rec := doSWFRequest(t, h2, "DescribeWorkflowExecution", map[string]any{
		"domain":    "dom",
		"execution": map[string]any{"workflowId": "parent-1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	openCounts, ok := body["openCounts"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(
		t,
		1,
		openCounts["openChildWorkflowExecutions"],
		0,
		"ParentWorkflowID/ParentRunID must survive restore",
	)
	assert.InDelta(t, 1, openCounts["openTimers"], 0, "OpenTimerIDs must survive restore")

	// The child-close-propagation link (ParentInitiatedEventID/
	// ParentStartedEventID) must have survived too: completing the child
	// after restore must still notify the parent. decisionQueues is
	// deliberately ephemeral (see the FullState test's comment above), so
	// re-seed the child's decision task the same way that test re-seeds
	// activity/decision tasks post-restore.
	b2.EnqueueDecisionTaskInternal("dom", "child-tasks", "child-1", child.RunID)
	childToken := pollDecisionTask(t, b2, "dom", "child-tasks")
	require.NoError(t, b2.RespondDecisionTaskCompleted(childToken, "", []swf.Decision{{
		DecisionType:                   "CompleteWorkflowExecution",
		CompleteWorkflowExecutionAttrs: &swf.CompleteWorkflowExecutionDecisionAttrs{Result: "done"},
	}}))
	events, _ := b2.GetWorkflowExecutionHistory("dom", "parent-1", "", 0, "", false)
	var sawChildCompleted bool
	for i := range events {
		if events[i].EventType == "ChildWorkflowExecutionCompleted" {
			sawChildCompleted = true
		}
	}
	assert.True(t, sawChildCompleted, "parent link must survive restore for child-closure propagation")
}
