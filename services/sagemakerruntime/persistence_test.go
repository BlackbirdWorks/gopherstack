package sagemakerruntime_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

// newFullPersistenceTestBackend creates a backend with one populated entry
// in every store.Table (sessions, asyncInvocations -- see
// services/sagemakerruntime/store_setup.go) plus the raw invocations history
// left un-converted, so a Snapshot from it exercises the entire persisted
// surface of the backend.
func newFullPersistenceTestBackend(t *testing.T) *sagemakerruntime.InMemoryBackend {
	t.Helper()

	b := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")

	b.RecordInvocation("InvokeEndpoint", "ep", `{"seq":0}`, `{}`)
	b.StartSession("ep")
	b.RecordAsyncInvocation("ep", "full-inference-id", "input", "s3://bucket/output")

	return b
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (sessions, asyncInvocations) and the raw invocations history
// left un-converted through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newFullPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	wantInvocations := original.ListInvocations()
	gotInvocations := restored.ListInvocations()
	assert.Equal(t, wantInvocations, gotInvocations)

	wantSessions := original.ListSessions()
	gotSessions := restored.ListSessions()
	require.Len(t, gotSessions, 1)
	require.Len(t, wantSessions, 1)
	assert.Equal(t, wantSessions[0].ID, gotSessions[0].ID)
	assert.Equal(t, wantSessions[0].EndpointName, gotSessions[0].EndpointName)

	wantAsync := original.ListAsyncInvocations()
	gotAsync := restored.ListAsyncInvocations()
	require.Len(t, gotAsync, 1)
	require.Len(t, wantAsync, 1)
	assert.Equal(t, wantAsync[0], gotAsync[0])

	// A fresh session started against the restored backend must not collide
	// with (or otherwise be perturbed by) the restored nextID counter.
	newSession := restored.StartSession("ep-2")
	assert.NotEmpty(t, newSession.ID)
	assert.Len(t, restored.ListSessions(), 2)
}

// TestInMemoryBackend_SnapshotRestore_EmptyState round-trips a backend with
// no recorded state.
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	assert.Empty(t, restored.ListInvocations())
	assert.Empty(t, restored.ListSessions())
	assert.Empty(t, restored.ListAsyncInvocations())
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newFullPersistenceTestBackend(t)

	// A syntactically valid but version-mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListInvocations())
	assert.Empty(t, b.ListSessions())
	assert.Empty(t, b.ListAsyncInvocations())
}

// TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero verifies that a
// pre-Phase-3.3 snapshot (a flat map of sessions/asyncInvocations, no
// "tables"/"version" envelope) decodes with Version == 0, which mismatches
// sagemakerruntimeSnapshotVersion and is discarded the same way any other
// incompatible version is -- never partially decoded under the old flat-map
// shape.
func TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero(t *testing.T) {
	t.Parallel()

	oldFormatSnap := `{` +
		`"sessions":{"gopherstack-session-1":{"id":"gopherstack-session-1","endpointName":"ep"}},` +
		`"asyncInvocations":{"legacy-id":{"inferenceId":"legacy-id","endpointName":"ep"}},` +
		`"invocations":[],` +
		`"nextId":1}`

	b := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte(oldFormatSnap))
	require.NoError(t, err)

	assert.Empty(t, b.ListSessions(), "old-format snapshot must not be partially decoded")
	assert.Empty(t, b.ListAsyncInvocations(), "old-format snapshot must not be partially decoded")
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend := newFullPersistenceTestBackend(t)
	h := sagemakerruntime.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	restoredBackend := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")
	restoredHandler := sagemakerruntime.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(t.Context(), snap))

	assert.Len(t, restoredBackend.ListSessions(), 1)
	assert.Len(t, restoredBackend.ListAsyncInvocations(), 1)
	assert.Len(t, restoredBackend.ListInvocations(), 1)
}
