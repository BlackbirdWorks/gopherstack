package bedrockruntime_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.StartAsyncInvoke("seed-model", "s3://bucket/out", "", nil)
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}))
	assert.Empty(t, b.ListInvocations())
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches bedrockruntimeSnapshotVersion and is discarded the same way any
// other incompatible version is -- not partially applied. BedrockRuntime had
// no persistence at all before Phase 3.3, so this also covers "no snapshot
// format predates this one" -- any pre-existing on-disk data (there is none)
// would hit this same path.
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	_, err := b.StartAsyncInvoke("seed-model", "s3://bucket/out", "", nil)
	require.NoError(t, err)

	// A shape entirely lacking "version"/"tables" keys.
	err = b.Restore(t.Context(), []byte(`{"asyncInvokeCounter":5}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across the store.Table-backed asyncInvokes collection plus
// every plain field left un-converted by Phase 3.3: tokenIndex (idempotency
// lookups must keep working post-restore), the order-sensitive invocations
// ring buffer, and asyncInvokeCounter (new ARNs minted after a restore must
// never collide with ones issued before the snapshot).
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := bedrockruntime.NewInMemoryBackend("111122223333", "us-west-2")

	tokened, err := original.StartAsyncInvoke(
		"anthropic.claude-v2", "s3://bucket/out1", "client-token-1", map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	_, err = original.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/out2", "", nil)
	require.NoError(t, err)

	original.RecordInvocation("InvokeModel", "model-a", "input-a", "output-a")
	original.RecordInvocation("Converse", "model-b", "input-b", "output-b")
	original.RecordInvocation("CountTokens", "model-c", "input-c", "output-c")

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// asyncInvokes table: both invocations round-trip with their fields intact.
	got, err := fresh.GetAsyncInvoke(tokened.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/out1", got.OutputS3URI)
	assert.Equal(t, "test", got.Tags["env"])

	all := fresh.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	require.Len(t, all, 2)

	// tokenIndex plain map: the idempotency lookup for client-token-1 must
	// still resolve to the same invocation after restore rather than minting
	// a new one.
	replay, err := fresh.StartAsyncInvoke(
		"anthropic.claude-v2", "s3://bucket/out1", "client-token-1", map[string]string{"env": "test"},
	)
	require.NoError(t, err)
	assert.Equal(t, tokened.InvocationArn, replay.InvocationArn)
	assert.Len(t, fresh.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}), 2)

	// asyncInvokeCounter scalar: a brand-new (un-tokened) invocation must get
	// a fresh ARN, never reusing one already handed out before the snapshot.
	fresh3, err := fresh.StartAsyncInvoke("anthropic.claude-v2", "s3://bucket/out3", "", nil)
	require.NoError(t, err)
	assert.NotEqual(t, tokened.InvocationArn, fresh3.InvocationArn)
	for _, inv := range all {
		assert.NotEqual(t, inv.InvocationArn, fresh3.InvocationArn)
	}

	// invocations ring buffer: order-sensitive, so must round-trip in the
	// exact oldest-first insertion order RecordInvocation produced.
	invs := fresh.ListInvocations()
	require.Len(t, invs, 3)
	assert.Equal(t, "model-a", invs[0].ModelID)
	assert.Equal(t, "model-b", invs[1].ModelID)
	assert.Equal(t, "model-c", invs[2].ModelID)

	// accountID/region scalars surface indirectly through a freshly minted ARN.
	assert.Contains(t, fresh3.InvocationArn, "us-west-2")
	assert.Contains(t, fresh3.InvocationArn, "111122223333")
}

// TestHandler_SnapshotRestoreDelegate verifies the Handler-level Snapshot and
// Restore -- which the persistence layer actually calls, per
// setupPersistence in cli.go -- correctly delegate to the backend. Before
// this conversion, Handler had no Snapshot/Restore at all, so BedrockRuntime
// was never wired into persistence in the first place (dead wiring).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := bedrockruntime.NewHandler(bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))

	_, err := h.Backend.StartAsyncInvoke("delegate-model", "s3://bucket/out", "", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := bedrockruntime.NewHandler(bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	require.NoError(t, h2.Restore(t.Context(), snap))

	got := h2.Backend.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{})
	require.Len(t, got, 1)
	assert.Equal(t, "s3://bucket/out", got[0].OutputS3URI)
}

// TestInMemoryBackend_Snapshot_EmptyBackend verifies Snapshot/Restore round
// trips cleanly on a backend with no data at all -- the registered table and
// every plain map/scalar must decode back to empty/zero, not nil-panic or error.
func TestInMemoryBackend_Snapshot_EmptyBackend(t *testing.T) {
	t.Parallel()

	original := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	require.NoError(t, fresh.Restore(t.Context(), snap))
	assert.Empty(t, fresh.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}))
	assert.Empty(t, fresh.ListInvocations())
}

// TestInMemoryBackend_AdvanceAsyncInvokesForTest_MutatesTableInPlace verifies
// that advancing an in-progress async invocation via the store.Table (no
// secondary index is registered on asyncInvokes, so in-place field mutation
// through a pointer obtained from store.Table.All is safe -- see
// store_setup.go) is visible through a subsequent Get.
func TestInMemoryBackend_AdvanceAsyncInvokesForTest_MutatesTableInPlace(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	inv, err := b.StartAsyncInvoke("model-x", "s3://bucket/out", "", nil)
	require.NoError(t, err)
	require.Equal(t, bedrockruntime.AsyncInvokeStatusInProgress, inv.Status)

	b.AdvanceAsyncInvokesForTest(0)

	got, err := b.GetAsyncInvoke(inv.InvocationArn)
	require.NoError(t, err)
	assert.Equal(t, bedrockruntime.AsyncInvokeStatusCompleted, got.Status)
	require.NotNil(t, got.EndTime)
}

// TestInMemoryBackend_Reset verifies Reset clears the store.Table-backed
// asyncInvokes collection plus every raw field (tokenIndex, invocations
// ring, asyncInvokeCounter), matching the pre-conversion behavior.
func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	before, err := b.StartAsyncInvoke("model-x", "s3://bucket/out", "reset-token", nil)
	require.NoError(t, err)
	b.RecordInvocation("InvokeModel", "model-x", "in", "out")

	b.Reset()

	assert.Empty(t, b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}))
	assert.Empty(t, b.ListInvocations())

	// asyncInvokeCounter reset to 0: the next minted ARN must reuse id "1",
	// matching a brand-new backend instead of continuing from before Reset.
	after, err := b.StartAsyncInvoke("model-x", "s3://bucket/out", "", nil)
	require.NoError(t, err)
	assert.Contains(t, after.InvocationArn, "async-invoke/1")

	// tokenIndex reset: reusing "reset-token" now mints a brand-new
	// invocation rather than resolving the pre-Reset one.
	replay, err := b.StartAsyncInvoke("model-x", "s3://bucket/out", "reset-token", nil)
	require.NoError(t, err)
	assert.NotEqual(t, before.InvocationArn, replay.InvocationArn)
}
