package textract //nolint:testpackage // needs access to the unexported region context key for multi-region coverage.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_PersistenceRoundTrip proves that every store.Table-backed resource
// (jobs, expenseJobs, lendingJobs, adapters, adapterVersions) and every
// raw-left persisted map (clientTokenToJobID, adapterClientTokenToID)
// survives a Snapshot/Restore round trip -- including cross-region isolation
// and idempotency-token behavior -- exactly as it did before the Phase 3.3
// store.Table conversion.
func Test_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		populate func(t *testing.T, b *InMemoryBackend)
		verify   func(t *testing.T, b *InMemoryBackend)
		name     string
	}{
		{
			name:     "empty backend",
			populate: func(t *testing.T, _ *InMemoryBackend) { t.Helper() },
			verify: func(t *testing.T, b *InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 0, JobCount(b))
				assert.Equal(t, 0, ExpenseJobCount(b))
				assert.Equal(t, 0, LendingJobCount(b))
				assert.Equal(t, 0, AdapterCount(b))
				assert.Equal(t, 0, AdapterVersionCount(b))
			},
		},
		{
			name:     "full state across two regions",
			populate: populateFullPersistenceState,
			verify:   verifyFullPersistenceState,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackendSync("123456789012", "us-east-1")
			tt.populate(t, b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			restored := NewInMemoryBackendSync("123456789012", "us-east-1")
			require.NoError(t, restored.Restore(t.Context(), snap))

			tt.verify(t, restored)
		})
	}
}

// populateFullPersistenceState seeds every store.Table-backed resource and
// every raw-left persisted map, split across two regions so region isolation
// (composite key regionKey(region, id)) is exercised by the round trip too.
func populateFullPersistenceState(t *testing.T, b *InMemoryBackend) {
	t.Helper()

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	// DocumentJob (both job types) + clientTokenToJobID idempotency tokens.
	_, err := b.StartDocumentAnalysisWithOptions(
		ctxEast, "s3://bucket/east.pdf", nil, nil, nil, "east-tag", "east-token",
	)
	require.NoError(t, err)

	_, err = b.StartDocumentTextDetectionWithOptions(
		ctxWest, "s3://bucket/west.png", nil, nil, "west-tag", "west-token",
	)
	require.NoError(t, err)

	// ExpenseJob / LendingJob in both regions.
	_, err = b.StartExpenseAnalysis(ctxEast, "s3://bucket/invoice-east.pdf")
	require.NoError(t, err)

	_, err = b.StartExpenseAnalysis(ctxWest, "s3://bucket/invoice-west.pdf")
	require.NoError(t, err)

	_, err = b.StartLendingAnalysis(ctxEast, "s3://bucket/loan-east.pdf")
	require.NoError(t, err)

	_, err = b.StartLendingAnalysis(ctxWest, "s3://bucket/loan-west.pdf")
	require.NoError(t, err)

	// Adapter (+ adapterClientTokenToID idempotency token) and AdapterVersion
	// in both regions.
	_, err = b.CreateAdapterWithToken(
		ctxEast, "east-adapter", "east desc", "ENABLED",
		[]string{"FORMS"}, map[string]string{"k": "east"}, "east-adapter-token",
	)
	require.NoError(t, err)

	westAdapter, err := b.CreateAdapterWithToken(
		ctxWest, "west-adapter", "west desc", "DISABLED",
		[]string{"QUERIES"}, map[string]string{"k": "west"}, "west-adapter-token",
	)
	require.NoError(t, err)

	_, err = b.CreateAdapterVersionWithOptions(
		ctxWest, westAdapter.AdapterID, map[string]string{"v": "1"}, nil, nil, "", "",
	)
	require.NoError(t, err)
}

// verifyFullPersistenceState checks every resource populated by
// populateFullPersistenceState survived Snapshot/Restore, including
// cross-region isolation and idempotency-token replay behavior.
func verifyFullPersistenceState(t *testing.T, b *InMemoryBackend) {
	t.Helper()

	ctxEast := ctxRegion("us-east-1")
	ctxWest := ctxRegion("us-west-2")

	assert.Equal(t, 2, JobCount(b))
	assert.Equal(t, 2, ExpenseJobCount(b))
	assert.Equal(t, 2, LendingJobCount(b))
	assert.Equal(t, 2, AdapterCount(b))
	assert.Equal(t, 1, AdapterVersionCount(b))

	eastJobs := b.ListJobs(ctxEast)
	require.Len(t, eastJobs, 1)
	assert.Equal(t, jobTypeDocumentAnalysis, eastJobs[0].JobType)

	westJobs := b.ListJobs(ctxWest)
	require.Len(t, westJobs, 1)
	assert.Equal(t, jobTypeTextDetection, westJobs[0].JobType)

	// clientTokenToJobID idempotency survives restore: replaying the same
	// token in the same region returns the SAME job rather than minting a
	// new one.
	replay, err := b.StartDocumentAnalysisWithOptions(
		ctxEast, "s3://bucket/east.pdf", nil, nil, nil, "east-tag", "east-token",
	)
	require.NoError(t, err)
	assert.Equal(t, eastJobs[0].JobID, replay.JobID)
	assert.Equal(t, 2, JobCount(b), "idempotent replay must not create a new job")

	eastAdapters := b.ListAdapters(ctxEast)
	require.Len(t, eastAdapters, 1)

	// adapterClientTokenToID idempotency survives restore too.
	replayAdapter, err := b.CreateAdapterWithToken(
		ctxEast, "east-adapter", "east desc", "ENABLED",
		[]string{"FORMS"}, map[string]string{"k": "east"}, "east-adapter-token",
	)
	require.NoError(t, err)
	assert.Equal(t, eastAdapters[0].AdapterID, replayAdapter.AdapterID)
	assert.Equal(t, 2, AdapterCount(b), "idempotent replay must not create a new adapter")

	// Region isolation preserved after restore: west has its own adapter and
	// its adapter version, east has neither.
	westAdapters := b.ListAdapters(ctxWest)
	require.Len(t, westAdapters, 1)
	assert.NotEqual(t, eastAdapters[0].AdapterID, westAdapters[0].AdapterID)

	westVersions, err := b.ListAdapterVersions(ctxWest, westAdapters[0].AdapterID)
	require.NoError(t, err)
	require.Len(t, westVersions, 1)
	assert.Equal(t, "ACTIVE", westVersions[0].Status)
	assert.Equal(t, "1", westVersions[0].Tags["v"])

	eastVersions, err := b.ListAdapterVersions(ctxEast, eastAdapters[0].AdapterID)
	require.NoError(t, err)
	assert.Empty(t, eastVersions)
}

// Test_PersistenceVersionMismatch proves that Restore discards state and
// starts empty when the snapshot's version field doesn't match
// textractSnapshotVersion, rather than partially decoding an incompatible
// shape -- covering both a newer/foreign version and a legacy
// (pre-Phase-3.3) snapshot with no version field at all.
func Test_PersistenceVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rewrite func(t *testing.T, snap []byte) []byte
		name    string
	}{
		{
			name: "future version",
			rewrite: func(t *testing.T, snap []byte) []byte {
				t.Helper()

				return rewriteSnapshotVersion(t, snap, textractSnapshotVersion+1)
			},
		},
		{
			name: "legacy snapshot with no version field",
			rewrite: func(t *testing.T, snap []byte) []byte {
				t.Helper()

				var m map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(snap, &m))
				delete(m, "version")

				out, err := json.Marshal(m)
				require.NoError(t, err)

				return out
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackendSync("123456789012", "us-east-1")

			_, err := b.StartDocumentAnalysis(context.Background(), "s3://bucket/doc.pdf")
			require.NoError(t, err)

			_, err = b.CreateAdapterWithToken(
				context.Background(), "adapter", "desc", "ENABLED", []string{"FORMS"}, nil, "tok",
			)
			require.NoError(t, err)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			mismatched := tt.rewrite(t, snap)

			// Restore target already has state of its own, which must be wiped
			// -- not merged with -- the discarded incompatible snapshot.
			target := NewInMemoryBackendSync("123456789012", "us-east-1")
			_, err = target.StartLendingAnalysis(context.Background(), "s3://bucket/loan.pdf")
			require.NoError(t, err)

			require.NoError(t, target.Restore(t.Context(), mismatched))

			assert.Equal(t, 0, JobCount(target))
			assert.Equal(t, 0, ExpenseJobCount(target))
			assert.Equal(t, 0, LendingJobCount(target))
			assert.Equal(t, 0, AdapterCount(target))
			assert.Equal(t, 0, AdapterVersionCount(target))

			// Idempotency token maps were wiped too: replaying the original
			// token now mints a brand-new adapter rather than finding a
			// (discarded) match.
			newAdapter, err := target.CreateAdapterWithToken(
				context.Background(), "adapter", "desc", "ENABLED", []string{"FORMS"}, nil, "tok",
			)
			require.NoError(t, err)
			assert.Equal(t, 1, AdapterCount(target))
			assert.NotEmpty(t, newAdapter.AdapterID)
		})
	}
}

// rewriteSnapshotVersion decodes snap, overwrites its top-level "version"
// field with version, and re-encodes it.
func rewriteSnapshotVersion(t *testing.T, snap []byte, version int) []byte {
	t.Helper()

	var m map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(snap, &m))

	versionJSON, err := json.Marshal(version)
	require.NoError(t, err)

	m["version"] = versionJSON

	out, err := json.Marshal(m)
	require.NoError(t, err)

	return out
}

// Test_PersistenceMalformedSnapshot proves that Restore surfaces an error
// (rather than silently resetting) when the snapshot bytes are not valid
// JSON at all -- distinct from the recoverable version-mismatch case above.
func Test_PersistenceMalformedSnapshot(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackendSync("123456789012", "us-east-1")

	err := b.Restore(t.Context(), []byte("{not valid json"))
	require.Error(t, err)
}

// TestInMemoryBackend_PersistenceSnapshotRestore proves that a backend
// containing a mix of DocumentAnalysis and TextDetection jobs survives a
// Snapshot/Restore round trip, and that the restored backend's snapshots are
// isolated from further mutation of the restored backend.
func TestInMemoryBackend_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		jobCount int
	}{
		{
			name:     "empty_backend",
			jobCount: 0,
		},
		{
			name:     "with_jobs",
			jobCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackendSync("123456789012", "us-east-1")

			var lastJobID string

			for i := range tt.jobCount {
				var job *DocumentJob
				var err error

				if i%2 == 0 {
					job, err = b.StartDocumentAnalysis(context.Background(), "s3://bucket/doc.pdf")
				} else {
					job, err = b.StartDocumentTextDetection(context.Background(), "s3://bucket/doc.png")
				}

				require.NoError(t, err)
				lastJobID = job.JobID
			}

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := NewInMemoryBackendSync("123456789012", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			jobs := b2.ListJobs(context.Background())
			assert.Len(t, jobs, tt.jobCount)

			if tt.jobCount > 0 {
				// The last job from original backend should be retrievable after restore.
				retrieved, err := b2.GetDocumentAnalysis(context.Background(), lastJobID)
				if err != nil {
					// May be text detection type; try that.
					retrieved, err = b2.GetDocumentTextDetection(context.Background(), lastJobID)
					require.NoError(t, err)
				}

				assert.Equal(t, lastJobID, retrieved.JobID)

				// Snapshot isolation: adding to b2 after restore should not affect original snap.
				_, _ = b2.StartDocumentAnalysis(context.Background(), "s3://bucket/extra.pdf")
				snap2 := b2.Snapshot(t.Context())
				assert.NotEqual(t, snap, snap2)
			}
		})
	}
}
