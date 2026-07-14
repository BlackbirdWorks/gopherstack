package mwaa //nolint:testpackage // needs isoCtxRegion/newIsoCreateReq (isolation_test.go) + region context key.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// TestInMemoryBackend_RestoreRejectsBadSnapshots is the Phase 3.3
// version-guard test: it verifies Restore never partially decodes a snapshot
// it cannot trust -- malformed JSON is reported as an error, and any
// version mismatch (including the pre-Phase-3.3 shape, which has no
// "version"/"tables" keys at all and so decodes with Version == 0) is
// discarded cleanly (backend reset to empty, no error) rather than
// misinterpreted.
func TestInMemoryBackend_RestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		data      []byte
		wantError bool
	}{
		{
			name:      "malformed JSON is an error",
			data:      []byte("not-valid-json"),
			wantError: true,
		},
		{
			name:      "version mismatch is discarded cleanly",
			data:      []byte(`{"version":999,"tables":{}}`),
			wantError: false,
		},
		{
			name:      "pre-Phase-3.3 shape decodes Version as zero and is discarded",
			data:      []byte(`{"environments":{"us-east-1":{"seed":{"Name":"seed"}}},"arnIndex":{}}`),
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("us-east-1", "000000000000")
			_, err := b.CreateEnvironment(isoCtxRegion("us-east-1"), "seed-env", newIsoCreateReq())
			require.NoError(t, err)

			err = b.Restore(t.Context(), tt.data)

			if tt.wantError {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 0, EnvironmentCount(b))
			assert.Equal(t, 0, ARNIndexSize(b))
		})
	}
}

// TestInMemoryBackend_SnapshotRestore_EmptyBackend verifies Snapshot/Restore
// round trips cleanly on a backend with no data at all -- the registered
// table and the raw metrics map must both decode back to empty, not
// nil-panic or error.
func TestInMemoryBackend_SnapshotRestore_EmptyBackend(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("us-east-1", "000000000000")
	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("us-east-1", "000000000000")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 0, EnvironmentCount(fresh))
	assert.Equal(t, 0, ARNIndexSize(fresh))

	list, err := fresh.ListEnvironments(isoCtxRegion("us-east-1"))
	require.NoError(t, err)
	assert.Empty(t, list)
}

// TestInMemoryBackend_SnapshotRestore_FullState is the Phase 3.3 full-state
// round-trip test: it exercises the flat store.Table[Environment] (composite
// "region|name" key, byRegion index, byARN index) with the SAME environment
// name in TWO regions to prove the composite key survives a round trip and
// keeps same-named environments in different regions fully isolated -- both
// for direct lookups and for the region-scoped ARN reverse index used by
// Tag/Untag/ListTagsForResource. It also covers the raw (non-store.Table)
// region-nested metrics map, to guard against a silent persistence
// regression there.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("us-east-1", "111122223333")

	ctxEast := isoCtxRegion("us-east-1")
	ctxWest := isoCtxRegion("us-west-2")

	const sharedName = "shared-env"

	eastReq := newIsoCreateReq()
	eastEnv, err := original.CreateEnvironment(ctxEast, sharedName, eastReq)
	require.NoError(t, err)

	westReq := newIsoCreateReq()
	westReq.EnvironmentClass = "mw1.medium"
	westEnv, err := original.CreateEnvironment(ctxWest, sharedName, westReq)
	require.NoError(t, err)

	require.NoError(t, original.TagResource(ctxEast, eastEnv.ARN, map[string]string{"team": "east"}))

	require.NoError(t, original.PublishMetrics(ctxWest, sharedName, &publishMetricsRequest{
		MetricData: []MetricDatum{{MetricName: "m1"}},
	}))

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := NewInMemoryBackend("us-east-1", "111122223333")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Composite key kept both same-named environments distinct rather than
	// one overwriting the other.
	assert.Equal(t, 2, EnvironmentCount(fresh))
	assert.Equal(t, 2, ARNIndexSize(fresh))
	assert.Equal(t, 1, EnvironmentCountRegion(fresh, "us-east-1"))
	assert.Equal(t, 1, EnvironmentCountRegion(fresh, "us-west-2"))

	gotEast, err := fresh.GetEnvironment(ctxEast, sharedName)
	require.NoError(t, err)
	assert.Contains(t, gotEast.ARN, "us-east-1")
	assert.Equal(t, defaultEnvironmentClass, gotEast.EnvironmentClass)

	gotWest, err := fresh.GetEnvironment(ctxWest, sharedName)
	require.NoError(t, err)
	assert.Contains(t, gotWest.ARN, "us-west-2")
	assert.Equal(t, "mw1.medium", gotWest.EnvironmentClass)

	// byARN index: tags on the east environment survived and stayed off the
	// west environment despite sharing a name.
	eastTags, err := fresh.ListTagsForResource(ctxEast, eastEnv.ARN)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "east"}, eastTags)

	westTags, err := fresh.ListTagsForResource(ctxWest, westEnv.ARN)
	require.NoError(t, err)
	assert.Empty(t, westTags)

	// The byARN index is region-scoped: the east ARN must not resolve from
	// the west region, exactly as it did not before Phase 3.3 (see
	// TestMWAATagsAndMetricsRegionIsolation).
	_, err = fresh.ListTagsForResource(ctxWest, eastEnv.ARN)
	require.ErrorIs(t, err, ErrEnvironmentNotFound)

	// metrics (raw map, left unconverted): published only into west, must
	// stay off east after restore.
	westMetrics, err := fresh.GetMetrics(ctxWest, sharedName)
	require.NoError(t, err)
	require.Len(t, westMetrics, 1)
	assert.Equal(t, "m1", westMetrics[0].MetricName)

	eastMetrics, err := fresh.GetMetrics(ctxEast, sharedName)
	require.NoError(t, err)
	assert.Empty(t, eastMetrics)
}

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing the same two methods is not enough on its own,
// since Handler.Backend is the StorageBackend interface and does not promote
// them. Mirrors services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := NewInMemoryBackend("us-east-1", "000000000000")
	h := NewHandler(backend)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	_, err := backend.CreateEnvironment(isoCtxRegion("us-east-1"), "delegate-env", newIsoCreateReq())
	require.NoError(t, err)

	snap := h.Snapshot(ctx)
	require.NotNil(t, snap)

	backend2 := NewInMemoryBackend("us-east-1", "000000000000")
	h2 := NewHandler(backend2)
	require.NoError(t, h2.Restore(ctx, snap))

	got, err := backend2.GetEnvironment(isoCtxRegion("us-east-1"), "delegate-env")
	require.NoError(t, err)
	assert.Equal(t, "delegate-env", got.Name)
}
