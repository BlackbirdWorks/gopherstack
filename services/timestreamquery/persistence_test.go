package timestreamquery //nolint:testpackage // needs tsqCtxRegion (isolation_test.go) for cross-region coverage.

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// rejected with an error rather than silently accepted.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateScheduledQuery(
		t.Context(), "seed-sq", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123456789012:role/x",
		"", "", "", "", "", nil,
	)
	require.NoError(t, err)

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, ScheduledQueryCount(b))
	assert.Equal(t, 0, QueryCount(b))

	settings := b.DescribeAccountSettings(t.Context())
	assert.Equal(t, pricingModelComputeUnits, settings.QueryPricingModel)
}

// TestInMemoryBackend_SnapshotRestore_EmptyState verifies that a backend with
// no resources at all round-trips cleanly (every store.Table and raw map left
// empty, not nil).
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("123456789012", "us-east-1")

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 0, ScheduledQueryCount(fresh))
	assert.Equal(t, 0, QueryCount(fresh))

	_, err := fresh.CreateScheduledQuery(
		t.Context(), "post-restore", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123456789012:role/x",
		"", "", "", "", "", nil,
	)
	require.NoError(t, err)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched: the scheduledQueries store.Table (flattened from the
// old region-nested map + arnIndex, keyed directly by the globally-unique
// Arn field, with its byRegion index), the queries store.Table (a
// persistence-coverage expansion -- query results were not persisted before
// this refactor), and the accountSettings plain map left un-converted.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("123456789012", "us-east-1")

	ctxEast := tsqCtxRegion("us-east-1")
	ctxWest := tsqCtxRegion("us-west-2")

	// Two scheduled queries with the SAME name in two different regions --
	// this is exactly the case the old code needed the region-nested map
	// (and the new code needs the Arn-embeds-region property) to keep apart.
	eastSQ, err := original.CreateScheduledQuery(
		ctxEast, "shared-sq", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123456789012:role/east",
		"sns-topic", "err-bucket", "db1", "tbl1", "", map[string]string{"env": "east"},
	)
	require.NoError(t, err)

	westSQ, err := original.CreateScheduledQuery(
		ctxWest, "shared-sq", "SELECT 2", "rate(2 hours)", "arn:aws:iam::123456789012:role/west",
		"", "", "", "", "", nil,
	)
	require.NoError(t, err)

	require.NoError(t, original.UpdateScheduledQuery(t.Context(), eastSQ.Arn, scheduledQueryStateDisabled))
	require.NoError(t, original.TagResource(t.Context(), westSQ.Arn, map[string]string{"team": "obs"}))

	// Populate the queries store.Table via the legacy Query() path.
	qr := original.Query(t.Context(), "SELECT * FROM x")
	require.NotNil(t, qr)

	settings, err := original.UpdateAccountSettings(ctxEast, pricingModelBytesScanned, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, pricingModelBytesScanned, settings.QueryPricingModel)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// scheduledQueries: both regions' entries survived, with correct fields.
	assert.Equal(t, 2, ScheduledQueryCount(fresh))

	eastDesc, err := fresh.DescribeScheduledQuery(t.Context(), eastSQ.Arn)
	require.NoError(t, err)
	assert.Equal(t, scheduledQueryStateDisabled, eastDesc.State)
	assert.Equal(t, "SELECT 1", eastDesc.QueryString)
	assert.Equal(t, "east", eastDesc.Tags["env"])

	westDesc, err := fresh.DescribeScheduledQuery(t.Context(), westSQ.Arn)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 2", westDesc.QueryString)
	assert.Equal(t, "obs", westDesc.Tags["team"])

	// byRegion index survived: List* filters by region correctly post-restore.
	eastList := fresh.ListScheduledQueriesFull(ctxEast)
	require.Len(t, eastList, 1)
	assert.Equal(t, "shared-sq", eastList[0].Name)
	assert.Contains(t, eastList[0].Arn, "us-east-1")

	westList := fresh.ListScheduledQueriesFull(ctxWest)
	require.Len(t, westList, 1)
	assert.Contains(t, westList[0].Arn, "us-west-2")

	// queries: the QueryResult round-tripped, and CancelQuery still finds it.
	assert.Equal(t, 1, QueryCount(fresh))
	require.NoError(t, fresh.CancelQuery(t.Context(), qr.QueryID))

	// CancelQuery is documented as idempotent: a repeat cancellation of the
	// same QueryId must still succeed (CancelQueryOutput.CancellationMessage),
	// not 404, so the entry stays in place rather than being deleted.
	require.NoError(t, fresh.CancelQuery(t.Context(), qr.QueryID))
	assert.Equal(t, 1, QueryCount(fresh))

	// accountSettings: plain map, still persisted as before.
	restoredSettings := fresh.DescribeAccountSettings(ctxEast)
	assert.Equal(t, pricingModelBytesScanned, restoredSettings.QueryPricingModel)

	// Deleting one region's query post-restore leaves the other untouched --
	// proves the ARN-keyed table + byRegion index are both fully wired, not
	// just read-only artifacts of the restore.
	require.NoError(t, fresh.DeleteScheduledQuery(t.Context(), eastSQ.Arn))
	assert.Equal(t, 1, ScheduledQueryCount(fresh))

	_, err = fresh.DescribeScheduledQuery(t.Context(), westSQ.Arn)
	require.NoError(t, err)
}

// TestInMemoryBackend_SnapshotRestore_QueryCompute verifies that a QueryCompute
// switched to PROVISIONED via UpdateAccountSettings survives a Snapshot/Restore
// round trip. Before this fix, accountSettingsSnapshot only captured
// QueryPricingModel/MaxQueryTCU, so a PROVISIONED account would silently
// revert to the ON_DEMAND default after a restore (e.g. a gopherstack restart).
func TestInMemoryBackend_SnapshotRestore_QueryCompute(t *testing.T) {
	t.Parallel()

	original := NewInMemoryBackend("123456789012", "us-east-1")
	tcu := int32(16)

	_, err := original.UpdateAccountSettings(t.Context(), "", nil,
		&QueryComputeUpdate{ComputeMode: "PROVISIONED", TargetQueryTCU: &tcu})
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	settings := fresh.DescribeAccountSettings(t.Context())
	require.NotNil(t, settings.QueryCompute)
	assert.Equal(t, "PROVISIONED", settings.QueryCompute.ComputeMode)
	require.NotNil(t, settings.QueryCompute.ProvisionedCapacity)
	require.NotNil(t, settings.QueryCompute.ProvisionedCapacity.ActiveQueryTCU)
	assert.Equal(t, int32(16), *settings.QueryCompute.ProvisionedCapacity.ActiveQueryTCU)
}

// TestPersistence_HandlerSnapshotDelegates verifies that Handler.Snapshot
// delegates to the underlying InMemoryBackend and that the resulting snapshot
// round-trips a scheduled query (including its tags) via a fresh backend's
// Restore.
func TestPersistence_HandlerSnapshotDelegates(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("123456789012", "us-east-1")
	h := NewHandler(backend)

	_, err := backend.CreateScheduledQuery(
		t.Context(), "persist-test", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123:role/r",
		"arn:aws:sns:us-east-1:123:topic", "my-errors-bucket", "", "", "", map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	_, err = backend.UpdateAccountSettings(t.Context(), pricingModelComputeUnits, nil, nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	backend2 := NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, backend2.Restore(t.Context(), snap))

	assert.Equal(t, 1, ScheduledQueryCount(backend2))

	settings := backend2.DescribeAccountSettings(t.Context())
	assert.Equal(t, pricingModelComputeUnits, settings.QueryPricingModel)

	queries := backend2.ListScheduledQueriesFull(t.Context())
	require.Len(t, queries, 1)
	assert.Equal(t, "v", queries[0].Tags["k"])
}

// TestPersistence_HandlerRestoreDelegates verifies that Handler.Restore
// delegates to a (possibly different) InMemoryBackend instance.
func TestPersistence_HandlerRestoreDelegates(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend("123456789012", "us-east-1")
	h := NewHandler(backend)

	_, err := backend.CreateScheduledQuery(
		t.Context(), "handler-snap-test", "SELECT 1", "rate(1 hour)", "arn:aws:iam::123:role/r",
		"arn:aws:sns:us-east-1:123:topic", "my-errors-bucket", "", "", "", nil,
	)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	backend2 := NewInMemoryBackend("123456789012", "us-east-1")
	h2 := NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, ScheduledQueryCount(backend2))
}

// TestPersistence_HandlerSnapshotWithInMemoryBackend verifies that
// Handler.Snapshot always produces a valid (non-nil) snapshot for a fresh
// InMemoryBackend.
func TestPersistence_HandlerSnapshotWithInMemoryBackend(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend("123", "us-east-1"))

	snap := h.Snapshot(t.Context())
	assert.NotNil(t, snap, "InMemoryBackend always produces a valid snapshot")
}

// TestPersistence_RestoreInvalidJSON verifies Restore returns an error on
// malformed JSON but succeeds (starting empty) on a syntactically valid but
// version-less snapshot.
func TestPersistence_RestoreInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{name: "invalid json fails", data: []byte(`not-json`), wantErr: true},
		{name: "empty object succeeds", data: []byte(`{}`), wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1")
			err := b.Restore(t.Context(), tt.data)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
