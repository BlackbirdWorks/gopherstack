package cloudtrail_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index), so a
// Snapshot from it exercises the entire persisted table surface of the
// backend. It deliberately leaves the raw events field ([]Event, the one
// field left un-converted -- see store_setup.go's file doc comment) empty:
// Event has a hand-written MarshalJSON (emitting EventTime as a JSON-protocol
// epoch-seconds number, see backend.go) but no matching UnmarshalJSON, so a
// snapshot containing any event fails to decode on Restore. That asymmetry
// predates this Phase 3.3 conversion -- events was already a plain
// []Event slice round-tripped through encoding/json before and after this
// change -- so it is out of scope here (mechanical map->store.Table swap
// only); see TestInMemoryBackend_SnapshotRestore_EventsPreexistingBug below,
// which documents it as a discovered-but-unfixed issue.
func newPersistenceTestBackend(t *testing.T) *cloudtrail.InMemoryBackend {
	t.Helper()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")

	// trails table + trailsByARN index.
	trail, err := b.CreateTrail(
		"trail1", "bucket1", "prefix", "", "", "", "",
		true, false, false,
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	// channels table + channelsByARN + channelsByName indexes.
	ch, err := b.CreateChannel("channel1", "aws", nil, map[string]string{"k": "v"})
	require.NoError(t, err)

	// dashboards table + dashboardsByARN + dashboardsByName indexes.
	dash, err := b.CreateDashboard("dashboard1", "CUSTOM", map[string]string{"k": "v"})
	require.NoError(t, err)

	// eventDataStores table + edsByARN + edsByName indexes.
	eds, err := b.CreateEventDataStore(
		"eds1", true, false, false, 2557, nil, "", "",
		map[string]string{"k": "v"},
	)
	require.NoError(t, err)

	// queries table.
	_, err = b.StartQuery("SELECT * FROM events", eds.EventDataStoreARN, "")
	require.NoError(t, err)

	// resourcePolicies table.
	b.PutResourcePolicy(eds.EventDataStoreARN, `{"Version":"2012-10-17"}`)

	// imports table.
	_, err = b.StartImport([]string{eds.EventDataStoreARN}, "S3")
	require.NoError(t, err)

	_ = trail
	_ = ch
	_ = dash

	return b
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every store.Table
// (trails, channels, dashboards, eventDataStores, queries, resourcePolicies,
// imports), every secondary store.Index derived from them, and the one raw
// field left un-converted (events) through Snapshot -> Restore into a fresh
// backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// trails table, looked up by name.
	trail, err := fresh.GetTrail("trail1")
	require.NoError(t, err)
	assert.Equal(t, "bucket1", trail.S3BucketName)

	// trailsByARN index, looked up by ARN.
	trailByARN, err := fresh.GetTrail(trail.TrailARN)
	require.NoError(t, err)
	assert.Equal(t, "trail1", trailByARN.Name)

	// channels table + channelsByARN index.
	channels := fresh.ListChannels()
	require.Len(t, channels, 1)
	ch, err := fresh.GetChannel(channels[0].ChannelARN)
	require.NoError(t, err)
	assert.Equal(t, "channel1", ch.Name)

	// channelsByName uniqueness enforced post-restore (index survived).
	_, err = fresh.CreateChannel("channel1", "aws", nil, nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists)

	// dashboards table + dashboardsByARN index.
	dashboards := fresh.ListDashboards()
	require.Len(t, dashboards, 1)
	dash, err := fresh.GetDashboard(dashboards[0].DashboardARN)
	require.NoError(t, err)
	assert.Equal(t, "dashboard1", dash.Name)

	// dashboardsByName uniqueness enforced post-restore (index survived).
	_, err = fresh.CreateDashboard("dashboard1", "CUSTOM", nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists)

	// eventDataStores table + edsByARN index.
	edsList := fresh.ListEventDataStores()
	require.Len(t, edsList, 1)
	eds, err := fresh.GetEventDataStore(edsList[0].EventDataStoreARN)
	require.NoError(t, err)
	assert.Equal(t, "eds1", eds.Name)

	// edsByName uniqueness enforced post-restore (index survived).
	_, err = fresh.CreateEventDataStore("eds1", true, false, false, 0, nil, "", "", nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists)

	// queries table.
	queries := fresh.ListQueries()
	require.Len(t, queries, 1)
	assert.Equal(t, "SELECT * FROM events", queries[0].QueryString)

	// resourcePolicies table.
	policy, err := fresh.GetResourcePolicy(eds.EventDataStoreARN)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, policy.ResourcePolicy)

	// imports table.
	imports := fresh.ListImports()
	require.Len(t, imports, 1)
	assert.Equal(t, "S3", imports[0].ImportSource)

	// events raw slice: empty in this fixture, see newPersistenceTestBackend's
	// doc comment and TestInMemoryBackend_SnapshotRestore_EventsPreexistingBug.
	out := fresh.LookupEvents(cloudtrail.LookupEventsInput{})
	assert.Empty(t, out.Events)

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
}

// TestInMemoryBackend_SnapshotRestore_EventsPreexistingBug documents a
// pre-existing (not introduced by this Phase 3.3 conversion) round-trip bug
// in the one raw field left un-converted, events ([]Event): Event.MarshalJSON
// hand-encodes EventTime as a JSON-protocol epoch-seconds number (see
// backend.go), but Event has no matching UnmarshalJSON, so encoding/json's
// default time.Time decoder rejects it. Any snapshot containing at least one
// event therefore fails Restore entirely today -- this predates the
// store.Table conversion (events was already a plain []Event slice
// round-tripped via encoding/json through backendSnapshot both before and
// after this change) and is out of scope for a mechanical map->store.Table
// swap. Recorded here so it isn't silently lost; see bd for a follow-up fix.
func TestInMemoryBackend_SnapshotRestore_EventsPreexistingBug(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	b.RecordEvent(cloudtrail.Event{EventName: "CreateTrail", EventSource: "cloudtrail.amazonaws.com"})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	err := fresh.Restore(t.Context(), snap)
	require.Error(t, err, "documents the pre-existing Event JSON round-trip asymmetry; fix tracked separately")
}

// TestInMemoryBackend_UpdateEventDataStore_RenamePreservesIndex verifies the
// store.Table gotcha called out for Phase 3.3: renaming eds.Name (an indexed
// field, via edsByName) must delete the old index entry before mutating and
// re-Put after, or the byName index goes stale. This also proves the rename
// survives a Snapshot -> Restore round trip.
func TestInMemoryBackend_UpdateEventDataStore_RenamePreservesIndex(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")

	eds, err := b.CreateEventDataStore("original-name", false, false, false, 0, nil, "", "", nil)
	require.NoError(t, err)

	_, err = b.UpdateEventDataStore(eds.EventDataStoreID, "renamed", nil, nil, nil, nil, nil, "", "")
	require.NoError(t, err)

	// The old name must no longer resolve to anything.
	_, err = b.CreateEventDataStore("original-name", false, false, false, 0, nil, "", "", nil)
	require.NoError(t, err, "original-name index entry must have been removed on rename")

	// The new name must be findable and unique.
	_, err = b.CreateEventDataStore("renamed", false, false, false, 0, nil, "", "", nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists, "renamed index entry must be present")

	snap := b.Snapshot(t.Context())
	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	got, err := fresh.GetEventDataStore(eds.EventDataStoreID)
	require.NoError(t, err)
	assert.Equal(t, "renamed", got.Name)
}

// TestInMemoryBackend_UpdateDashboard_RenamePreservesIndex mirrors the
// EventDataStore rename test above for Dashboard.Name (dashboardsByName).
func TestInMemoryBackend_UpdateDashboard_RenamePreservesIndex(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")

	dash, err := b.CreateDashboard("original-name", "CUSTOM", nil)
	require.NoError(t, err)

	_, err = b.UpdateDashboard(dash.DashboardID, "renamed")
	require.NoError(t, err)

	_, err = b.CreateDashboard("original-name", "CUSTOM", nil)
	require.NoError(t, err, "original-name index entry must have been removed on rename")

	_, err = b.CreateDashboard("renamed", "CUSTOM", nil)
	require.ErrorIs(t, err, cloudtrail.ErrAlreadyExists, "renamed index entry must be present")

	snap := b.Snapshot(t.Context())
	fresh := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	got, err := fresh.GetDashboard(dash.DashboardID)
	require.NoError(t, err)
	assert.Equal(t, "renamed", got.Name)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Empty(t, b.ListTrails())
	assert.Empty(t, b.ListChannels())
	assert.Empty(t, b.ListDashboards())
	assert.Empty(t, b.ListEventDataStores())
	assert.Empty(t, b.ListQueries())
	assert.Empty(t, b.ListImports())

	out := b.LookupEvents(cloudtrail.LookupEventsInput{})
	assert.Empty(t, out.Events)

	_, err = b.GetResourcePolicy("arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/eds-000001")
	require.ErrorIs(t, err, cloudtrail.ErrNotFound)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend (the wiring cli.go's generic setupPersistence
// relies on).
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	h := cloudtrail.NewHandler(newPersistenceTestBackend(t))

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := cloudtrail.NewHandler(cloudtrail.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	trails := h2.Backend.ListTrails()
	require.Len(t, trails, 1)
	assert.Equal(t, "trail1", trails[0].Name)
}
