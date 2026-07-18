package iotdataplane_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// newFullPersistenceTestBackend creates a backend with data in every
// store.Table-backed collection (shadows, connections, retainedMessages --
// see services/iotdataplane/store_setup.go), so a Snapshot from it exercises
// the entire persisted surface of the backend:
//   - two shadows (named "config" plus the classic "" shadow) for thing
//     "sensor-1", to exercise the shadows table's composite key and the
//     shadowsByThing secondary index grouping multiple entries under one
//     thing;
//   - one classic shadow for a second thing, "sensor-2", to exercise
//     ThingCount (distinct-thing) round-tripping across more than one thing;
//   - one connection;
//   - one retained message.
func newFullPersistenceTestBackend(t *testing.T) *iotdataplane.InMemoryBackend {
	t.Helper()

	b := iotdataplane.NewInMemoryBackend()

	_, err := b.UpdateThingShadow("sensor-1", "config", []byte(
		`{"state":{"desired":{"interval":60},"reported":{"interval":60}}}`,
	))
	require.NoError(t, err)

	_, err = b.UpdateThingShadow("sensor-1", "", []byte(
		`{"state":{"reported":{"temp":72}}}`,
	))
	require.NoError(t, err)

	_, err = b.UpdateThingShadow("sensor-2", "", []byte(
		`{"state":{"reported":{"humidity":40}}}`,
	))
	require.NoError(t, err)

	require.NoError(t, b.RegisterConnection("client-1", "10.0.0.1"))

	require.NoError(t, b.StoreRetainedMessage("home/sensor1/state", []byte(`{"temp":72}`), 1))

	return b
}

// assertFullPersistenceState asserts that b holds exactly the state built by
// newFullPersistenceTestBackend.
func assertFullPersistenceState(t *testing.T, b *iotdataplane.InMemoryBackend) {
	t.Helper()

	assert.Equal(t, 3, iotdataplane.ShadowCount(b))
	assert.Equal(t, 2, iotdataplane.ThingCount(b))

	configShadow, err := b.GetThingShadow("sensor-1", "config")
	require.NoError(t, err)
	assert.Contains(t, string(configShadow), `"interval":60`)

	classicShadow, err := b.GetThingShadow("sensor-1", "")
	require.NoError(t, err)
	assert.Contains(t, string(classicShadow), `"temp":72`)

	sensor2Shadow, err := b.GetThingShadow("sensor-2", "")
	require.NoError(t, err)
	assert.Contains(t, string(sensor2Shadow), `"humidity":40`)

	names, err := b.ListNamedShadowsForThing("sensor-1")
	require.NoError(t, err)
	assert.Equal(t, []string{"config"}, names)

	assert.Equal(t, []string{"sensor-1", "sensor-2"}, b.ListThingsWithShadows())

	assert.Equal(t, 1, iotdataplane.ConnectionCount(b))

	conns := b.ListConnections()
	require.Len(t, conns, 1)
	assert.Equal(t, "client-1", conns[0].ClientID)
	assert.Equal(t, "10.0.0.1", conns[0].SourceIP)

	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))

	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "home/sensor1/state", msgs[0].Topic)
	assert.JSONEq(t, `{"temp":72}`, string(msgs[0].Payload))
	assert.Equal(t, int32(1), msgs[0].Qos)
}

// assertEmptyPersistenceState asserts that b holds no shadows, connections,
// or retained messages -- the state every registered/dirty table must fall
// back to when a snapshot is discarded (version mismatch or absent).
func assertEmptyPersistenceState(t *testing.T, b *iotdataplane.InMemoryBackend) {
	t.Helper()

	assert.Equal(t, 0, iotdataplane.ShadowCount(b))
	assert.Equal(t, 0, iotdataplane.ThingCount(b))
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b))
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b))
	assert.Empty(t, b.ListThingsWithShadows())
	assert.Empty(t, b.ListConnections())

	msgs, err := b.ListRetainedMessages()
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table-backed collection (shadows, connections, retainedMessages)
// through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := newFullPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := iotdataplane.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertFullPersistenceState(t, fresh)
}

// TestInMemoryBackend_SnapshotRestore_EmptyState round-trips a backend with
// no shadows, connections, or retained messages at all -- the degenerate
// case where every registered/dirty table snapshots to an empty array.
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := iotdataplane.NewInMemoryBackend()

	snap := original.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	fresh := iotdataplane.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assertEmptyPersistenceState(t, fresh)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := newFullPersistenceTestBackend(t)

	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assertEmptyPersistenceState(t, b)
}

// TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero verifies that a
// pre-Phase-3.3 snapshot (the old "shadows"/"connections"/"retainedMessages"
// top-level shape, with no "version"/"tables" fields at all) decodes with
// Version == 0, which mismatches iotdataplaneSnapshotVersion and is
// discarded the same way any other incompatible version is -- never
// partially decoded under the old nested-map shape.
func TestInMemoryBackend_RestoreOldFormatDecodesAsVersionZero(t *testing.T) {
	t.Parallel()

	b := newFullPersistenceTestBackend(t)

	oldFormat := []byte(`{
		"shadows": {"sensor-1": {"": {"version": 1, "updatedAt": "2020-01-01T00:00:00Z"}}},
		"connections": {"client-1": {"connectedAt": "2020-01-01T00:00:00Z"}},
		"retainedMessages": {"topic": {"topic": "topic", "payload": "", "qos": 0, "lastModifiedTime": 0}}
	}`)

	err := b.Restore(t.Context(), oldFormat)
	require.NoError(t, err)

	assertEmptyPersistenceState(t, b)
}

// TestInMemoryBackend_RestoreMalformedData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreMalformedData(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()

	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_Reset verifies that Reset clears every store.Table-
// backed collection (registered and dirty alike).
func TestInMemoryBackend_Reset(t *testing.T) {
	t.Parallel()

	b := newFullPersistenceTestBackend(t)

	b.Reset()

	assertEmptyPersistenceState(t, b)
}

// TestHandler_SnapshotRestore verifies Handler.Snapshot/Handler.Restore
// delegate to the backend correctly, table-driven over each collection kind
// so a regression in any single delegation path is isolated to its own
// subtest.
func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, b *iotdataplane.InMemoryBackend)
		check func(t *testing.T, b *iotdataplane.InMemoryBackend)
		name  string
	}{
		{
			name: "shadow",
			setup: func(t *testing.T, b *iotdataplane.InMemoryBackend) {
				t.Helper()

				_, err := b.UpdateThingShadow("thing-a", "", []byte(`{"state":{"reported":{"x":1}}}`))
				require.NoError(t, err)
			},
			check: func(t *testing.T, b *iotdataplane.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, iotdataplane.ShadowCount(b))
			},
		},
		{
			name: "connection",
			setup: func(t *testing.T, b *iotdataplane.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.RegisterConnection("client-a", ""))
			},
			check: func(t *testing.T, b *iotdataplane.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, iotdataplane.ConnectionCount(b))
			},
		},
		{
			name: "retainedMessage",
			setup: func(t *testing.T, b *iotdataplane.InMemoryBackend) {
				t.Helper()

				require.NoError(t, b.StoreRetainedMessage("a/b", []byte("payload"), 0))
			},
			check: func(t *testing.T, b *iotdataplane.InMemoryBackend) {
				t.Helper()

				assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := iotdataplane.NewInMemoryBackend()
			tt.setup(t, backend)

			h := iotdataplane.NewHandler(backend)

			snap := h.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			freshBackend := iotdataplane.NewInMemoryBackend()
			freshHandler := iotdataplane.NewHandler(freshBackend)
			require.NoError(t, freshHandler.Restore(t.Context(), snap))

			tt.check(t, freshBackend)
		})
	}
}

func Test_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"state":{"desired":{"k":"v"}}}`))
	b.AddConnectionInternal("client-1")
	require.NoError(t, b.StoreRetainedMessage("sensor/temp", []byte("42"), 1))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, iotdataplane.ShadowCount(b2))
	assert.Equal(t, 1, iotdataplane.ConnectionCount(b2))
	assert.Equal(t, 1, iotdataplane.RetainedMessageCount(b2))

	// Verify retained message data integrity.
	msg, err := b2.GetRetainedMessage("sensor/temp")
	require.NoError(t, err)
	assert.Equal(t, []byte("42"), msg.Payload)
	assert.Equal(t, int32(1), msg.Qos)
}
func Test_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 0, iotdataplane.ShadowCount(b2))
	assert.Equal(t, 0, iotdataplane.ConnectionCount(b2))
	assert.Equal(t, 0, iotdataplane.RetainedMessageCount(b2))
}
func Test_HandlerSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddShadowInternal("thing1", "", []byte(`{"k":"v"}`))
	h := iotdataplane.NewHandler(b)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := iotdataplane.NewHandler(iotdataplane.NewInMemoryBackend())
	require.NoError(t, h2.Restore(t.Context(), snap))
	assert.Equal(t, 1, iotdataplane.ShadowCount(b))
}
func Test_Persistence_ConnectionsPreserved(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	b.AddConnectionInternal("client-1")
	b.AddConnectionInternal("client-2")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 2, iotdataplane.ConnectionCount(b2))

	// Verify connections are present with their timestamps preserved.
	conns := b2.ListConnections()
	assert.Len(t, conns, 2)
	for _, c := range conns {
		assert.False(t, c.ConnectedAt.IsZero(), "ConnectedAt must be restored")
	}
}
func Test_Persistence_ShadowWithMetadata(t *testing.T) {
	t.Parallel()

	b := iotdataplane.NewInMemoryBackend()
	h := iotdataplane.NewHandler(b)

	doRequest(t, h, http.MethodPost, "/things/dev-persist/shadow", []byte(`{
		"state": {"desired": {"temp": 68}, "reported": {"current": 65}}
	}`))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	h2 := iotdataplane.NewHandler(b2)

	rec := doRequest(t, h2, http.MethodGet, "/things/dev-persist/shadow", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	state := resp["state"].(map[string]any)
	desired := state["desired"].(map[string]any)
	reported := state["reported"].(map[string]any)
	assert.InDelta(t, float64(68), desired["temp"], 0)
	assert.InDelta(t, float64(65), reported["current"], 0)

	_, hasDelta := state["delta"]
	assert.True(t, hasDelta, "delta must be recomputed after restore")

	_, hasMeta := resp["metadata"]
	assert.True(t, hasMeta, "metadata must survive snapshot/restore")
}
func Test_Persistence_NullWipeSurvivesRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := iotdataplane.NewInMemoryBackend()

	_, err := b1.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":{"k":"v"},"reported":{"temp":72}}}`))
	require.NoError(t, err)
	// Wipe desired.
	_, err = b1.UpdateThingShadow("dev", "", []byte(`{"state":{"desired":null}}`))
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iotdataplane.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	resp, err := b2.GetThingShadow("dev", "")
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(resp, &doc))
	state := doc["state"].(map[string]any)

	_, hasDesired := state["desired"]
	assert.False(t, hasDesired, "desired must still be absent after restore")

	reported, hasReported := state["reported"].(map[string]any)
	require.True(t, hasReported, "reported must survive restore")
	assert.InDelta(t, float64(72), reported["temp"], 0)
}
