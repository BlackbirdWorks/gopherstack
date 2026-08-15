package mediatailor_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// persistenceFixtureIDs carries every identifier newPersistenceTestBackend
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving names.
type persistenceFixtureIDs struct {
	playbackConfigName string
	channelName        string
	sourceLocationName string
	vodSourceName      string
	liveSourceName     string
	prefetchScheduleID string
	programName        string
	functionID         string
}

// newPersistenceTestBackend creates a backend with one populated entry in
// every store.Table (and, transitively, every secondary store.Index) plus
// the one raw grouping map left un-converted (tags), so a Snapshot from it
// exercises the entire persisted surface of the backend.
func newPersistenceTestBackend(t *testing.T) (*mediatailor.InMemoryBackend, persistenceFixtureIDs) {
	t.Helper()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	cfg, err := b.PutPlaybackConfiguration(
		"pc1", "https://ads.example.com", "https://video.example.com",
		map[string]string{"env": "test"}, nil,
	)
	require.NoError(t, err)

	ch, err := b.CreateChannel("ch1", "LOOP", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	sl, err := b.CreateSourceLocation(
		"sl1", "https://origin.example.com",
		&mediatailor.AccessConfiguration{AccessType: "S3_SIGV4"},
		&mediatailor.DefaultSegmentDeliveryConfiguration{BaseURL: "https://cdn.example.com"},
		[]mediatailor.SegmentDeliveryConfiguration{{Name: "primary", BaseURL: "https://cdn2.example.com"}},
		nil,
	)
	require.NoError(t, err)

	vs, err := b.CreateVodSource("sl1", "vs1", nil, nil)
	require.NoError(t, err)

	ls, err := b.CreateLiveSource("sl1", "ls1", nil, nil)
	require.NoError(t, err)

	ps, err := b.CreatePrefetchSchedule("pc1", "sched1", "", "", nil, nil, nil, nil)
	require.NoError(t, err)

	prog, err := b.CreateProgram("ch1", "prog1", "sl1", "vs1", "", testScheduleConfig(1_700_000_000_000), nil, nil, nil)
	require.NoError(t, err)

	fn, err := b.PutFunction("fn1", "HTTP_REQUEST", "test function", nil, nil, nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.TagResource(cfg.PlaybackConfigurationARN, map[string]string{"team": "mediatailor"}))

	return b, persistenceFixtureIDs{
		playbackConfigName: cfg.Name,
		channelName:        ch.Name,
		sourceLocationName: sl.Name,
		vodSourceName:      vs.VodSourceName,
		liveSourceName:     ls.LiveSourceName,
		prefetchScheduleID: ps.Name,
		programName:        prog.ProgramName,
		functionID:         fn.FunctionID,
	}
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table (playbackConfigurations, channels, sourceLocations,
// vodSources, liveSources, prefetchSchedules, programs, functions), every
// secondary store.Index derived from them (byLocation on vodSources and
// liveSources, byPlaybackConfig on prefetchSchedules, byChannel on
// programs), and the one raw grouping map left un-converted (tags) through
// Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// playbackConfigurations table.
	cfg, err := fresh.GetPlaybackConfiguration(ids.playbackConfigName)
	require.NoError(t, err)
	assert.Equal(t, ids.playbackConfigName, cfg.Name)

	// channels table.
	ch, err := fresh.DescribeChannel(ids.channelName)
	require.NoError(t, err)
	assert.Equal(t, ids.channelName, ch.Name)

	// sourceLocations table.
	sl, err := fresh.DescribeSourceLocation(ids.sourceLocationName)
	require.NoError(t, err)
	assert.Equal(t, ids.sourceLocationName, sl.Name)
	require.NotNil(t, sl.AccessConfiguration)
	assert.Equal(t, "S3_SIGV4", sl.AccessConfiguration.AccessType)
	require.NotNil(t, sl.DefaultSegmentDeliveryConfiguration)
	assert.Equal(t, "https://cdn.example.com", sl.DefaultSegmentDeliveryConfiguration.BaseURL)
	require.Len(t, sl.SegmentDeliveryConfigurations, 1)
	assert.Equal(t, "primary", sl.SegmentDeliveryConfigurations[0].Name)

	// vodSources table + vodSourcesByLocation index.
	vs, err := fresh.DescribeVodSource(ids.sourceLocationName, ids.vodSourceName)
	require.NoError(t, err)
	assert.Equal(t, ids.vodSourceName, vs.VodSourceName)

	vsList, _, err := fresh.ListVodSources(ids.sourceLocationName, 0, "")
	require.NoError(t, err)
	require.Len(t, vsList, 1)
	assert.Equal(t, ids.vodSourceName, vsList[0].VodSourceName)

	// liveSources table + liveSourcesByLocation index.
	ls, err := fresh.DescribeLiveSource(ids.sourceLocationName, ids.liveSourceName)
	require.NoError(t, err)
	assert.Equal(t, ids.liveSourceName, ls.LiveSourceName)

	lsList, _, err := fresh.ListLiveSources(ids.sourceLocationName, 0, "")
	require.NoError(t, err)
	require.Len(t, lsList, 1)
	assert.Equal(t, ids.liveSourceName, lsList[0].LiveSourceName)

	// prefetchSchedules table + prefetchSchedulesByConfig index.
	ps, err := fresh.GetPrefetchSchedule(ids.playbackConfigName, ids.prefetchScheduleID)
	require.NoError(t, err)
	assert.Equal(t, ids.prefetchScheduleID, ps.Name)

	psList, _, err := fresh.ListPrefetchSchedules(ids.playbackConfigName, "", "", 0, "")
	require.NoError(t, err)
	require.Len(t, psList, 1)

	// programs table + programsByChannel index.
	prog, err := fresh.DescribeProgram(ids.channelName, ids.programName)
	require.NoError(t, err)
	assert.Equal(t, ids.programName, prog.ProgramName)

	schedule, _, err := fresh.GetChannelSchedule(ids.channelName, 0, "")
	require.NoError(t, err)
	require.Len(t, schedule, 1)
	assert.Equal(t, ids.programName, schedule[0].ProgramName)

	// functions table.
	fn, err := fresh.GetFunction(ids.functionID)
	require.NoError(t, err)
	assert.Equal(t, ids.functionID, fn.FunctionID)

	// tags raw map (left un-converted, persisted alongside the tables).
	tags, err := fresh.ListTagsForResource(cfg.PlaybackConfigurationARN)
	require.NoError(t, err)
	assert.Equal(t, "mediatailor", tags["team"])

	// Sanity: account/region carried through too.
	assert.Equal(t, "us-east-1", fresh.Region())
	assert.Equal(t, "000000000000", fresh.AccountID())
}

// TestInMemoryBackend_SnapshotRestore_ChannelPolicyNotPersisted documents
// that channelPolicies (a map[string]string, not a map[string]*T) was never
// carried through Snapshot/Restore before this conversion -- it is not
// registered on the store.Registry and is not added to backendSnapshot, so
// it stays that way after the conversion too.
func TestInMemoryBackend_SnapshotRestore_ChannelPolicyNotPersisted(t *testing.T) {
	t.Parallel()

	original, ids := newPersistenceTestBackend(t)
	require.NoError(t, original.PutChannelPolicy(ids.channelName, `{"Version":"2012-10-17"}`))

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	_, err := fresh.GetChannelPolicy(ids.channelName)
	require.ErrorIs(t, err, mediatailor.ErrNotFound)
}

// TestInMemoryBackend_SnapshotRestore_PlaybackConfigAdsPersonalization
// verifies AdsPersonalizationConcurrency/AdsPersonalizationTimeouts (stored
// via PutPlaybackConfiguration's pre-existing Extra pass-through, see
// gopherstack-gt9o) survive Snapshot -> Restore -- no new persisted field or
// mediatailorSnapshotVersion bump was needed since Extra already carries
// them.
func TestInMemoryBackend_SnapshotRestore_PlaybackConfigAdsPersonalization(t *testing.T) {
	t.Parallel()

	original := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	extra := map[string]any{
		"AdsPersonalizationConcurrency": map[string]any{
			"MaxConcurrentAdsRequests": float64(3),
		},
		"AdsPersonalizationTimeouts": map[string]any{
			"AdsRequestTimeoutMilliseconds": float64(4200),
		},
	}
	_, err := original.PutPlaybackConfiguration(
		"pc-ads", "https://ads.example.com", "https://video.example.com", nil, extra,
	)
	require.NoError(t, err)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	cfg, err := fresh.GetPlaybackConfiguration("pc-ads")
	require.NoError(t, err)
	require.NotNil(t, cfg.Extra)

	concurrency, ok := cfg.Extra["AdsPersonalizationConcurrency"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(3), concurrency["MaxConcurrentAdsRequests"], 0.0001)

	timeouts, ok := cfg.Extra["AdsPersonalizationTimeouts"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(4200), timeouts["AdsRequestTimeoutMilliseconds"], 0.0001)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b, ids := newPersistenceTestBackend(t)

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, mediatailor.PlaybackConfigurationCount(b))
	assert.Equal(t, 0, mediatailor.ChannelCount(b))
	assert.Equal(t, 0, mediatailor.SourceLocationCount(b))
	assert.Equal(t, 0, mediatailor.VodSourceCount(b))

	_, err = b.GetPlaybackConfiguration(ids.playbackConfigName)
	require.ErrorIs(t, err, mediatailor.ErrNotFound)

	_, err = b.DescribeChannel(ids.channelName)
	require.ErrorIs(t, err, mediatailor.ErrNotFound)

	_, err = b.ListTagsForResource("arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/pc1")
	require.NoError(t, err) // tags reset to an empty map, not an error.
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to the backend. Before Phase 3.3, MediaTailor had no such
// delegation even though InMemoryBackend implemented both methods -- dead
// wiring, since nothing ever called them through the Handler/Persistable
// path the rest of the fleet uses.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	backend, ids := newPersistenceTestBackend(t)
	h := mediatailor.NewHandler(backend)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := mediatailor.NewHandler(mediatailor.NewInMemoryBackend("000000000000", "us-east-1"))
	require.NoError(t, h2.Restore(t.Context(), snap))

	cfg, err := h2.Backend.GetPlaybackConfiguration(ids.playbackConfigName)
	require.NoError(t, err)
	assert.Equal(t, ids.playbackConfigName, cfg.Name)
}

// TestSnapshotRestore_LiveSourcesPrefetchSchedulesPrograms verifies
// Snapshot/Restore persists live sources, prefetch schedules, and programs.
func TestSnapshotRestore_LiveSourcesPrefetchSchedulesPrograms(t *testing.T) {
	t.Parallel()

	b1 := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b1.CreateSourceLocation("sl1", "https://example.com", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b1.CreateLiveSource("sl1", "ls1", nil, nil)
	require.NoError(t, err)

	_, err = b1.PutPlaybackConfiguration("pc1", "https://ads.com", "https://video.com", nil, nil)
	require.NoError(t, err)

	_, err = b1.CreatePrefetchSchedule("pc1", "sched1", "", "", nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b1.CreateChannel("ch1", "LOOP", "", nil, nil, nil, nil, nil)
	require.NoError(t, err)

	_, err = b1.CreateProgram(
		"ch1", "prog1", "sl1", "", "ls1", testScheduleConfig(1_700_000_000_000), nil, nil, nil,
	)
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())

	b2 := mediatailor.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	ls, err := b2.DescribeLiveSource("sl1", "ls1")
	require.NoError(t, err)
	assert.Equal(t, "ls1", ls.LiveSourceName)

	sched, err := b2.GetPrefetchSchedule("pc1", "sched1")
	require.NoError(t, err)
	assert.Equal(t, "sched1", sched.Name)

	prog, err := b2.DescribeProgram("ch1", "prog1")
	require.NoError(t, err)
	assert.Equal(t, "prog1", prog.ProgramName)
}
