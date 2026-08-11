package xray_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestXRay_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *xray.InMemoryBackend)
		verify func(t *testing.T, b *xray.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty",
			setup: func(_ *xray.InMemoryBackend) {},
			verify: func(t *testing.T, b *xray.InMemoryBackend) {
				t.Helper()

				assert.Empty(t, b.GetGroups())
			},
		},
		{
			name: "group_and_rule_preserved",
			setup: func(b *xray.InMemoryBackend) {
				_, _ = b.CreateGroup("my-group", `service("my-svc")`)
				rule := xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, ReservoirSize: 50, Priority: 10}
				_, _ = b.CreateSamplingRule(rule)
			},
			verify: func(t *testing.T, b *xray.InMemoryBackend) {
				t.Helper()

				groups := b.GetGroups()
				require.Len(t, groups, 1)
				assert.Equal(t, "my-group", groups[0].GroupName)
				// GetSamplingRules returns the user-created rule + the built-in Default rule.
				rules := b.GetSamplingRules()
				require.GreaterOrEqual(t, len(rules), 2, "expected at least 2 rules (my-rule + Default)")
				ruleNames := make(map[string]bool, len(rules))
				for _, r := range rules {
					ruleNames[r.RuleName] = true
				}
				assert.True(t, ruleNames["my-rule"], "my-rule should be present")
				assert.True(t, ruleNames["Default"], "Default rule should always be present")
			},
		},
		{
			name: "insight_categories_and_impact_statistics_preserved",
			setup: func(b *xray.InMemoryBackend) {
				b.AddInsightInternal(xray.Insight{
					InsightID:  "insight-boost",
					State:      "ACTIVE",
					Summary:    "elevated fault rate",
					Categories: []string{"FAULT"},
					ClientRequestImpactStatistics: &xray.RequestImpactStatistics{
						OkCount: 5, FaultCount: 15, TotalCount: 20,
					},
				})
			},
			verify: func(t *testing.T, b *xray.InMemoryBackend) {
				t.Helper()

				summaries, err := b.GetInsightSummaries([]string{"ACTIVE"})
				require.NoError(t, err)
				require.NotEmpty(t, summaries)

				assert.Equal(t, []string{"FAULT"}, summaries[0].Categories)
				require.NotNil(t, summaries[0].ClientRequestImpactStatistics)
				assert.EqualValues(t, 20, summaries[0].ClientRequestImpactStatistics.TotalCount)
				assert.EqualValues(t, 15, summaries[0].ClientRequestImpactStatistics.FaultCount)
				assert.EqualValues(t, 5, summaries[0].ClientRequestImpactStatistics.OkCount)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := xray.NewInMemoryBackend("000000000000", "us-east-1")
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}

// TestXRay_PersistenceFullStateRoundTrip exercises Snapshot/Restore across
// every store.Table-backed resource (Phase 3.3 pkgs/store conversion),
// including the secondary indexes (groupsByARN, traceSegments) and the
// parsed-segment cache that must be re-derived from each Trace's raw segment
// JSON rather than round-tripped directly (Segment.Document is json:"-").
//
//nolint:paralleltest // reads/writes shared time.Now via helpers
func TestXRay_PersistenceFullStateRoundTrip(t *testing.T) {
	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	// groups (+ groupsByARN secondary index).
	group, err := b.CreateGroup("my-group", `service("my-svc")`)
	require.NoError(t, err)

	// samplingRules.
	_, err = b.CreateSamplingRule(xray.SamplingRule{
		RuleName: "my-rule", FixedRate: 0.05, ReservoirSize: 50, Priority: 10,
	})
	require.NoError(t, err)

	// traces + parsedSegments (+ traceSegments secondary index), via the raw
	// segment JSON path so Segment.Document gets populated exactly as a real
	// PutTraceSegments call would produce it.
	const traceID = "1-abcdef01-000000000000000000000001"

	segJSON := `{"trace_id":"` + traceID + `","id":"seg1","name":"my-service",` +
		`"start_time":1700000000.5,"end_time":1700000001.25}`
	unprocessed := b.PutTraceSegments([]string{segJSON})
	require.Empty(t, unprocessed)

	// insights + insightEvents.
	b.AddInsightInternal(xray.Insight{
		InsightID: "insight-1", GroupARN: group.GroupARN, GroupName: group.GroupName,
		State: "ACTIVE", Summary: "elevated fault rate",
	})
	b.AddInsightEventInternal(xray.InsightEvent{InsightID: "insight-1", Summary: "event-1"})

	// resourcePolicies.
	_, err = b.PutResourcePolicy("my-policy", `{"Version":"2012-10-17"}`, "")
	require.NoError(t, err)

	// traceRetrievals + retrievedTraces.
	token := b.StartTraceRetrieval([]string{traceID})
	require.NotEmpty(t, token)

	// samplingStats.
	_, unprocessedStats, _ := b.GetSamplingTargets([]xray.SamplingStatisticsDocument{
		{RuleName: "my-rule", ClientID: "client-1", RequestCount: 10, SampledCount: 5},
	}, nil)
	require.Empty(t, unprocessedStats)

	// encryptionConfig.
	_, err = b.PutEncryptionConfig("KMS", "alias/my-key")
	require.NoError(t, err)

	// indexingRules.
	_, err = b.UpdateIndexingRule("Default", &xray.ProbabilisticRuleValue{DesiredSamplingPercentage: 5})
	require.NoError(t, err)

	// resourceTags.
	require.NoError(t, b.TagResource(group.GroupARN, map[string]string{"env": "prod"}))

	// traceSegmentDest.
	dest := b.UpdateTraceSegmentDestination("CloudWatchLogs")
	require.Equal(t, "CloudWatchLogs", dest)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	// groups + groupsByARN index rebuilt.
	got, err := b2.GetGroup("my-group")
	require.NoError(t, err)
	assert.Equal(t, group.GroupARN, got.GroupARN)
	byARN, err := b2.GetGroupByARN(group.GroupARN)
	require.NoError(t, err)
	assert.Equal(t, "my-group", byARN.GroupName)

	// samplingRules (+ built-in Default).
	rules := b2.GetSamplingRules()
	ruleNames := make(map[string]bool, len(rules))
	for _, r := range rules {
		ruleNames[r.RuleName] = true
	}
	assert.True(t, ruleNames["my-rule"])
	assert.True(t, ruleNames["Default"])

	// traces + parsedSegments/traceSegments rebuilt from raw segment JSON,
	// including the json:"-" Document field.
	trace := b2.GetTrace(traceID)
	require.NotNil(t, trace)
	require.Len(t, trace.Segments, 1)
	assert.JSONEq(t, segJSON, trace.Segments[0])

	segs := b2.GetParsedSegments(traceID)
	require.Len(t, segs, 1)
	assert.Equal(t, "seg1", segs[0].ID)
	assert.Equal(t, traceID, segs[0].TraceID)
	assert.JSONEq(t, segJSON, segs[0].Document, "Document must survive restore despite its json:\"-\" tag")

	// insights + insightEvents.
	ins, err := b2.GetInsight("insight-1")
	require.NoError(t, err)
	assert.Equal(t, "elevated fault rate", ins.Summary)
	events, err := b2.GetInsightEvents("insight-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "event-1", events[0].Summary)

	// resourcePolicies.
	policies := b2.ListResourcePolicies()
	require.Len(t, policies, 1)
	assert.Equal(t, "my-policy", policies[0].PolicyName)

	// traceRetrievals + retrievedTraces.
	status, retrieved, err := b2.ListRetrievedTraces(token)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", status)
	require.Len(t, retrieved, 1)
	assert.Equal(t, traceID, retrieved[0].TraceID)

	// resourceTags.
	tags, err := b2.ListTagsForResource(group.GroupARN)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0]["Key"])
	assert.Equal(t, "prod", tags[0]["Value"])

	// samplingStats.
	stats := b2.GetSamplingStatisticSummaries()
	require.Len(t, stats, 1)
	assert.Equal(t, "my-rule", stats[0].RuleName)
	assert.Equal(t, int32(10), stats[0].RequestCount)

	// encryptionConfig.
	cfg := b2.GetEncryptionConfig()
	assert.Equal(t, "KMS", cfg.Type)
	assert.Equal(t, "alias/my-key", cfg.KeyID)

	// indexingRules, including the Rule.DesiredSamplingPercentage set above.
	found := false
	for _, r := range b2.GetIndexingRules() {
		if r.Name == "Default" {
			found = true
			require.NotNil(t, r.Rule)
			assert.InDelta(t, 5.0, r.Rule.DesiredSamplingPercentage, 0)
		}
	}
	assert.True(t, found)

	// traceSegmentDest must survive the round trip; UpdateTraceSegmentDestination
	// mutates real backend state, so Restore losing it would silently revert
	// callers to the default "XRay" destination after a gopherstack restart.
	assert.Equal(t, "CloudWatchLogs", b2.GetTraceSegmentDestination())
}

// TestXRay_Reset_ClearsTraceSegmentDestination guards against Reset leaving
// traceSegmentDest stale: every other mutable field on InMemoryBackend is
// cleared by Reset (see InMemoryBackend.Reset), and traceSegmentDest must be
// too so a fresh Reset() truly returns to the default "XRay" destination.
func TestXRay_Reset_ClearsTraceSegmentDestination(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	dest := b.UpdateTraceSegmentDestination("CloudWatchLogs")
	require.Equal(t, "CloudWatchLogs", dest)

	b.Reset()

	assert.Equal(t, "XRay", b.GetTraceSegmentDestination())
}

// TestXRay_Reset_ClearsResourceTags guards against a real leak found during parity
// audit: resourceTags is a plain map (not store.Table-backed, so registry.ResetAll()
// does not touch it), and unlike every other plain-map field on InMemoryBackend
// (insightEvents, retrievedTraces, retrievalTimes), it was never explicitly cleared by
// Reset() -- the same bug class previously fixed for traceSegmentDest but missed here.
func TestXRay_Reset_ClearsResourceTags(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	group, err := b.CreateGroup("tag-reset-group", "")
	require.NoError(t, err)
	require.NoError(t, b.TagResource(group.GroupARN, map[string]string{"env": "prod"}))

	tagsBefore, err := b.ListTagsForResource(group.GroupARN)
	require.NoError(t, err)
	require.Len(t, tagsBefore, 1)

	b.Reset()

	// The group itself is gone after Reset, so re-create it at the same ARN (accountID
	// and region are unchanged, so the ARN is deterministic) and confirm no tag from
	// before the reset survived.
	group2, err := b.CreateGroup("tag-reset-group", "")
	require.NoError(t, err)
	require.Equal(t, group.GroupARN, group2.GroupARN)

	tagsAfter, err := b.ListTagsForResource(group2.GroupARN)
	require.NoError(t, err)
	assert.Empty(t, tagsAfter, "resourceTags must not survive Reset()")
}

// TestSnapshotRestoreWithEncryptionConfig verifies encryption config persists.
func TestSnapshotRestoreWithEncryptionConfig(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.PutEncryptionConfig("KMS", "arn:aws:kms:us-east-1:123:key/abc")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	cfg := b2.GetEncryptionConfig()
	assert.Equal(t, "KMS", cfg.Type)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/abc", cfg.KeyID)
}

// TestSnapshotRestoreWithIndexingRules verifies indexing rules persist.
func TestSnapshotRestoreWithIndexingRules(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	// Default backend has at least one indexing rule.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	rules := b2.GetIndexingRules()
	assert.NotEmpty(t, rules)
}

// TestSnapshotRestorePreservesGroups verifies group data persists.
func TestSnapshotRestorePreservesGroups(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup("snap-group", `service("svc")`)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	g, err := b2.GetGroup("snap-group")
	require.NoError(t, err)
	assert.Equal(t, "snap-group", g.GroupName)
}

// TestPersistence_RetrievedTracesPersistedInSnapshot verifies retrieval tokens survive snapshot/restore.
func TestPersistence_RetrievedTracesPersistedInSnapshot(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed a trace, then start retrieval.
	now := float64(time.Now().Unix())
	seg := segJSON("1-persist-ret", "s1", "", "svc", now-1, now, false, false, false)
	_ = b.PutTraceSegments([]string{seg})

	token := b.StartTraceRetrieval([]string{"1-persist-ret"})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	status, traces, err := b2.ListRetrievedTraces(token)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", status)
	assert.NotEmpty(t, traces, "retrieved traces should survive snapshot/restore")
}
