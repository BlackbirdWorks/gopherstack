package xray_test

import (
	"testing"

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
	_, unprocessedStats := b.GetSamplingTargets([]xray.SamplingStatisticsDocument{
		{RuleName: "my-rule", ClientID: "client-1", RequestCount: 10, SampledCount: 5},
	})
	require.Empty(t, unprocessedStats)

	// encryptionConfig.
	_, err = b.PutEncryptionConfig("KMS", "alias/my-key")
	require.NoError(t, err)

	// indexingRules.
	_, err = b.UpdateIndexingRule("Default")
	require.NoError(t, err)

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
	status, retrieved := b2.ListRetrievedTraces(token)
	assert.Equal(t, "COMPLETE", status)
	require.Len(t, retrieved, 1)
	assert.Equal(t, traceID, retrieved[0].TraceID)

	// samplingStats.
	stats := b2.GetSamplingStatisticSummaries()
	require.Len(t, stats, 1)
	assert.Equal(t, "my-rule", stats[0].RuleName)
	assert.Equal(t, int32(10), stats[0].RequestCount)

	// encryptionConfig.
	cfg := b2.GetEncryptionConfig()
	assert.Equal(t, "KMS", cfg.Type)
	assert.Equal(t, "alias/my-key", cfg.KeyID)

	// indexingRules.
	found := false
	for _, r := range b2.GetIndexingRules() {
		if r.Name == "Default" {
			found = true
		}
	}
	assert.True(t, found)
}
