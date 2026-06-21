package xray_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// --- Default sampling rule tests ---

// TestRefinement2_DefaultSamplingRulePresent verifies the Default rule is always seeded.
func TestRefinement2_DefaultSamplingRulePresent(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	rules := b.GetSamplingRules()

	var found bool

	for _, r := range rules {
		if r.RuleName == "Default" {
			found = true
			assert.EqualValues(t, 10000, r.Priority, "Default rule must have priority 10000")
			assert.NotEmpty(t, r.RuleARN, "Default rule must have an ARN")
		}
	}

	assert.True(t, found, "Default sampling rule must always be present in a new backend")
}

// TestRefinement2_DefaultSamplingRuleUndeletable verifies the Default rule cannot be deleted via handler.
func TestRefinement2_DefaultSamplingRuleUndeletable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/DeleteSamplingRule", map[string]any{
		"RuleName": "Default",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "deleting Default rule must return 400")
}

// TestRefinement2_DefaultSamplingRuleUndeletableBackend verifies the Default rule cannot be deleted via backend.
func TestRefinement2_DefaultSamplingRuleUndeletableBackend(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	_, err := b.DeleteSamplingRule("Default")
	require.Error(t, err, "DeleteSamplingRule(Default) must return an error")
	assert.ErrorIs(t, err, xray.ErrDefaultRuleUndeletable)
}

// TestRefinement2_DefaultSamplingRuleSortedLast verifies Default rule is last in priority-sorted list.
func TestRefinement2_DefaultSamplingRuleSortedLast(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()
	_, err := b.CreateSamplingRule(xray.SamplingRule{RuleName: "my-rule", Priority: 100, FixedRate: 0.1})
	require.NoError(t, err)

	rules := b.GetSamplingRules()
	require.NotEmpty(t, rules)
	assert.Equal(t, "Default", rules[len(rules)-1].RuleName,
		"Default rule (priority 10000) must be sorted last")
}

// TestRefinement2_ResetPreservesDefaultRule verifies Reset() re-seeds the Default rule.
func TestRefinement2_ResetPreservesDefaultRule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Reset()

	rules := h.Backend.GetSamplingRules()
	var found bool

	for _, r := range rules {
		if r.RuleName == "Default" {
			found = true
		}
	}

	assert.True(t, found, "Default rule must be present after Reset()")
}

// --- PutTraceSegments malformed segment ---

// TestRefinement2_PutTraceSegments_MalformedJSON verifies malformed JSON doesn't crash.
func TestRefinement2_PutTraceSegments_MalformedJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			"{not-valid-json",
			`{"trace_id":"1-valid","id":"s1","name":"ok"}`,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "malformed segments should not crash the handler")

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	unprocessed, ok := resp["UnprocessedTraceSegments"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed, 1, "only the invalid segment should be unprocessed")
}

// TestRefinement2_PutTraceSegments_MissingTraceID verifies missing trace_id → unprocessed.
func TestRefinement2_PutTraceSegments_MissingTraceID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			`{"id":"s1","name":"no-trace-id"}`,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	unprocessed, ok := resp["UnprocessedTraceSegments"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed, 1, "segment missing trace_id should be unprocessed")

	u, ok := unprocessed[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, u["Id"], "unprocessed segment must have an Id")
	assert.NotEmpty(t, u["ErrorCode"])
}

// --- GetTraceSummaries time window filtering ---

// TestRefinement2_GetTraceSummaries_TimeWindowExclusion verifies traces outside the window are excluded.
func TestRefinement2_GetTraceSummaries_TimeWindowExclusion(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	past := time.Unix(1600000000, 0)
	recent := time.Now()

	b.PutTraceForTest(past)
	b.PutTraceForTest(recent)

	// Filter to only recent traces.
	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"StartTime": float64(recent.Unix() - 10),
		"EndTime":   float64(recent.Unix() + 10),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1, "only the recent trace should be within the window")
}

// --- GetTimeSeriesServiceStatistics period validation ---

// TestRefinement2_GetTimeSeriesServiceStatistics_InvalidPeriod verifies non-60/300 periods are rejected.
func TestRefinement2_GetTimeSeriesServiceStatistics_InvalidPeriod(t *testing.T) {
	t.Parallel()

	now := float64(time.Now().Unix())

	tests := []struct {
		name       string
		period     int
		wantStatus int
	}{
		{name: "period 60 accepted", period: 60, wantStatus: http.StatusOK},
		{name: "period 300 accepted", period: 300, wantStatus: http.StatusOK},
		{name: "period 120 rejected", period: 120, wantStatus: http.StatusBadRequest},
		{name: "period 1 rejected", period: 1, wantStatus: http.StatusBadRequest},
		{name: "period 0 defaults to 60", period: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"StartTime": now - 300,
				"EndTime":   now,
			}
			if tt.period != 0 {
				body["Period"] = tt.period
			}

			rec := doXrayRequest(t, h, "/TimeSeriesServiceStatistics", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- GetEncryptionConfig default ---

// TestRefinement2_GetEncryptionConfig_DefaultIsNONE verifies fresh backend returns Type=NONE.
func TestRefinement2_GetEncryptionConfig_DefaultIsNONE(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayGETRequest(t, h)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	enc, ok := resp["EncryptionConfig"].(map[string]any)
	require.True(t, ok, "EncryptionConfig must be present in response")
	assert.Equal(t, "NONE", enc["Type"], "default encryption type must be NONE")
	assert.Equal(t, "ACTIVE", enc["Status"], "default encryption status must be ACTIVE")
}

// --- PutEncryptionConfig type validation ---

// TestRefinement2_PutEncryptionConfig_InvalidType verifies invalid Type is rejected.
func TestRefinement2_PutEncryptionConfig_InvalidType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{
		"Type": "INVALID_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "invalid Type must return 400")
}

// --- TraceSummary Users field ---

// TestRefinement2_TraceSummary_UsersFromAnnotations verifies Users field is derived from annotation.user.
func TestRefinement2_TraceSummary_UsersFromAnnotations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())
	seg := `{"trace_id":"1-users-001","id":"s1","name":"svc","start_time":` +
		segFmt(now-1) + `,"end_time":` + segFmt(now) +
		`,"annotations":{"user":"alice"}}`

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{seg},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, summaries)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	users, ok := s["Users"].([]any)
	require.True(t, ok, "Users field must be present when annotation.user is set")
	require.Len(t, users, 1)
	assert.Equal(t, "alice", users[0])
}

// segFmt formats a float64 timestamp as a JSON value string (no quotes).
func segFmt(ts float64) string {
	out, _ := json.Marshal(ts)

	return string(out)
}

// --- TraceSummary ForecastStatistics field ---

// TestRefinement2_TraceSummary_ForecastStatisticsPresent verifies ForecastStatistics is in response.
func TestRefinement2_TraceSummary_ForecastStatisticsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-forecast-001", "s1", "", "svc", now-1, now, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, summaries)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	// ForecastStatistics must be present (even as empty object).
	_, hasForecast := s["ForecastStatistics"]
	assert.True(t, hasForecast, "ForecastStatistics must be present in trace summaries")
}

// --- GetServiceGraph ContainsOldGroupVersions field ---

// TestRefinement2_GetServiceGraph_ContainsOldGroupVersions verifies the field is in response.
func TestRefinement2_GetServiceGraph_ContainsOldGroupVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{
		"StartTime": now - 60,
		"EndTime":   now,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["ContainsOldGroupVersions"]
	assert.True(t, ok, "ContainsOldGroupVersions must be present in GetServiceGraph response")
}

// --- GetSamplingTargets ClientID validation ---

// TestRefinement2_GetSamplingTargets_EmptyClientID verifies empty ClientID → unprocessed.
func TestRefinement2_GetSamplingTargets_EmptyClientID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, Priority: 1})

	rec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{
				"RuleName":     "my-rule",
				"ClientId":     "", // empty — should be rejected
				"RequestCount": 10,
				"SampledCount": 1,
				"BorrowCount":  0,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	unprocessed, ok := resp["UnprocessedStatistics"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed, 1, "empty ClientID should result in unprocessed entry")

	targets, ok := resp["SamplingTargetDocuments"].([]any)
	require.True(t, ok)
	assert.Empty(t, targets, "no target should be returned for empty ClientID")
}

// --- Janitor cleanup of segment indexes ---

// TestRefinement2_Janitor_CleansUpSegmentIndexes verifies janitor removes parsedSegments and traceSegments.
func TestRefinement2_Janitor_CleansUpSegmentIndexes(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()

	// Seed a trace with a parsed segment.
	now := float64(time.Now().Unix())
	seg := segJSON("1-janitor-001", "s1", "", "svc", now-1, now, false, false, false)
	_ = b.PutTraceSegments([]string{seg})

	require.Equal(t, 1, b.TraceCount())
	require.NotEmpty(t, b.GetParsedSegments("1-janitor-001"))

	// Run janitor with a TTL of 0 to evict immediately.
	j := xray.NewJanitor(b, time.Millisecond, time.Millisecond)
	j.SweepOnce(t.Context())

	// Trace and segment indexes should all be gone.
	assert.Equal(t, 0, b.TraceCount())
	assert.Empty(t, b.GetParsedSegments("1-janitor-001"), "parsedSegments should be cleaned up by janitor")
}

// --- ListResourcePolicies NextToken present ---

// TestRefinement2_ListResourcePolicies_NextTokenInResponse verifies NextToken field is present.
func TestRefinement2_ListResourcePolicies_NextTokenInResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["NextToken"]
	assert.True(t, ok, "NextToken must be present in ListResourcePolicies response")
}

// --- GetServiceGraph requires StartTime/EndTime ---

// TestRefinement2_GetServiceGraph_MissingTimeReturnsError verifies required time params.
func TestRefinement2_GetServiceGraph_MissingTimeReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "GetServiceGraph without StartTime/EndTime must return 400")
}

// --- Sampling rule sorted by priority ---

// TestRefinement2_SamplingRules_SortedByPriority verifies rules are returned sorted by priority ascending.
func TestRefinement2_SamplingRules_SortedByPriority(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()

	// Create rules with various priorities.
	rules := []xray.SamplingRule{
		{RuleName: "high-prio", Priority: 500, FixedRate: 0.1},
		{RuleName: "low-prio", Priority: 9000, FixedRate: 0.05},
		{RuleName: "top-prio", Priority: 50, FixedRate: 0.5},
	}

	for _, r := range rules {
		_, err := b.CreateSamplingRule(r)
		require.NoError(t, err)
	}

	got := b.GetSamplingRules()
	require.NotEmpty(t, got)

	// Verify ascending priority order.
	for i := 1; i < len(got); i++ {
		assert.LessOrEqual(t, got[i-1].Priority, got[i].Priority,
			"rules must be sorted by priority ascending (index %d→%d: %s→%s)",
			i-1, i, got[i-1].RuleName, got[i].RuleName)
	}
}

// --- Persistence includes retrievedTraces ---

// TestRefinement2_Persistence_RetrievedTracesPersistedInSnapshot verifies retrieval tokens survive snapshot/restore.
func TestRefinement2_Persistence_RetrievedTracesPersistedInSnapshot(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend()

	// Seed a trace, then start retrieval.
	now := float64(time.Now().Unix())
	seg := segJSON("1-persist-ret", "s1", "", "svc", now-1, now, false, false, false)
	_ = b.PutTraceSegments([]string{seg})

	token := b.StartTraceRetrieval([]string{"1-persist-ret"})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := xray.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	status, traces := b2.ListRetrievedTraces(token)
	assert.Equal(t, "COMPLETE", status)
	assert.NotEmpty(t, traces, "retrieved traces should survive snapshot/restore")
}

// --- BatchGetTraces UnprocessedTraceIds ---

// TestRefinement2_BatchGetTraces_UnprocessedForMissingIDs verifies unknown IDs go to UnprocessedTraceIds.
func TestRefinement2_BatchGetTraces_UnprocessedForMissingIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/Traces", map[string]any{
		"TraceIds": []string{"1-missing-a", "1-missing-b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	unprocessed, ok := resp["UnprocessedTraceIds"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed, 2, "unknown trace IDs must appear in UnprocessedTraceIds")

	traces, ok := resp["Traces"].([]any)
	require.True(t, ok)
	assert.Empty(t, traces)
}
