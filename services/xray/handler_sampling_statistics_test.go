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

func TestGetSamplingTargets_LastRuleModAndTTL(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "ttl-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	rec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "ttl-rule", "ClientId": "c-1", "RequestCount": 100, "SampledCount": 5, "BorrowCount": 0},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// LastRuleModification should be a timestamp.
	lastMod, ok := resp["LastRuleModification"].(float64)
	require.True(t, ok, "LastRuleModification should be a float64 timestamp")
	assert.Greater(t, lastMod, 0.0)

	// Target should have ReservoirQuotaTTL.
	targets, ok := resp["SamplingTargetDocuments"].([]any)
	require.True(t, ok)
	require.Len(t, targets, 1)

	target, ok := targets[0].(map[string]any)
	require.True(t, ok)

	ttl, ok := target["ReservoirQuotaTTL"].(float64)
	require.True(t, ok, "ReservoirQuotaTTL should be present")
	assert.Greater(t, ttl, float64(time.Now().Unix()), "TTL should be in the future")
}

func TestGetSamplingStatisticSummaries_AfterPut(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "stat-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	// Submit statistics via GetSamplingTargets.
	putRec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "stat-rule", "ClientId": "c-1", "RequestCount": 200, "SampledCount": 10, "BorrowCount": 2},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Now fetch summaries.
	rec := doXrayRequest(t, h, "/SamplingStatisticSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["SamplingStatisticSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "stat-rule", s["RuleName"])
	assert.InDelta(t, 200.0, s["RequestCount"], 0.001)
	assert.InDelta(t, 10.0, s["SampledCount"], 0.001)
	assert.InDelta(t, 2.0, s["BorrowCount"], 0.001)
}

func TestGetSamplingTargets_UnprocessedWithErrorCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "no-such-rule", "ClientId": "c-1", "RequestCount": 10, "SampledCount": 1, "BorrowCount": 0},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	unprocessed, ok := resp["UnprocessedStatistics"].([]any)
	require.True(t, ok)
	require.Len(t, unprocessed, 1)

	u, ok := unprocessed[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "no-such-rule", u["RuleName"])
	assert.NotEmpty(t, u["ErrorCode"])
	assert.NotEmpty(t, u["Message"])
}

// TestGetSamplingTargets_EmptyClientID verifies empty ClientID → unprocessed.
func TestGetSamplingTargets_EmptyClientID(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, Priority: 1})

	rec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
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

func TestHandler_GetSamplingStatisticSummaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "returns empty list",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/SamplingStatisticSummaries", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, ok := resp["SamplingStatisticSummaries"].([]any)
			require.True(t, ok)
			assert.Empty(t, summaries)
		})
	}
}

func TestHandler_GetSamplingTargets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantStatus      int
		wantTargets     int
		wantUnprocessed int
	}{
		{
			name: "returns target for existing rule",
			body: map[string]any{
				"SamplingStatisticsDocuments": []map[string]any{
					{
						"RuleName":     "my-rule",
						"ClientId":     "client-1",
						"RequestCount": 100,
						"SampledCount": 5,
						"BorrowCount":  0,
					},
				},
			},
			wantStatus:      http.StatusOK,
			wantTargets:     1,
			wantUnprocessed: 0,
		},
		{
			name: "returns unprocessed for unknown rule",
			body: map[string]any{
				"SamplingStatisticsDocuments": []map[string]any{
					{
						"RuleName":     "no-such-rule",
						"ClientId":     "client-1",
						"RequestCount": 10,
						"SampledCount": 1,
						"BorrowCount":  0,
					},
				},
			},
			wantStatus:      http.StatusOK,
			wantTargets:     0,
			wantUnprocessed: 1,
		},
		{
			name:            "empty documents returns empty response",
			body:            map[string]any{"SamplingStatisticsDocuments": []map[string]any{}},
			wantStatus:      http.StatusOK,
			wantTargets:     0,
			wantUnprocessed: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			// Pre-seed the rule used in the "existing rule" case.
			b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, ReservoirSize: 5})

			rec := doXrayRequest(t, h, "/SamplingTargets", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			targets, ok := resp["SamplingTargetDocuments"].([]any)
			require.True(t, ok)
			assert.Len(t, targets, tt.wantTargets)

			unprocessed, ok := resp["UnprocessedStatistics"].([]any)
			require.True(t, ok)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

func TestHandler_GetSamplingTargets_TargetFields(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "check-rule", FixedRate: 0.1, ReservoirSize: 10})

	rec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "check-rule", "ClientId": "c-1", "RequestCount": 50, "SampledCount": 5, "BorrowCount": 0},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	targets, ok := resp["SamplingTargetDocuments"].([]any)
	require.True(t, ok)
	require.Len(t, targets, 1)

	target, ok := targets[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "check-rule", target["RuleName"])
	assert.InDelta(t, 0.1, target["FixedRate"], 0.001)
}

func TestHandler_GetSamplingStatisticSummaries_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "empty body accepted",
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "NextToken field accepted",
			body:       map[string]any{"NextToken": ""},
			wantStatus: http.StatusOK,
		},
		{
			name:       "response includes both required keys",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/SamplingStatisticSummaries", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "SamplingStatisticSummaries")
			assert.Contains(t, resp, "NextToken")
		})
	}
}

func TestGetSamplingTargets_LastRuleModification(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "mod-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	rec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "mod-rule", "ClientId": "c-1", "RequestCount": 100, "SampledCount": 5, "BorrowCount": 0},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	lastMod, ok := resp["LastRuleModification"].(float64)
	require.True(t, ok, "LastRuleModification must be present")
	assert.Greater(t, lastMod, 0.0, "LastRuleModification must be a positive timestamp")
}

func TestGetSamplingTargets_ReservoirQuotaTTL(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "ttl-rule", FixedRate: 0.1, ReservoirSize: 10, Priority: 1})

	rec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "ttl-rule", "ClientId": "c-1", "RequestCount": 50, "SampledCount": 5, "BorrowCount": 0},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	targets, ok := resp["SamplingTargetDocuments"].([]any)
	require.True(t, ok)
	require.Len(t, targets, 1)

	target, ok := targets[0].(map[string]any)
	require.True(t, ok)

	ttl, ok := target["ReservoirQuotaTTL"].(float64)
	require.True(t, ok, "ReservoirQuotaTTL must be present")
	assert.Greater(t, ttl, float64(time.Now().Unix()), "ReservoirQuotaTTL must be in the future")
}

func TestGetSamplingTargets_UnprocessedForUnknownRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		docs            []map[string]any
		wantTargets     int
		wantUnprocessed int
	}{
		{
			name: "known rule returns target",
			docs: []map[string]any{
				{"RuleName": "known-rule", "ClientId": "c-1", "RequestCount": 10, "SampledCount": 1},
			},
			wantTargets:     1,
			wantUnprocessed: 0,
		},
		{
			name: "unknown rule returns unprocessed",
			docs: []map[string]any{
				{"RuleName": "no-such-rule", "ClientId": "c-1", "RequestCount": 10, "SampledCount": 1},
			},
			wantTargets:     0,
			wantUnprocessed: 1,
		},
		{
			name: "missing ClientId returns unprocessed",
			docs: []map[string]any{
				{"RuleName": "known-rule", "RequestCount": 10, "SampledCount": 1},
			},
			wantTargets:     0,
			wantUnprocessed: 1,
		},
		{
			name:            "empty docs returns empty response",
			docs:            []map[string]any{},
			wantTargets:     0,
			wantUnprocessed: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			b.AddSamplingRuleInternal(
				xray.SamplingRule{RuleName: "known-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1},
			)

			rec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
				"SamplingStatisticsDocuments": tt.docs,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			targets, _ := resp["SamplingTargetDocuments"].([]any)
			assert.Len(t, targets, tt.wantTargets)

			unprocessed, _ := resp["UnprocessedStatistics"].([]any)
			assert.Len(t, unprocessed, tt.wantUnprocessed)
		})
	}
}

func TestGetSamplingStatisticSummaries_AccumulationFromTargets(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "stat-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	// Submit statistics via GetSamplingTargets.
	putRec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "stat-rule", "ClientId": "c-1", "RequestCount": 200, "SampledCount": 10, "BorrowCount": 2},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/SamplingStatisticSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["SamplingStatisticSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "stat-rule", s["RuleName"])
	assert.InDelta(t, 200.0, s["RequestCount"], 0.001)
	assert.InDelta(t, 10.0, s["SampledCount"], 0.001)
	assert.InDelta(t, 2.0, s["BorrowCount"], 0.001)
}

func TestGetSamplingStatisticSummaries_EmptyWithoutStats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/SamplingStatisticSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["SamplingStatisticSummaries"].([]any)
	require.True(t, ok)
	assert.Empty(t, summaries)
}

func TestGetSamplingStatisticSummaries_AccumulatesAcrossCalls(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "accum-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	for range 3 {
		putRec := doXrayRequest(t, h, "/SamplingTargets", map[string]any{
			"SamplingStatisticsDocuments": []map[string]any{
				{"RuleName": "accum-rule", "ClientId": "c-1", "RequestCount": 100, "SampledCount": 5, "BorrowCount": 0},
			},
		})
		require.Equal(t, http.StatusOK, putRec.Code)
	}

	rec := doXrayRequest(t, h, "/SamplingStatisticSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, _ := resp["SamplingStatisticSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	assert.InDelta(t, 300.0, s["RequestCount"], 0.001, "RequestCount must accumulate across calls")
	assert.InDelta(t, 15.0, s["SampledCount"], 0.001, "SampledCount must accumulate across calls")
}
