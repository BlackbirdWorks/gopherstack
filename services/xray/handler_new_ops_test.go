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

func TestHandler_CancelTraceRetrieval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "cancels with valid token",
			body:       map[string]any{"RetrievalToken": "token-abc"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing RetrievalToken returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-existent token is idempotent",
			body:       map[string]any{"RetrievalToken": "does-not-exist"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CancelTraceRetrieval", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "deletes existing policy",
			setup: func(b *xray.InMemoryBackend) {
				b.AddResourcePolicyInternal(xray.ResourcePolicy{
					PolicyName:       "my-policy",
					PolicyDocument:   `{"Version":"2012-10-17"}`,
					PolicyRevisionID: "rev-1",
				})
			},
			body:       map[string]any{"PolicyName": "my-policy"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing PolicyName returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"PolicyName": "no-such-policy"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doXrayRequest(t, h, "/DeleteResourcePolicy", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetIndexingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantStatus   int
		wantMinRules int
	}{
		{
			name:         "returns default rules",
			wantStatus:   http.StatusOK,
			wantMinRules: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/GetIndexingRules", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			rules, ok := resp["IndexingRules"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(rules), tt.wantMinRules)
		})
	}
}

func TestHandler_GetInsight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "gets existing insight",
			setup: func(b *xray.InMemoryBackend) {
				b.AddInsightInternal(xray.Insight{
					InsightID: "insight-123",
					GroupName: "my-group",
					State:     "ACTIVE",
					StartTime: time.Now(),
				})
			},
			body:       map[string]any{"InsightId": "insight-123"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing InsightId returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"InsightId": "missing-id"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doXrayRequest(t, h, "/Insight", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetInsight_ResponseFields(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddInsightInternal(xray.Insight{
		InsightID: "insight-abc",
		GroupName: "grp",
		State:     "ACTIVE",
		Summary:   "Test summary",
		StartTime: time.Unix(1700000000, 0),
	})

	rec := doXrayRequest(t, h, "/Insight", map[string]any{"InsightId": "insight-abc"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	insight, ok := resp["Insight"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "insight-abc", insight["InsightId"])
	assert.Equal(t, "ACTIVE", insight["State"])
	assert.Equal(t, "Test summary", insight["Summary"])
}

func TestHandler_GetInsightEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus int
		wantEvents int
	}{
		{
			name: "returns empty events for insight with no events",
			setup: func(b *xray.InMemoryBackend) {
				b.AddInsightInternal(xray.Insight{
					InsightID: "i-1",
					State:     "ACTIVE",
					StartTime: time.Now(),
				})
			},
			body:       map[string]any{"InsightId": "i-1"},
			wantStatus: http.StatusOK,
			wantEvents: 0,
		},
		{
			name:       "missing InsightId returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "not found returns 400",
			body:       map[string]any{"InsightId": "no-such"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doXrayRequest(t, h, "/InsightEvents", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				events, ok := resp["InsightEvents"].([]any)
				require.True(t, ok)
				assert.Len(t, events, tt.wantEvents)
			}
		})
	}
}

func TestHandler_GetInsightImpactGraph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "returns impact graph for existing insight",
			setup: func(b *xray.InMemoryBackend) {
				b.AddInsightInternal(xray.Insight{
					InsightID: "insight-xyz",
					State:     "ACTIVE",
					StartTime: time.Now(),
				})
			},
			body: map[string]any{
				"InsightId": "insight-xyz",
				"StartTime": 1700000000.0,
				"EndTime":   1700001000.0,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing InsightId returns 400",
			body:       map[string]any{"StartTime": 1700000000.0, "EndTime": 1700001000.0},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non-existent insight returns 400",
			body: map[string]any{
				"InsightId": "no-such-insight",
				"StartTime": 1700000000.0,
				"EndTime":   1700001000.0,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doXrayRequest(t, h, "/InsightImpactGraph", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				assert.Equal(t, "insight-xyz", resp["InsightId"])

				services, ok := resp["Services"].([]any)
				require.True(t, ok)
				assert.Empty(t, services)
			}
		})
	}
}

func TestHandler_GetInsightSummaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*xray.InMemoryBackend)
		body       map[string]any
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "returns empty list",
			body:       map[string]any{"StartTime": 1700000000.0, "EndTime": 1700001000.0},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
		{
			name: "returns seeded insights",
			setup: func(b *xray.InMemoryBackend) {
				b.AddInsightInternal(xray.Insight{
					InsightID: "i-a",
					State:     "ACTIVE",
					StartTime: time.Now(),
				})
				b.AddInsightInternal(xray.Insight{
					InsightID: "i-b",
					State:     "CLOSED",
					StartTime: time.Now(),
				})
			},
			body:       map[string]any{"StartTime": 1700000000.0, "EndTime": 1700001000.0},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doXrayRequest(t, h, "/InsightSummaries", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, ok := resp["InsightSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

func TestHandler_GetRetrievedTracesGraph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantRetrievalSt string
		wantStatus      int
	}{
		{
			name:            "returns COMPLETE for unknown token",
			body:            map[string]any{"RetrievalToken": "unknown-token"},
			wantStatus:      http.StatusOK,
			wantRetrievalSt: "COMPLETE",
		},
		{
			name:       "missing RetrievalToken returns 400",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/GetRetrievedTracesGraph", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				assert.Equal(t, tt.wantRetrievalSt, resp["RetrievalStatus"])

				services, ok := resp["Services"].([]any)
				require.True(t, ok)
				assert.Empty(t, services)
			}
		})
	}
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
