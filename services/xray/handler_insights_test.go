package xray_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestGetInsightSummaries_ALLState(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddInsightInternal(xray.Insight{InsightID: "all-active", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightInternal(xray.Insight{InsightID: "all-closed", State: "CLOSED", StartTime: time.Now()})

	rec := doXrayRequest(t, h, "/InsightSummaries", map[string]any{
		"States": []any{"ALL"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["InsightSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 2, "ALL should return both ACTIVE and CLOSED insights")
}

func TestGetInsightSummaries_UnknownState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/InsightSummaries", map[string]any{
		"States": []any{"BOGUS"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGroup_NotificationsEnabledRequiresInsightsEnabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
		"GroupName": "bad-group",
		"InsightsConfiguration": map[string]any{
			"InsightsEnabled":      false,
			"NotificationsEnabled": true,
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetInsightSummariesStateFilter verifies state filtering.
func TestGetInsightSummariesStateFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddInsightInternal(xray.Insight{InsightID: "active-1", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightInternal(xray.Insight{InsightID: "active-2", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightInternal(xray.Insight{InsightID: "closed-1", State: "CLOSED", StartTime: time.Now()})

	tests := []struct {
		name      string
		states    []any
		wantCount int
	}{
		{name: "no filter returns all", states: nil, wantCount: 3},
		{name: "filter by ACTIVE", states: []any{"ACTIVE"}, wantCount: 2},
		{name: "filter by CLOSED", states: []any{"CLOSED"}, wantCount: 1},
		{name: "filter by both states", states: []any{"ACTIVE", "CLOSED"}, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{}
			if tt.states != nil {
				body["States"] = tt.states
			}

			rec := doXrayRequest(t, h, "/InsightSummaries", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, ok := resp["InsightSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

// TestGroupInsightsConfigurationRoundtrip verifies InsightsConfiguration in GroupView.
func TestGroupInsightsConfigurationRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"GroupName":        "insights-group",
		"FilterExpression": `service("svc")`,
		"InsightsConfiguration": map[string]any{
			"InsightsEnabled":      true,
			"NotificationsEnabled": false,
		},
	}

	rec := doXrayRequest(t, h, "/CreateGroup", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	group, ok := resp["Group"].(map[string]any)
	require.True(t, ok)

	ic, ok := group["InsightsConfiguration"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, ic["InsightsEnabled"])
}

// TestGetInsightImpactGraphNotFound verifies insight-not-found returns 400.
func TestGetInsightImpactGraphNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"InsightId": "non-existent-insight",
		"StartTime": 1700000000.0,
		"EndTime":   1700001000.0,
	}

	rec := doXrayRequest(t, h, "/InsightImpactGraph", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestHandler_GetInsightEvents_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantStatus  int
		wantHasNext bool
		eventCount  int
	}{
		{
			name:        "no MaxResults returns all events",
			eventCount:  3,
			wantStatus:  http.StatusOK,
			wantHasNext: false,
		},
		{
			name:        "MaxResults limits events and produces NextToken",
			eventCount:  5,
			body:        map[string]any{"MaxResults": int32(2)},
			wantStatus:  http.StatusOK,
			wantHasNext: true,
		},
		{
			name:       "missing InsightId returns error",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			var insightID string
			if tt.eventCount > 0 {
				insightID = "test-insight-id"
				seedInsight(b, insightID, "my-group", "arn:aws:xray:us-east-1:123:group/default/my-group")

				for i := range tt.eventCount {
					seedInsightEvent(b, insightID, fmt.Sprintf("event-%d", i))
				}
			}

			body := map[string]any{}
			if insightID != "" {
				body["InsightId"] = insightID
			}
			maps.Copy(body, tt.body)

			rec := doXrayRequest(t, h, "/InsightEvents", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				nextToken, _ := resp["NextToken"].(string)
				if tt.wantHasNext {
					assert.NotEmpty(t, nextToken)
				} else {
					assert.Empty(t, nextToken)
				}
			}
		})
	}
}

func TestHandler_GetInsightEvents_NextTokenContinuation(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	insightID := "test-paginate-insight"
	seedInsight(b, insightID, "group", "arn:aws:xray:us-east-1:123:group/default/group")

	for i := range 6 {
		seedInsightEvent(b, insightID, fmt.Sprintf("event-%d", i))
	}

	// Page 1
	rec1 := doXrayRequest(t, h, "/InsightEvents", map[string]any{
		"InsightId":  insightID,
		"MaxResults": int32(4),
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	events1, _ := resp1["InsightEvents"].([]any)
	assert.Len(t, events1, 4)
	nextToken := resp1["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Page 2
	rec2 := doXrayRequest(t, h, "/InsightEvents", map[string]any{
		"InsightId":  insightID,
		"MaxResults": int32(4),
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	events2, _ := resp2["InsightEvents"].([]any)
	assert.Len(t, events2, 2)
	assert.Empty(t, resp2["NextToken"])
}

func TestHandler_GetInsightSummaries_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantStatus   int
		wantCount    int
		wantHasNext  bool
		insightCount int
	}{
		{
			name:         "no insights returns empty list",
			insightCount: 0,
			wantStatus:   http.StatusOK,
			wantCount:    0,
			wantHasNext:  false,
		},
		{
			name:         "all insights returned under limit",
			insightCount: 3,
			wantStatus:   http.StatusOK,
			wantCount:    3,
			wantHasNext:  false,
		},
		{
			name:         "MaxResults limits and sets NextToken",
			insightCount: 5,
			body:         map[string]any{"MaxResults": int32(2)},
			wantStatus:   http.StatusOK,
			wantCount:    2,
			wantHasNext:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.insightCount {
				seedInsight(b,
					fmt.Sprintf("insight-%d", i),
					fmt.Sprintf("group-%d", i),
					fmt.Sprintf("arn:aws:xray:us-east-1:123:group/default/group-%d", i),
				)
			}

			rec := doXrayRequest(t, h, "/InsightSummaries", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, _ := resp["InsightSummaries"].([]any)
			assert.Len(t, summaries, tt.wantCount)

			nextToken, _ := resp["NextToken"].(string)
			if tt.wantHasNext {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

func TestHandler_GetInsight_ResponseShape(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	seedInsight(b, "shape-test-id", "my-group", "arn:aws:xray:us-east-1:123456789012:group/default/my-group")

	rec := doXrayRequest(t, h, "/Insight", map[string]any{"InsightId": "shape-test-id"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	insightData, ok := resp["Insight"].(map[string]any)
	require.True(t, ok)

	// Required fields present
	assert.Contains(t, insightData, "InsightId")
	assert.Contains(t, insightData, "GroupARN")
	assert.Contains(t, insightData, "GroupName")
	assert.Contains(t, insightData, "State")
	assert.Contains(t, insightData, "StartTime")

	startTime, _ := insightData["StartTime"].(float64)
	assert.Greater(t, startTime, float64(0))
}

func TestHandler_GetInsightSummaries_ResponseShape(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	seedInsight(b, "summary-shape-id", "my-group", "arn:aws:xray:us-east-1:123456789012:group/default/my-group")

	rec := doXrayRequest(t, h, "/InsightSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["InsightSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	assert.Contains(t, s, "InsightId")
	assert.Contains(t, s, "GroupARN")
	assert.Contains(t, s, "GroupName")
	assert.Contains(t, s, "State")
	assert.Contains(t, s, "StartTime")
	// LastUpdateTime is a real InsightSummary field ("the time...that the insight was
	// last updated") that was previously entirely absent from gopherstack's wire view.
	assert.Contains(t, s, "LastUpdateTime")
}

// TestGetInsightSummaries_LastUpdateTimeReflectsActivity guards against a real gap
// found during parity audit: InsightSummary.LastUpdateTime was completely absent.
// GetInsight's Insight type genuinely has no LastUpdateTime field (unlike
// InsightSummary), so this must only appear on GetInsightSummaries.
func TestGetInsightSummaries_LastUpdateTimeReflectsActivity(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	now := time.Now()
	b.AddInsightInternal(xray.Insight{
		InsightID:      "lut-id",
		GroupName:      "my-group",
		GroupARN:       "arn:aws:xray:us-east-1:123456789012:group/default/my-group",
		State:          "ACTIVE",
		StartTime:      now.Add(-time.Hour),
		LastUpdateTime: now,
	})

	rec := doXrayRequest(t, h, "/InsightSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, ok := resp["InsightSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	lastUpdate, ok := s["LastUpdateTime"].(float64)
	require.True(t, ok)
	assert.InDelta(t, float64(now.Unix()), lastUpdate, 1)

	// GetInsight (singular) must NOT expose LastUpdateTime -- the real Insight type
	// (unlike InsightSummary) has no such field.
	getRec := doXrayRequest(t, h, "/Insight", map[string]any{"InsightId": "lut-id"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	insight, ok := getResp["Insight"].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, insight, "LastUpdateTime")
}

func TestGetInsight_FieldsReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantState   string
		wantSummary string
		insight     xray.Insight
	}{
		{
			name: "active insight fields",
			insight: xray.Insight{
				InsightID: "i-active-001",
				GroupARN:  "arn:aws:xray:us-east-1:123456789012:group/default/my-group",
				GroupName: "my-group",
				State:     "ACTIVE",
				Summary:   "Fault rate spike detected",
				StartTime: time.Unix(1700000000, 0),
			},
			wantState:   "ACTIVE",
			wantSummary: "Fault rate spike detected",
		},
		{
			name: "closed insight fields",
			insight: xray.Insight{
				InsightID: "i-closed-001",
				GroupName: "other-group",
				State:     "CLOSED",
				Summary:   "Fault rate normalised",
				StartTime: time.Unix(1700000500, 0),
			},
			wantState:   "CLOSED",
			wantSummary: "Fault rate normalised",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			b.AddInsightInternal(tt.insight)

			rec := doXrayRequest(t, h, "/Insight", map[string]any{"InsightId": tt.insight.InsightID})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			insight, ok := resp["Insight"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.insight.InsightID, insight["InsightId"])
			assert.Equal(t, tt.wantState, insight["State"])
			assert.Equal(t, tt.wantSummary, insight["Summary"])
		})
	}
}

func TestGetInsight_NotFoundReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/Insight", map[string]any{"InsightId": "no-such-insight"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetInsight_MissingInsightIdReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/Insight", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetInsightEvents_ReturnsEventsForInsight(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddInsightInternal(xray.Insight{InsightID: "i-evt-001", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightEventInternal(xray.InsightEvent{InsightID: "i-evt-001", Summary: "Spike began", EventTime: time.Now()})
	b.AddInsightEventInternal(
		xray.InsightEvent{InsightID: "i-evt-001", Summary: "Spike worsened", EventTime: time.Now()},
	)

	rec := doXrayRequest(t, h, "/InsightEvents", map[string]any{"InsightId": "i-evt-001"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	events, ok := resp["InsightEvents"].([]any)
	require.True(t, ok)
	assert.Len(t, events, 2, "both events must be returned")
}

func TestGetInsightImpactGraph_ReturnsInsightId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "returns impact graph for known insight",
			body: map[string]any{
				"InsightId": "i-impact-001",
				"StartTime": 1700000000.0,
				"EndTime":   1700001000.0,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing InsightId rejected",
			body: map[string]any{
				"StartTime": 1700000000.0,
				"EndTime":   1700001000.0,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown insight rejected",
			body: map[string]any{
				"InsightId": "no-such",
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
			b.AddInsightInternal(xray.Insight{InsightID: "i-impact-001", State: "ACTIVE", StartTime: time.Now()})

			rec := doXrayRequest(t, h, "/InsightImpactGraph", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "i-impact-001", resp["InsightId"])
			}
		})
	}
}

func TestGetInsightSummaries_StatesValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		states     []any
		wantStatus int
		wantCount  int
	}{
		{name: "no states returns all", states: nil, wantStatus: http.StatusOK, wantCount: 2},
		{name: "ALL returns both", states: []any{"ALL"}, wantStatus: http.StatusOK, wantCount: 2},
		{name: "ACTIVE only", states: []any{"ACTIVE"}, wantStatus: http.StatusOK, wantCount: 1},
		{name: "CLOSED only", states: []any{"CLOSED"}, wantStatus: http.StatusOK, wantCount: 1},
		{name: "ACTIVE and CLOSED", states: []any{"ACTIVE", "CLOSED"}, wantStatus: http.StatusOK, wantCount: 2},
		{name: "unknown state rejected", states: []any{"BOGUS"}, wantStatus: http.StatusBadRequest, wantCount: 0},
		{
			name:       "mixed valid and invalid rejected",
			states:     []any{"ACTIVE", "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			b.AddInsightInternal(xray.Insight{InsightID: "sum-active-001", State: "ACTIVE", StartTime: time.Now()})
			b.AddInsightInternal(xray.Insight{InsightID: "sum-closed-001", State: "CLOSED", StartTime: time.Now()})

			body := map[string]any{}
			if tt.states != nil {
				body["States"] = tt.states
			}

			rec := doXrayRequest(t, h, "/InsightSummaries", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				summaries, ok := resp["InsightSummaries"].([]any)
				require.True(t, ok)
				assert.Len(t, summaries, tt.wantCount)
			}
		})
	}
}

func TestGroups_NotificationsEnabledRequiresInsightsEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		insightsEnabled      bool
		notificationsEnabled bool
		wantStatus           int
	}{
		{
			name:                 "insights enabled, notifications enabled allowed",
			insightsEnabled:      true,
			notificationsEnabled: true,
			wantStatus:           http.StatusOK,
		},
		{
			name:                 "insights disabled, notifications disabled allowed",
			insightsEnabled:      false,
			notificationsEnabled: false,
			wantStatus:           http.StatusOK,
		},
		{
			name:                 "insights disabled, notifications enabled rejected",
			insightsEnabled:      false,
			notificationsEnabled: true,
			wantStatus:           http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
				"GroupName": fmt.Sprintf("notif-group-%v-%v", tt.insightsEnabled, tt.notificationsEnabled),
				"InsightsConfiguration": map[string]any{
					"InsightsEnabled":      tt.insightsEnabled,
					"NotificationsEnabled": tt.notificationsEnabled,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
