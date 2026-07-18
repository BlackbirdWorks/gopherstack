package xray_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetServiceGraph_NonEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-sg-001", "seg1", "", "frontend", now-5, now-4, false, false, false),
			segJSON("1-sg-001", "seg2", "seg1", "backend", now-4, now-3, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{
		"StartTime": now - 10,
		"EndTime":   now + 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, ok := resp["Services"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, services, "service graph should have nodes after segments are stored")
}

func TestGetTimeSeriesServiceStatistics_BucketedData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-ts-001", "seg1", "", "svc", now-30, now-29, false, false, false),
			segJSON("1-ts-002", "seg2", "", "svc", now-10, now-9, false, true, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TimeSeriesServiceStatistics", map[string]any{
		"StartTime": now - 60,
		"EndTime":   now + 60,
		"Period":    60,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stats, ok := resp["TimeSeriesServiceStatistics"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, stats, "should return at least one time-series bucket")

	// Each entry should have Timestamp and ServiceSummaryStatistics.
	entry, ok := stats[0].(map[string]any)
	require.True(t, ok)
	assert.NotNil(t, entry["Timestamp"])
	assert.NotNil(t, entry["ServiceSummaryStatistics"])

	assert.Equal(t, false, resp["ContainsOldGroupVersions"])
}

func TestGetTraceGraph_ReturnsNodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-tg-001", "seg1", "", "service-a", now-5, now-4, false, false, false),
			segJSON("1-tg-001", "seg2", "seg1", "service-b", now-4, now-3, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TraceGraph", map[string]any{
		"TraceIds": []string{"1-tg-001"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, ok := resp["Services"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, services, "trace graph should have service nodes")
}

func TestGetTraceGraph_EmptyForUnknownTraces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceGraph", map[string]any{
		"TraceIds": []string{"1-unknown-trace"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, ok := resp["Services"].([]any)
	require.True(t, ok)
	assert.Empty(t, services)
}

// TestGetTimeSeriesServiceStatistics_InvalidPeriod verifies non-60/300 periods are rejected.
func TestGetTimeSeriesServiceStatistics_InvalidPeriod(t *testing.T) {
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

// TestGetServiceGraph_ContainsOldGroupVersions verifies the field is in response.
func TestGetServiceGraph_ContainsOldGroupVersions(t *testing.T) {
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

// TestGetServiceGraph_MissingTimeReturnsError verifies required time params.
func TestGetServiceGraph_MissingTimeReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "GetServiceGraph without StartTime/EndTime must return 400")
}

func TestHandler_GetServiceGraph_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing StartTime rejected",
			body:       map[string]any{"EndTime": float64(time.Now().Unix())},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing EndTime rejected",
			body:       map[string]any{"StartTime": float64(time.Now().Unix())},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "valid request returns NextToken field",
			body: map[string]any{
				"StartTime": float64(time.Now().Add(-time.Hour).Unix()),
				"EndTime":   float64(time.Now().Unix()),
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/ServiceGraph", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "NextToken")
				assert.Contains(t, resp, "Services")
				assert.Contains(t, resp, "StartTime")
				assert.Contains(t, resp, "EndTime")
			}
		})
	}
}

func TestHandler_GetTraceGraph_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing TraceIds rejected",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "valid request returns NextToken field",
			body:       map[string]any{"TraceIds": []string{"1-abc-123"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "NextToken in request accepted",
			body: map[string]any{
				"TraceIds":  []string{"1-abc-def"},
				"NextToken": "",
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/TraceGraph", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "NextToken")
				assert.Contains(t, resp, "Services")
			}
		})
	}
}

func TestHandler_GetTimeSeriesServiceStatistics_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing both times rejected",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid period rejected",
			body: map[string]any{
				"StartTime": float64(time.Now().Add(-time.Hour).Unix()),
				"EndTime":   float64(time.Now().Unix()),
				"Period":    30,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "period 60 accepted and returns NextToken",
			body: map[string]any{
				"StartTime": float64(time.Now().Add(-time.Hour).Unix()),
				"EndTime":   float64(time.Now().Unix()),
				"Period":    60,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "period 300 accepted",
			body: map[string]any{
				"StartTime": float64(time.Now().Add(-time.Hour).Unix()),
				"EndTime":   float64(time.Now().Unix()),
				"Period":    300,
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/TimeSeriesServiceStatistics", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "NextToken")
				assert.Contains(t, resp, "TimeSeriesServiceStatistics")
			}
		})
	}
}

func TestGetServiceGraph_NodesAfterSegments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-sg-001","id":"r1","name":"web","start_time":%f,"end_time":%f}`, now-5, now-4),
		fmt.Sprintf(
			`{"trace_id":"1-sg-001","id":"c1","parent_id":"r1","name":"api","start_time":%f,"end_time":%f}`,
			now-4,
			now-3,
		),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{"StartTime": now - 10, "EndTime": now + 10})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, ok := resp["Services"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, services, "service graph must have nodes after PutTraceSegments")
}

func TestGetServiceGraph_NodeFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	seg := fmt.Sprintf(
		`{"trace_id":"1-sgf-001","id":"r1","name":"frontend","origin":"AWS::ECS","start_time":%f,"end_time":%f}`,
		now-2,
		now-1,
	)
	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": []string{seg}})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{"StartTime": now - 10, "EndTime": now + 10})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["Services"].([]any)
	require.NotEmpty(t, services)

	svc, ok := services[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, svc["Name"], "service node must have a Name")
	assert.NotNil(t, svc["SummaryStatistics"], "service node must have SummaryStatistics")
}

func TestGetServiceGraph_RequiresStartAndEndTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing StartTime rejected",
			body:       map[string]any{"EndTime": 1700001000.0},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing EndTime rejected",
			body:       map[string]any{"StartTime": 1700000000.0},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "both present accepted",
			body:       map[string]any{"StartTime": 1700000000.0, "EndTime": 1700001000.0},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/ServiceGraph", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestGetServiceGraph_EmptyOutsideTimeWindow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	seg := fmt.Sprintf(`{"trace_id":"1-sgw-001","id":"s1","name":"svc","start_time":%f}`, now-5)
	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": []string{seg}})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Query window does NOT include the segment.
	rec := doXrayRequest(t, h, "/ServiceGraph", map[string]any{
		"StartTime": now - 100,
		"EndTime":   now - 50,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["Services"].([]any)
	assert.Empty(t, services, "service graph must be empty when no segments fall in the time window")
}

func TestGetTimeSeriesServiceStatistics_BucketsByPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		period     int
		wantStatus int
	}{
		{name: "period 60 accepted", period: 60, wantStatus: http.StatusOK},
		{name: "period 300 accepted", period: 300, wantStatus: http.StatusOK},
		{name: "period 120 rejected", period: 120, wantStatus: http.StatusBadRequest},
		{name: "period 0 defaults to 60", period: 0, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"StartTime": 1700000000.0,
				"EndTime":   1700001000.0,
			}
			if tt.period != 0 {
				body["Period"] = tt.period
			}

			rec := doXrayRequest(t, h, "/TimeSeriesServiceStatistics", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestGetTimeSeriesServiceStatistics_ReturnsContainsOldGroupVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TimeSeriesServiceStatistics", map[string]any{
		"StartTime": 1700000000.0,
		"EndTime":   1700001000.0,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["ContainsOldGroupVersions"]
	assert.True(t, ok, "ContainsOldGroupVersions must be present in response")
}

func TestGetTimeSeriesServiceStatistics_BucketedStatsAfterSegments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-ts-001","id":"s1","name":"svc","start_time":%f,"end_time":%f}`, now-30, now-29),
		fmt.Sprintf(
			`{"trace_id":"1-ts-002","id":"s2","name":"svc","start_time":%f,"end_time":%f,"fault":true}`,
			now-10,
			now-9,
		),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TimeSeriesServiceStatistics", map[string]any{
		"StartTime": now - 60,
		"EndTime":   now + 60,
		"Period":    60,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stats, ok := resp["TimeSeriesServiceStatistics"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, stats, "must return at least one bucket after PutTraceSegments")

	// Each bucket must have Timestamp and ServiceSummaryStatistics.
	entry, ok := stats[0].(map[string]any)
	require.True(t, ok)
	assert.NotNil(t, entry["Timestamp"])
	assert.NotNil(t, entry["ServiceSummaryStatistics"])
}

func TestGetTraceGraph_ReturnsNodesForTraceIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-tg-001","id":"r1","name":"svc-a","start_time":%f}`, now-3),
		fmt.Sprintf(`{"trace_id":"1-tg-001","id":"c1","parent_id":"r1","name":"svc-b","start_time":%f}`, now-2),
		fmt.Sprintf(`{"trace_id":"1-tg-002","id":"r2","name":"svc-c","start_time":%f}`, now-1),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Only request trace 1, not trace 2.
	rec := doXrayRequest(t, h, "/TraceGraph", map[string]any{"TraceIds": []string{"1-tg-001"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, ok := resp["Services"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, services, "trace graph must have nodes for the requested trace")

	// Nodes should come from trace 1 (svc-a, svc-b), not trace 2 (svc-c).
	names := make([]string, 0, len(services))
	for _, s := range services {
		svc, _ := s.(map[string]any)
		names = append(names, svc["Name"].(string))
	}
	assert.Contains(t, names, "svc-a")
	assert.Contains(t, names, "svc-b")
	assert.NotContains(t, names, "svc-c")
}

func TestGetTraceGraph_EmptyForUnknownTrace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceGraph", map[string]any{"TraceIds": []string{"1-no-such-trace"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["Services"].([]any)
	assert.Empty(t, services)
}

func TestGetTraceGraph_RequiresTraceIds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceGraph", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
