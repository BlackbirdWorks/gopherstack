package xray_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestHandler_GetTraceSummaries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"StartTime": 1700000000.0,
		"EndTime":   1700001000.0,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_BatchGetTraces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*xray.Handler)
		body        map[string]any
		name        string
		wantStatus  int
		wantTraces  int
		wantMissing int
	}{
		{
			name: "returns known trace",
			setup: func(h *xray.Handler) {
				_ = h.Backend.PutTraceSegments([]string{`{"trace_id":"1-abc123","id":"s1","name":"test"}`})
			},
			body:        map[string]any{"TraceIds": []string{"1-abc123"}},
			wantStatus:  http.StatusOK,
			wantTraces:  1,
			wantMissing: 0,
		},
		{
			name:        "returns unprocessed for unknown trace",
			body:        map[string]any{"TraceIds": []string{"1-unknown"}},
			wantStatus:  http.StatusOK,
			wantTraces:  0,
			wantMissing: 1,
		},
		{
			name:       "empty trace IDs",
			body:       map[string]any{"TraceIds": []string{}},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doXrayRequest(t, h, "/Traces", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantTraces > 0 {
				traces, ok := resp["Traces"].([]any)
				require.True(t, ok)
				assert.Len(t, traces, tt.wantTraces)
			}

			if tt.wantMissing > 0 {
				unprocessed, ok := resp["UnprocessedTraceIds"].([]any)
				require.True(t, ok)
				assert.Len(t, unprocessed, tt.wantMissing)
			}
		})
	}
}

func TestBatchGetTraces_SegmentShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-bgt-001", "seg1", "", "svc", now-2, now-1, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/Traces", map[string]any{
		"TraceIds": []string{"1-bgt-001"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	traces, ok := resp["Traces"].([]any)
	require.True(t, ok)
	require.Len(t, traces, 1)

	traceObj, ok := traces[0].(map[string]any)
	require.True(t, ok)

	segs, ok := traceObj["Segments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, segs)

	seg0, ok := segs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, seg0["Id"], "each segment must have an Id")
	assert.NotEmpty(t, seg0["Document"], "each segment must have a Document")
}

func TestBatchGetTraces_5TraceCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ids := []string{"1-cap-001", "1-cap-002", "1-cap-003", "1-cap-004", "1-cap-005", "1-cap-006"}

	rec := doXrayRequest(t, h, "/Traces", map[string]any{"TraceIds": ids})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetTraceSummaries_FaultFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			// Faulted trace
			segJSON("1-filter-fault", "seg1", "", "svc", now-5, now-4, true, false, false),
			// Non-faulted trace
			segJSON("1-filter-ok", "seg2", "", "svc", now-3, now-2, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"FilterExpression": "fault",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1, "only one trace should match the fault filter")

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "1-filter-fault", s["Id"])
	assert.Equal(t, true, s["HasFault"])
}

func TestGetTraceSummaries_TimeRangeType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-trt-001", "seg1", "", "svc", now-5, now-4, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"StartTime":     now - 60,
		"EndTime":       now + 60,
		"TimeRangeType": "TraceId",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	// Should still return the trace — TimeRangeType is accepted without error.
	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, summaries)
}

func TestTraceSummary_FaultAndErrorPropagation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			segJSON("1-prop-fault", "seg1", "", "svc", now-5, now-4, true, false, false),
			segJSON("1-prop-err", "seg2", "", "svc", now-3, now-2, false, true, false),
			segJSON("1-prop-ok", "seg3", "", "svc", now-1, now, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)

	byID := map[string]map[string]any{}

	for _, s := range summaries {
		sm, isSM := s.(map[string]any)
		require.True(t, isSM)
		byID[sm["Id"].(string)] = sm
	}

	require.Contains(t, byID, "1-prop-fault")
	assert.Equal(t, true, byID["1-prop-fault"]["HasFault"])

	require.Contains(t, byID, "1-prop-err")
	assert.Equal(t, true, byID["1-prop-err"]["HasError"])

	require.Contains(t, byID, "1-prop-ok")
	assert.Equal(t, false, byID["1-prop-ok"]["HasFault"])
	assert.Equal(t, false, byID["1-prop-ok"]["HasError"])
}

func TestGetTraceSummaries_ThrottleFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{
			// Throttled trace (error=true AND throttle=true per X-Ray convention)
			segJSON("1-throttle-001", "seg1", "", "svc", now-5, now-4, false, true, true),
			// Non-throttled trace
			segJSON("1-throttle-002", "seg2", "", "svc", now-3, now-2, false, false, false),
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)

	byID := map[string]bool{}
	for _, s := range summaries {
		sm, isSM := s.(map[string]any)
		require.True(t, isSM)

		if throttle, tOK := sm["HasThrottle"].(bool); tOK && throttle {
			byID[sm["Id"].(string)] = true
		}
	}

	assert.Contains(t, byID, "1-throttle-001", "throttled trace should have HasThrottle=true")
}

// TestGetTraceSummariesTimeFilter verifies time window filtering.
func TestGetTraceSummariesTimeFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	// Insert traces at specific times.
	oldTime := time.Unix(1600000000, 0)
	newTime := time.Unix(1700000000, 0)

	b.PutTraceForTest(oldTime)
	b.PutTraceForTest(newTime)

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{
			name:      "no filter returns all traces",
			body:      map[string]any{},
			wantCount: 2,
		},
		{
			name: "filter within window",
			body: map[string]any{
				"StartTime": float64(newTime.Unix() - 10),
				"EndTime":   float64(newTime.Unix() + 10),
			},
			wantCount: 1,
		},
		{
			name: "filter excludes all",
			body: map[string]any{
				"StartTime": float64(newTime.Unix() + 100),
				"EndTime":   float64(newTime.Unix() + 200),
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doXrayRequest(t, h, "/TraceSummaries", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			summaries, ok := resp["TraceSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantCount)
		})
	}
}

// TestGetTraceSummaries_TimeWindowExclusion verifies traces outside the window are excluded.
func TestGetTraceSummaries_TimeWindowExclusion(t *testing.T) {
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

// TestTraceSummary_UsersFromAnnotations verifies Users field is derived from annotation.user.
func TestTraceSummary_UsersFromAnnotations(t *testing.T) {
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

// TestTraceSummary_ForecastStatisticsPresent verifies ForecastStatistics is in response.
func TestTraceSummary_ForecastStatisticsPresent(t *testing.T) {
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

// TestBatchGetTraces_UnprocessedForMissingIDs verifies unknown IDs go to UnprocessedTraceIds.
func TestBatchGetTraces_UnprocessedForMissingIDs(t *testing.T) {
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

func TestHandler_GetTraceSummaries_TimeRangeTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		timeRangeType string
		wantStatus    int
	}{
		{
			name:          "empty TimeRangeType accepted",
			timeRangeType: "",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "TraceId accepted",
			timeRangeType: "TraceId",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "Event accepted",
			timeRangeType: "Event",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "Service accepted",
			timeRangeType: "Service",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "INVALID rejected",
			timeRangeType: "INVALID",
			wantStatus:    http.StatusBadRequest,
		},
		{
			name:          "traceid (lowercase) rejected",
			timeRangeType: "traceid",
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any
			if tt.timeRangeType != "" {
				body = map[string]any{"TimeRangeType": tt.timeRangeType}
			}

			rec := doXrayRequest(t, h, "/TraceSummaries", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetTraceSummaries_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	// Put 5 traces via the segments API
	for i := range 5 {
		docs := []string{
			fmt.Sprintf(`{"trace_id":"1-pag-%d","id":"s%d","name":"svc","start_time":%f}`, i, i, now-float64(i+1)),
		}
		rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": docs})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: 3 results
	rec1 := doXrayRequest(t, h, "/TraceSummaries", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	summaries1, _ := resp1["TraceSummaries"].([]any)
	assert.Len(t, summaries1, 3)
	nextToken, _ := resp1["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Page 2: remaining 2
	rec2 := doXrayRequest(t, h, "/TraceSummaries", map[string]any{"MaxResults": 3, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	summaries2, _ := resp2["TraceSummaries"].([]any)
	assert.Len(t, summaries2, 2)
	assert.Empty(t, resp2["NextToken"])

	// TracesProcessedCount reports the full set count, not the page count
	totalCount, _ := resp1["TracesProcessedCount"].(float64)
	assert.InDelta(t, float64(5), totalCount, 0)
}

func TestGetTraceSummaries_DurationFromSegmentTimes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	seg := fmt.Sprintf(`{"trace_id":"1-dur-001","id":"s1","name":"svc","start_time":%f,"end_time":%f}`, now-2, now)
	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": []string{seg}})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{"StartTime": now - 10, "EndTime": now + 10})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	summaries, _ := resp["TraceSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)

	dur, ok := s["Duration"].(float64)
	require.True(t, ok)
	assert.InDelta(t, 2.0, dur, 0.1, "Duration must equal end_time - start_time")
}

func TestGetTraceSummaries_BooleanFlagFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		segJSON      string
		wantFault    bool
		wantError    bool
		wantThrottle bool
	}{
		{
			name:    "no flags",
			segJSON: `{"trace_id":"1-flags-001","id":"s1","name":"svc","start_time":1700000000.0}`,
		},
		{
			name:      "fault flag",
			segJSON:   `{"trace_id":"1-flags-002","id":"s1","name":"svc","start_time":1700000001.0,"fault":true}`,
			wantFault: true,
		},
		{
			name:      "error flag",
			segJSON:   `{"trace_id":"1-flags-003","id":"s1","name":"svc","start_time":1700000002.0,"error":true}`,
			wantError: true,
		},
		{
			name: "throttle flag (error+throttle)",
			segJSON: `{"trace_id":"1-flags-004","id":"s1","name":"svc",` +
				`"start_time":1700000003.0,"error":true,"throttle":true}`,
			wantError:    true,
			wantThrottle: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			putRec := doXrayRequest(
				t,
				h,
				"/TraceSegments",
				map[string]any{"TraceSegmentDocuments": []string{tt.segJSON}},
			)
			require.Equal(t, http.StatusOK, putRec.Code)

			sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
			require.Equal(t, http.StatusOK, sumRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

			summaries, _ := resp["TraceSummaries"].([]any)
			require.Len(t, summaries, 1)

			s, _ := summaries[0].(map[string]any)
			assert.Equal(t, tt.wantFault, s["HasFault"])
			assert.Equal(t, tt.wantError, s["HasError"])
			assert.Equal(t, tt.wantThrottle, s["HasThrottle"])
		})
	}
}

func TestGetTraceSummaries_HTTPFieldsPopulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		segJSON        string
		wantHTTPMethod string
		wantHTTPURL    string
		wantHTTPStatus float64
	}{
		{
			name: "GET 200",
			segJSON: `{"trace_id":"1-http-001","id":"s1","name":"svc","start_time":1700000000.0,` +
				`"http":{"request":{"method":"GET","url":"https://api.example.com/v1/items"},"response":{"status":200}}}`,
			wantHTTPMethod: "GET",
			wantHTTPURL:    "https://api.example.com/v1/items",
			wantHTTPStatus: 200,
		},
		{
			name: "POST 201",
			segJSON: `{"trace_id":"1-http-002","id":"s1","name":"svc","start_time":1700000001.0,` +
				`"http":{"request":{"method":"POST","url":"https://api.example.com/v1/items"},"response":{"status":201}}}`,
			wantHTTPMethod: "POST",
			wantHTTPURL:    "https://api.example.com/v1/items",
			wantHTTPStatus: 201,
		},
		{
			name: "DELETE 500",
			segJSON: `{"trace_id":"1-http-003","id":"s1","name":"svc","start_time":1700000002.0,"fault":true,` +
				`"http":{"request":{"method":"DELETE","url":"https://api.example.com/v1/items/1"},"response":{"status":500}}}`,
			wantHTTPMethod: "DELETE",
			wantHTTPURL:    "https://api.example.com/v1/items/1",
			wantHTTPStatus: 500,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			putRec := doXrayRequest(
				t,
				h,
				"/TraceSegments",
				map[string]any{"TraceSegmentDocuments": []string{tt.segJSON}},
			)
			require.Equal(t, http.StatusOK, putRec.Code)

			sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
			require.Equal(t, http.StatusOK, sumRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

			summaries, _ := resp["TraceSummaries"].([]any)
			require.Len(t, summaries, 1)

			s, _ := summaries[0].(map[string]any)
			httpField, ok := s["Http"].(map[string]any)
			require.True(t, ok, "Http field must be present")
			assert.Equal(t, tt.wantHTTPMethod, httpField["HttpMethod"])
			assert.Equal(t, tt.wantHTTPURL, httpField["HttpURL"])
			assert.InDelta(t, tt.wantHTTPStatus, httpField["HttpStatus"], 0.001)
		})
	}
}

func TestGetTraceSummaries_ServiceIDsFromSegments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(
			`{"trace_id":"1-svcid-001","id":"r1","name":"frontend","origin":"AWS::EC2","start_time":%f}`,
			now-3,
		),
		fmt.Sprintf(
			`{"trace_id":"1-svcid-001","id":"c1","parent_id":"r1","name":"backend","origin":"AWS::Lambda","start_time":%f}`,
			now-2,
		),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	summaries, _ := resp["TraceSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	svcIDs, ok := s["ServiceIds"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(svcIDs), 1, "ServiceIds must be populated from segment names/origins")
}

func TestGetTraceSummaries_EntryPointFromRootSegment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-entry-001","id":"root","name":"my-service","start_time":%f}`, now-1),
		fmt.Sprintf(
			`{"trace_id":"1-entry-001","id":"child","parent_id":"root","name":"downstream","start_time":%f}`,
			now-0.5,
		),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	summaries, _ := resp["TraceSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	assert.Equal(t, "my-service", s["EntryPoint"], "EntryPoint must be the root segment name")
}

func TestGetTraceSummaries_IsPartialWithoutRootSegment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	// Only a child segment (has parent_id), so trace is partial.
	seg := fmt.Sprintf(
		`{"trace_id":"1-partial-001","id":"child","parent_id":"missing-root","name":"svc","start_time":%f}`,
		now-1,
	)
	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": []string{seg}})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	summaries, _ := resp["TraceSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	assert.Equal(t, true, s["IsPartial"], "IsPartial must be true when root segment is missing")
}

func TestGetTraceSummaries_TracesProcessedCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-count-001","id":"s1","name":"svc","start_time":%f}`, now-3),
		fmt.Sprintf(`{"trace_id":"1-count-002","id":"s2","name":"svc","start_time":%f}`, now-2),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{"StartTime": now - 10, "EndTime": now + 10})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	count, ok := resp["TracesProcessedCount"].(float64)
	require.True(t, ok)
	assert.InDelta(t, 2.0, count, 0.001, "TracesProcessedCount must match number of matched summaries")
}

func TestGetTraceSummaries_TimeRangeTypeAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		timeRangeType string
		wantStatus    int
	}{
		{name: "TraceId accepted", timeRangeType: "TraceId", wantStatus: http.StatusOK},
		{name: "Event accepted", timeRangeType: "Event", wantStatus: http.StatusOK},
		{name: "Service accepted", timeRangeType: "Service", wantStatus: http.StatusOK},
		{name: "empty accepted", timeRangeType: "", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{}
			if tt.timeRangeType != "" {
				body["TimeRangeType"] = tt.timeRangeType
			}

			rec := doXrayRequest(t, h, "/TraceSummaries", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestGetTraceSummaries_FilterExpressions(t *testing.T) {
	t.Parallel()

	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-flt-fault","id":"s1","name":"svc","start_time":%f,"fault":true}`, now-5),
		fmt.Sprintf(`{"trace_id":"1-flt-err","id":"s2","name":"svc","start_time":%f,"error":true}`, now-4),
		fmt.Sprintf(
			`{"trace_id":"1-flt-throttle","id":"s3","name":"svc","start_time":%f,"error":true,"throttle":true}`,
			now-3,
		),
		fmt.Sprintf(`{"trace_id":"1-flt-ok","id":"s4","name":"svc","start_time":%f}`, now-2),
		fmt.Sprintf(
			`{"trace_id":"1-flt-http","id":"s5","name":"svc","start_time":%f,`+
				`"http":{"request":{"method":"GET","url":"https://x.com"},"response":{"status":500}}}`,
			now-1,
		),
	}

	tests := []struct {
		name       string
		filter     string
		wantTraces []string
		wantCount  int
	}{
		{name: "no filter returns all", filter: "", wantCount: 5},
		{name: "fault filter", filter: "fault", wantCount: 1, wantTraces: []string{"1-flt-fault"}},
		// "error" matches HasError=true; both 1-flt-err and 1-flt-throttle have error:true.
		{name: "error filter", filter: "error", wantCount: 2, wantTraces: []string{"1-flt-err", "1-flt-throttle"}},
		{name: "throttle filter", filter: "throttle", wantCount: 1, wantTraces: []string{"1-flt-throttle"}},
		{name: "http.status = 500", filter: "http.status = 500", wantCount: 1, wantTraces: []string{"1-flt-http"}},
		{name: "http.status = 200 no match", filter: "http.status = 200", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
			require.Equal(t, http.StatusOK, putRec.Code)

			body := map[string]any{}
			if tt.filter != "" {
				body["FilterExpression"] = tt.filter
			}

			sumRec := doXrayRequest(t, h, "/TraceSummaries", body)
			require.Equal(t, http.StatusOK, sumRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

			summaries, _ := resp["TraceSummaries"].([]any)
			assert.Len(t, summaries, tt.wantCount, "filter=%q wantCount=%d", tt.filter, tt.wantCount)

			if len(tt.wantTraces) > 0 {
				ids := make([]string, 0, len(summaries))
				for _, s := range summaries {
					sm, _ := s.(map[string]any)
					ids = append(ids, sm["Id"].(string))
				}
				for _, wantID := range tt.wantTraces {
					assert.Contains(t, ids, wantID)
				}
			}
		})
	}
}

func TestGetTraceSummaries_ForecastStatisticsPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	seg := fmt.Sprintf(`{"trace_id":"1-fc-001","id":"s1","name":"svc","start_time":%f}`, now-1)
	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": []string{seg}})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	summaries, _ := resp["TraceSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	// ForecastStatistics must be present (even as empty object) per AWS API.
	_, hasForecast := s["ForecastStatistics"]
	assert.True(t, hasForecast, "ForecastStatistics must be present in TraceSummary")
}

func TestGetTraceSummaries_AnnotationFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(
			`{"trace_id":"1-ann-001","id":"s1","name":"svc","start_time":%f,"annotations":{"env":"prod"}}`,
			now-2,
		),
		fmt.Sprintf(
			`{"trace_id":"1-ann-002","id":"s2","name":"svc","start_time":%f,"annotations":{"env":"staging"}}`,
			now-1,
		),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"FilterExpression": `annotation.env = "prod"`,
	})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	summaries, _ := resp["TraceSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	assert.Equal(t, "1-ann-001", s["Id"])
}

func TestGetTraceSummaries_ResponseTimeFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-rt-001","id":"s1","name":"svc","start_time":%f,"end_time":%f}`, now-5, now-4),
		fmt.Sprintf(`{"trace_id":"1-rt-002","id":"s2","name":"svc","start_time":%f,"end_time":%f}`, now-3, now),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	// The fast trace has duration ~1s; the slow trace has duration ~3s.
	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"FilterExpression": "responsetime > 2",
	})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &resp))

	summaries, _ := resp["TraceSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	assert.Equal(t, "1-rt-002", s["Id"])
}

func TestBatchGetTraces_TraceLimitValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		traceCount int
		wantStatus int
	}{
		{name: "1 trace accepted", traceCount: 1, wantStatus: http.StatusOK},
		{name: "5 traces accepted", traceCount: 5, wantStatus: http.StatusOK},
		{name: "6 traces rejected", traceCount: 6, wantStatus: http.StatusBadRequest},
		{name: "10 traces rejected", traceCount: 10, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ids := make([]string, tt.traceCount)
			for i := range ids {
				ids[i] = fmt.Sprintf("1-cap-%03d", i+1)
			}

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/Traces", map[string]any{"TraceIds": ids})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBatchGetTraces_UnprocessedForMissingTraces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/Traces", map[string]any{
		"TraceIds": []string{"1-missing-001", "1-missing-002"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	traces, _ := resp["Traces"].([]any)
	assert.Empty(t, traces, "missing traces produce no Traces entries")

	unprocessed, _ := resp["UnprocessedTraceIds"].([]any)
	assert.Len(t, unprocessed, 2, "missing trace IDs must appear in UnprocessedTraceIds")
}
