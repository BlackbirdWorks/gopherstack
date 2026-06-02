package xray_test

// AWS-accuracy audit batch-1 tests (go-0ll4).
//
// Covers all 18 gaps from issue #1856:
//   1.  PutTraceSegments full segment parsing and indexing
//   2.  GetTraceSummaries all documented fields
//   3.  GetTraceSummaries TimeRangeType / FilterExpression / ForecastStatistics
//   4.  BatchGetTraces {Id,Document} segment shape and 5-trace cap
//   5.  GetServiceGraph real service-graph computation
//   6.  GetTimeSeriesServiceStatistics per-period bucketing
//   7.  GetTraceGraph per-trace scoped graph
//   8.  Insights seeding and state model
//   9.  GetInsightSummaries States validation (ALL, ACTIVE, CLOSED, unknown)
//  10.  Insight view field completeness
//  11.  SamplingRule field validation (Priority, FixedRate, ReservoirSize, RuleName)
//  12.  UpdateSamplingRule pointer-semantic zero-value application
//  13.  GetSamplingTargets LastRuleModification and ReservoirQuotaTTL
//  14.  GetSamplingStatisticSummaries per-rule accumulation
//  15.  EncryptionConfig KMS KeyId format and UPDATING→ACTIVE status
//  16.  ResourcePolicy 5-policy cap and revision-ID conflict
//  17.  Groups ARN-based lookup, FilterExpression, NotificationsEnabled guard
//  18.  PutTelemetryRecords full field ingestion (no-op but accepted)

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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 1: PutTraceSegments — full segment parsing and indexing
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_PutTraceSegments_ParsesSegmentFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seg        string
		wantParsed bool
	}{
		{
			name:       "minimal segment with trace_id and id",
			seg:        `{"trace_id":"1-audit-001","id":"seg1","name":"svc","start_time":1700000000.0}`,
			wantParsed: true,
		},
		{
			name:       "segment with fault flag",
			seg:        `{"trace_id":"1-audit-002","id":"seg2","name":"svc","start_time":1700000001.0,"fault":true}`,
			wantParsed: true,
		},
		{
			name: "segment with error and throttle flags",
			seg: `{"trace_id":"1-audit-003","id":"seg3","name":"svc","start_time":1700000002.0,` +
				`"error":true,"throttle":true}`,
			wantParsed: true,
		},
		{
			name: "segment with http request and response",
			seg: `{"trace_id":"1-audit-004","id":"seg4","name":"svc","start_time":1700000003.0,` +
				`"http":{"request":{"method":"GET","url":"https://example.com/api"},"response":{"status":200}}}`,
			wantParsed: true,
		},
		{
			name: "segment with annotations",
			seg: `{"trace_id":"1-audit-005","id":"seg5","name":"svc",` +
				`"start_time":1700000004.0,"annotations":{"user":"alice","tier":"free"}}`,
			wantParsed: true,
		},
		{
			name:       "segment with parent_id (subsegment)",
			seg:        `{"trace_id":"1-audit-006","id":"seg6","parent_id":"root1","name":"child","start_time":1700000005.0}`,
			wantParsed: true,
		},
		{
			name:       "malformed JSON rejected",
			seg:        `{not valid json`,
			wantParsed: false,
		},
		{
			name:       "missing trace_id rejected",
			seg:        `{"id":"seg8","name":"svc","start_time":1700000007.0}`,
			wantParsed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
				"TraceSegmentDocuments": []string{tt.seg},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			unprocessed, _ := resp["UnprocessedTraceSegments"].([]any)
			if tt.wantParsed {
				assert.Empty(t, unprocessed, "segment should be accepted without error")
			} else {
				assert.NotEmpty(t, unprocessed, "invalid segment should appear in unprocessed list")
			}
		})
	}
}

func TestAudit_PutTraceSegments_IndexesSegmentsForDownstreamAPIs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(
			`{"trace_id":"1-idx-001","id":"root","name":"frontend","start_time":%f,"end_time":%f}`,
			now-3,
			now-2,
		),
		fmt.Sprintf(
			`{"trace_id":"1-idx-001","id":"child","parent_id":"root","name":"backend","start_time":%f,"end_time":%f}`,
			now-2,
			now-1,
		),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	// GetTraceSummaries should find the trace.
	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{"StartTime": now - 10, "EndTime": now + 10})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var sumResp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &sumResp))

	summaries, ok := sumResp["TraceSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1, "indexed trace must appear in GetTraceSummaries")

	// BatchGetTraces should find the trace.
	batchRec := doXrayRequest(t, h, "/Traces", map[string]any{"TraceIds": []string{"1-idx-001"}})
	require.Equal(t, http.StatusOK, batchRec.Code)

	var batchResp map[string]any
	require.NoError(t, json.Unmarshal(batchRec.Body.Bytes(), &batchResp))

	traces, ok := batchResp["Traces"].([]any)
	require.True(t, ok)
	assert.Len(t, traces, 1, "indexed trace must appear in BatchGetTraces")
}

func TestAudit_PutTraceSegments_MultipleTracesIndexedSeparately(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	segs := []string{
		fmt.Sprintf(`{"trace_id":"1-multi-001","id":"s1","name":"svc","start_time":%f}`, now-5),
		fmt.Sprintf(`{"trace_id":"1-multi-002","id":"s2","name":"svc","start_time":%f}`, now-4),
		fmt.Sprintf(`{"trace_id":"1-multi-003","id":"s3","name":"svc","start_time":%f}`, now-3),
	}

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": segs})
	require.Equal(t, http.StatusOK, putRec.Code)

	sumRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{"StartTime": now - 10, "EndTime": now + 10})
	require.Equal(t, http.StatusOK, sumRec.Code)

	var sumResp map[string]any
	require.NoError(t, json.Unmarshal(sumRec.Body.Bytes(), &sumResp))

	summaries, _ := sumResp["TraceSummaries"].([]any)
	assert.Len(t, summaries, 3, "three separate traces must be indexed separately")
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 2: GetTraceSummaries — all documented fields
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetTraceSummaries_DurationFromSegmentTimes(t *testing.T) {
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

func TestAudit_GetTraceSummaries_BooleanFlagFields(t *testing.T) {
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

func TestAudit_GetTraceSummaries_HTTPFieldsPopulated(t *testing.T) {
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

func TestAudit_GetTraceSummaries_ServiceIDsFromSegments(t *testing.T) {
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

func TestAudit_GetTraceSummaries_EntryPointFromRootSegment(t *testing.T) {
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

func TestAudit_GetTraceSummaries_IsPartialWithoutRootSegment(t *testing.T) {
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

func TestAudit_GetTraceSummaries_TracesProcessedCount(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 3: GetTraceSummaries — TimeRangeType, FilterExpression, ForecastStatistics
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetTraceSummaries_TimeRangeTypeAccepted(t *testing.T) {
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

func TestAudit_GetTraceSummaries_FilterExpressions(t *testing.T) {
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

func TestAudit_GetTraceSummaries_ForecastStatisticsPresent(t *testing.T) {
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

func TestAudit_GetTraceSummaries_AnnotationFilter(t *testing.T) {
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

func TestAudit_GetTraceSummaries_ResponseTimeFilter(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 4: BatchGetTraces — {Id,Document} shape and 5-trace cap
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_BatchGetTraces_SegmentShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	seg := fmt.Sprintf(`{"trace_id":"1-bgt-001","id":"seg1","name":"svc","start_time":%f,"end_time":%f}`, now-2, now-1)
	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": []string{seg}})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/Traces", map[string]any{"TraceIds": []string{"1-bgt-001"}})
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
	require.NotEmpty(t, segs, "Segments must be non-empty for a known trace")

	seg0, ok := segs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, seg0["Id"], "each segment must have an Id field")
	assert.NotEmpty(t, seg0["Document"], "each segment must have a Document field")
}

func TestAudit_BatchGetTraces_TraceLimitValidation(t *testing.T) {
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

func TestAudit_BatchGetTraces_UnprocessedForMissingTraces(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 5: GetServiceGraph — real service-graph computation
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetServiceGraph_NodesAfterSegments(t *testing.T) {
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

func TestAudit_GetServiceGraph_NodeFields(t *testing.T) {
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

func TestAudit_GetServiceGraph_RequiresStartAndEndTime(t *testing.T) {
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

func TestAudit_GetServiceGraph_EmptyOutsideTimeWindow(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 6: GetTimeSeriesServiceStatistics — per-period bucketing
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetTimeSeriesServiceStatistics_BucketsByPeriod(t *testing.T) {
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

func TestAudit_GetTimeSeriesServiceStatistics_ReturnsContainsOldGroupVersions(t *testing.T) {
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

func TestAudit_GetTimeSeriesServiceStatistics_BucketedStatsAfterSegments(t *testing.T) {
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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 7: GetTraceGraph — per-trace scoped graph
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetTraceGraph_ReturnsNodesForTraceIDs(t *testing.T) {
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

func TestAudit_GetTraceGraph_EmptyForUnknownTrace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceGraph", map[string]any{"TraceIds": []string{"1-no-such-trace"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	services, _ := resp["Services"].([]any)
	assert.Empty(t, services)
}

func TestAudit_GetTraceGraph_RequiresTraceIds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceGraph", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gaps 8 & 10: Insights — seeding and field completeness
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetInsight_FieldsReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		insight     xray.Insight
		name        string
		wantState   string
		wantSummary string
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

			rec := doXrayRequest(t, h, "/GetInsight", map[string]any{"InsightId": tt.insight.InsightID})
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

func TestAudit_GetInsight_NotFoundReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetInsight", map[string]any{"InsightId": "no-such-insight"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_GetInsight_MissingInsightIdReturns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetInsight", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_GetInsightEvents_ReturnsEventsForInsight(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddInsightInternal(xray.Insight{InsightID: "i-evt-001", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightEventInternal(xray.InsightEvent{InsightID: "i-evt-001", Summary: "Spike began", EventTime: time.Now()})
	b.AddInsightEventInternal(
		xray.InsightEvent{InsightID: "i-evt-001", Summary: "Spike worsened", EventTime: time.Now()},
	)

	rec := doXrayRequest(t, h, "/GetInsightEvents", map[string]any{"InsightId": "i-evt-001"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	events, ok := resp["InsightEvents"].([]any)
	require.True(t, ok)
	assert.Len(t, events, 2, "both events must be returned")
}

func TestAudit_GetInsightImpactGraph_ReturnsInsightId(t *testing.T) {
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

			rec := doXrayRequest(t, h, "/GetInsightImpactGraph", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "i-impact-001", resp["InsightId"])
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 9: GetInsightSummaries — States validation
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetInsightSummaries_StatesValidation(t *testing.T) {
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

			rec := doXrayRequest(t, h, "/GetInsightSummaries", body)
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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 11: SamplingRule validation
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_SamplingRule_PriorityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		priority   int
		wantStatus int
	}{
		{name: "priority 0 rejected", priority: 0, wantStatus: http.StatusBadRequest},
		{name: "priority 1 accepted", priority: 1, wantStatus: http.StatusOK},
		{name: "priority 100 accepted", priority: 100, wantStatus: http.StatusOK},
		{name: "priority 9999 accepted", priority: 9999, wantStatus: http.StatusOK},
		{name: "priority 10000 rejected", priority: 10000, wantStatus: http.StatusBadRequest},
		{name: "priority negative rejected", priority: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  fmt.Sprintf("rule-prio-%d", tt.priority+10000),
					"Priority":  tt.priority,
					"FixedRate": 0.05,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_SamplingRule_FixedRateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fixedRate  float64
		wantStatus int
	}{
		{name: "0.0 accepted", fixedRate: 0.0, wantStatus: http.StatusOK},
		{name: "0.5 accepted", fixedRate: 0.5, wantStatus: http.StatusOK},
		{name: "1.0 accepted", fixedRate: 1.0, wantStatus: http.StatusOK},
		{name: "1.001 rejected", fixedRate: 1.001, wantStatus: http.StatusBadRequest},
		{name: "-0.001 rejected", fixedRate: -0.001, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  fmt.Sprintf("rule-rate-%.3f", tt.fixedRate+10),
					"Priority":  100,
					"FixedRate": tt.fixedRate,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_SamplingRule_ReservoirSizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		reservoirSize int
		wantStatus    int
	}{
		{name: "0 accepted", reservoirSize: 0, wantStatus: http.StatusOK},
		{name: "100 accepted", reservoirSize: 100, wantStatus: http.StatusOK},
		{name: "-1 rejected", reservoirSize: -1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":      fmt.Sprintf("rule-res-%d", tt.reservoirSize+1000),
					"Priority":      100,
					"FixedRate":     0.05,
					"ReservoirSize": tt.reservoirSize,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_SamplingRule_RuleNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ruleName   string
		wantStatus int
	}{
		{name: "1 char accepted", ruleName: "x", wantStatus: http.StatusOK},
		{name: "32 chars accepted", ruleName: "a23456789012345678901234567890ab", wantStatus: http.StatusOK},
		{name: "33 chars rejected", ruleName: "a234567890123456789012345678901bc", wantStatus: http.StatusBadRequest},
		{name: "empty rejected", ruleName: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  tt.ruleName,
					"Priority":  100,
					"FixedRate": 0.05,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_SamplingRule_DefaultRulePresentAtStart(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetSamplingRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	records, ok := resp["SamplingRuleRecords"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, records)

	var foundDefault bool
	for _, r := range records {
		rec, _ := r.(map[string]any)
		rule, _ := rec["SamplingRule"].(map[string]any)
		if rule["RuleName"] == "Default" {
			foundDefault = true

			break
		}
	}

	assert.True(t, foundDefault, "Default sampling rule must always be present")
}

func TestAudit_SamplingRule_DefaultRuleUndeletable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/DeleteSamplingRule", map[string]any{"RuleName": "Default"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit_SamplingRule_DuplicateRuleNameRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"SamplingRule": map[string]any{
			"RuleName":  "dup-rule",
			"Priority":  100,
			"FixedRate": 0.05,
		},
	}

	rec1 := doXrayRequest(t, h, "/CreateSamplingRule", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doXrayRequest(t, h, "/CreateSamplingRule", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 12: UpdateSamplingRule — pointer-semantic zero-value application
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_UpdateSamplingRule_ZeroValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update        map[string]any
		name          string
		wantFixRate   float64
		wantReservoir float64
	}{
		{
			name:          "set FixedRate to 0.0",
			update:        map[string]any{"RuleName": "zero-test", "FixedRate": 0.0},
			wantFixRate:   0.0,
			wantReservoir: 10, // unchanged
		},
		{
			name:          "set ReservoirSize to 0",
			update:        map[string]any{"RuleName": "zero-test", "ReservoirSize": 0},
			wantFixRate:   0.5, // unchanged
			wantReservoir: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)
			b.AddSamplingRuleInternal(
				xray.SamplingRule{RuleName: "zero-test", FixedRate: 0.5, ReservoirSize: 10, Priority: 100},
			)

			rec := doXrayRequest(t, h, "/UpdateSamplingRule", map[string]any{"SamplingRuleUpdate": tt.update})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			record, ok := resp["SamplingRuleRecord"].(map[string]any)
			require.True(t, ok)

			rule, ok := record["SamplingRule"].(map[string]any)
			require.True(t, ok)

			if _, hasFixedRate := tt.update["FixedRate"]; hasFixedRate {
				assert.InDelta(t, tt.wantFixRate, rule["FixedRate"], 1e-9)
			}
			if _, hasReservoir := tt.update["ReservoirSize"]; hasReservoir {
				assert.InDelta(t, tt.wantReservoir, rule["ReservoirSize"], 0.001)
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 13: GetSamplingTargets — LastRuleModification and ReservoirQuotaTTL
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetSamplingTargets_LastRuleModification(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "mod-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	rec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
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

func TestAudit_GetSamplingTargets_ReservoirQuotaTTL(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "ttl-rule", FixedRate: 0.1, ReservoirSize: 10, Priority: 1})

	rec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
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

func TestAudit_GetSamplingTargets_UnprocessedForUnknownRules(t *testing.T) {
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

			rec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
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

// ─────────────────────────────────────────────────────────────────────────────
// Gap 14: GetSamplingStatisticSummaries — accumulation from GetSamplingTargets
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_GetSamplingStatisticSummaries_AccumulationFromTargets(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "stat-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	// Submit statistics via GetSamplingTargets.
	putRec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
		"SamplingStatisticsDocuments": []map[string]any{
			{"RuleName": "stat-rule", "ClientId": "c-1", "RequestCount": 200, "SampledCount": 10, "BorrowCount": 2},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	rec := doXrayRequest(t, h, "/GetSamplingStatisticSummaries", nil)
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

func TestAudit_GetSamplingStatisticSummaries_EmptyWithoutStats(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetSamplingStatisticSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["SamplingStatisticSummaries"].([]any)
	require.True(t, ok)
	assert.Empty(t, summaries)
}

func TestAudit_GetSamplingStatisticSummaries_AccumulatesAcrossCalls(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "accum-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	for range 3 {
		putRec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
			"SamplingStatisticsDocuments": []map[string]any{
				{"RuleName": "accum-rule", "ClientId": "c-1", "RequestCount": 100, "SampledCount": 5, "BorrowCount": 0},
			},
		})
		require.Equal(t, http.StatusOK, putRec.Code)
	}

	rec := doXrayRequest(t, h, "/GetSamplingStatisticSummaries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, _ := resp["SamplingStatisticSummaries"].([]any)
	require.Len(t, summaries, 1)

	s, _ := summaries[0].(map[string]any)
	assert.InDelta(t, 300.0, s["RequestCount"], 0.001, "RequestCount must accumulate across calls")
	assert.InDelta(t, 15.0, s["SampledCount"], 0.001, "SampledCount must accumulate across calls")
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 15: EncryptionConfig — KMS KeyId format and UPDATING→ACTIVE status
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_EncryptionConfig_KMSKeyIdFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		keyID      string
		wantStatus int
	}{
		{name: "alias format", keyID: "alias/my-key", wantStatus: http.StatusOK},
		{name: "key ARN format", keyID: "arn:aws:kms:us-east-1:123456789012:key/abc-123", wantStatus: http.StatusOK},
		{name: "UUID format", keyID: "12345678-1234-1234-1234-123456789abc", wantStatus: http.StatusOK},
		{name: "random string rejected", keyID: "not-a-valid-key", wantStatus: http.StatusBadRequest},
		{name: "empty rejected", keyID: "", wantStatus: http.StatusBadRequest},
		{name: "partial alias rejected", keyID: "alias", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Type": "KMS"}
			if tt.keyID != "" {
				body["KeyId"] = tt.keyID
			}

			rec := doXrayRequest(t, h, "/PutEncryptionConfig", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_EncryptionConfig_UpdatingThenActiveStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	putRec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{
		"Type":  "KMS",
		"KeyId": "alias/my-key",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putResp))

	enc, ok := putResp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UPDATING", enc["Status"], "PUT must return UPDATING for KMS")

	getRec := doXrayGETRequest(t, h)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	enc2, ok := getResp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ACTIVE", enc2["Status"], "first GET after KMS PUT must return ACTIVE")
}

func TestAudit_EncryptionConfig_NoneTypeAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{"Type": "NONE"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	enc, ok := resp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	// NONE type should be ACTIVE immediately.
	assert.Equal(t, "ACTIVE", enc["Status"])
}

func TestAudit_EncryptionConfig_InvalidTypeRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{"Type": "BOGUS"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 16: ResourcePolicy — 5-policy cap, revision-ID conflict, JSON validation
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_ResourcePolicy_FivePolicyCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 5; i++ {
		rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
			"PolicyName":     fmt.Sprintf("policy-%d", i),
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code, "policy %d must succeed", i)
	}

	rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "policy-6",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "6th policy must be rejected")
}

func TestAudit_ResourcePolicy_RevisionIDConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		revisionID string
		wantStatus int
	}{
		{
			name:       "no revision ID always succeeds",
			revisionID: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong revision ID rejected",
			revisionID: "wrong-revision-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
				"PolicyName":     "rev-test-policy",
				"PolicyDocument": `{"Version":"2012-10-17"}`,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			body := map[string]any{
				"PolicyName":     "rev-test-policy",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			}
			if tt.revisionID != "" {
				body["PolicyRevisionId"] = tt.revisionID
			}

			rec := doXrayRequest(t, h, "/PutResourcePolicy", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_ResourcePolicy_JSONValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		policyDocument string
		wantStatus     int
	}{
		{name: "valid JSON object accepted", policyDocument: `{"Version":"2012-10-17"}`, wantStatus: http.StatusOK},
		{name: "valid JSON array accepted", policyDocument: `[]`, wantStatus: http.StatusOK},
		{name: "malformed JSON rejected", policyDocument: `{not valid json`, wantStatus: http.StatusBadRequest},
		{name: "empty string rejected", policyDocument: ``, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
				"PolicyName":     "json-test-policy",
				"PolicyDocument": tt.policyDocument,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_ResourcePolicy_ListAfterPut(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 3; i++ {
		rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
			"PolicyName":     fmt.Sprintf("list-policy-%d", i),
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	policies, ok := resp["ResourcePolicies"].([]any)
	require.True(t, ok)
	assert.Len(t, policies, 3)
}

func TestAudit_ResourcePolicy_DeleteExistingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "del-policy",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	delRec := doXrayRequest(t, h, "/DeleteResourcePolicy", map[string]any{"PolicyName": "del-policy"})
	require.Equal(t, http.StatusOK, delRec.Code)

	listRec := doXrayRequest(t, h, "/ListResourcePolicies", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	policies, _ := resp["ResourcePolicies"].([]any)
	assert.Empty(t, policies, "policy must be removed after delete")
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 17: Groups — ARN-based lookup, FilterExpression, NotificationsEnabled guard
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_Groups_ARNBasedLookup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "arn-group"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, ok := createResp["Group"].(map[string]any)
	require.True(t, ok)

	groupARN, ok := group["GroupARN"].(string)
	require.True(t, ok)
	require.NotEmpty(t, groupARN)

	getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{"GroupARN": groupARN})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	g2, ok := getResp["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn-group", g2["GroupName"])
}

func TestAudit_Groups_UpdateByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "upd-arn-group"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, _ := createResp["Group"].(map[string]any)
	groupARN, _ := group["GroupARN"].(string)

	updRec := doXrayRequest(t, h, "/UpdateGroup", map[string]any{
		"GroupARN":         groupARN,
		"FilterExpression": "fault",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	var updResp map[string]any
	require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updResp))

	g2, ok := updResp["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "fault", g2["FilterExpression"])
}

func TestAudit_Groups_DeleteByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "del-arn-group"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, _ := createResp["Group"].(map[string]any)
	groupARN, _ := group["GroupARN"].(string)

	delRec := doXrayRequest(t, h, "/DeleteGroup", map[string]any{"GroupARN": groupARN})
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{"GroupARN": groupARN})
	assert.Equal(t, http.StatusBadRequest, getRec.Code, "deleted group must not be found")
}

func TestAudit_Groups_NotificationsEnabledRequiresInsightsEnabled(t *testing.T) {
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

func TestAudit_Groups_FilterExpressionStoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		filterExpression string
	}{
		{name: "fault filter", filterExpression: "fault"},
		{name: "error filter", filterExpression: "error"},
		{name: "response time filter", filterExpression: "responsetime > 1"},
		{name: "empty filter", filterExpression: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			groupName := fmt.Sprintf("flt-grp-%s", tt.name)

			createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
				"GroupName":        groupName,
				"FilterExpression": tt.filterExpression,
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{"GroupName": groupName})
			require.Equal(t, http.StatusOK, getRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

			g, ok := resp["Group"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.filterExpression, g["FilterExpression"])
		})
	}
}

func TestAudit_Groups_ARNPresentInCreateResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{"GroupName": "arn-check-group"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	group, ok := resp["Group"].(map[string]any)
	require.True(t, ok)

	arn, ok := group["GroupARN"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:xray:", "GroupARN must be a proper ARN")
}

func TestAudit_Groups_ListGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := 1; i <= 3; i++ {
		rec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
			"GroupName": fmt.Sprintf("list-group-%d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	listRec := doXrayRequest(t, h, "/Groups", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	groups, ok := resp["Groups"].([]any)
	require.True(t, ok)
	assert.Len(t, groups, 3)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gap 18: PutTelemetryRecords — full field ingestion
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_PutTelemetryRecords_AllFieldsAccepted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		records []map[string]any
	}{
		{
			name: "single record all fields",
			records: []map[string]any{
				{
					"Timestamp":              float64(time.Now().Unix()),
					"SegmentsReceivedCount":  100,
					"SegmentsSentCount":      95,
					"SegmentsSpilloverCount": 3,
					"SegmentsRejectedCount":  2,
				},
			},
		},
		{
			name: "multiple records",
			records: []map[string]any{
				{
					"Timestamp":             float64(time.Now().Unix() - 60),
					"SegmentsReceivedCount": 50,
					"SegmentsSentCount":     50,
				},
				{
					"Timestamp":             float64(time.Now().Unix()),
					"SegmentsReceivedCount": 75,
					"SegmentsSentCount":     70,
				},
			},
		},
		{
			name:    "empty records list accepted",
			records: []map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/TelemetryRecords", map[string]any{
				"TelemetryRecords": tt.records,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Empty(t, resp, "response must be an empty JSON object")
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional coverage: Tags, IndexingRules, TraceRetrieval
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_Tags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "single tag",
			tags: map[string]string{"env": "prod"},
		},
		{
			name: "multiple tags",
			tags: map[string]string{"env": "prod", "team": "platform", "cost-center": "eng"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := "arn:aws:xray:us-east-1:123456789012:group/default/test-group"

			tagRec := doXrayRequest(t, h, "/TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tt.tags,
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, len(tt.tags))
		})
	}
}

func TestAudit_Tags_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:xray:us-east-1:123456789012:group/default/untag-group"

	tagRec := doXrayRequest(t, h, "/TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        map[string]string{"key1": "val1", "key2": "val2"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	untagRec := doXrayRequest(t, h, "/UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"key1"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	tags, _ := resp["Tags"].([]any)
	assert.Len(t, tags, 1, "only key2 should remain after untagging key1")
}

func TestAudit_IndexingRules_GetDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetIndexingRules", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rules, ok := resp["IndexingRules"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, rules, "at least the Default indexing rule must be present")
}

func TestAudit_IndexingRules_UpdateModifiesTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/UpdateIndexingRule", map[string]any{"Name": "Default"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	rule, ok := resp["IndexingRule"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Default", rule["Name"])
	assert.NotNil(t, rule["ModifiedAt"])
}

func TestAudit_TraceRetrieval_StartAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	seg := fmt.Sprintf(`{"trace_id":"1-ret-001","id":"s1","name":"svc","start_time":%f}`, now-1)
	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{"TraceSegmentDocuments": []string{seg}})
	require.Equal(t, http.StatusOK, putRec.Code)

	startRec := doXrayRequest(t, h, "/StartTraceRetrieval", map[string]any{
		"TraceIds":  []string{"1-ret-001"},
		"StartTime": now - 10,
		"EndTime":   now + 10,
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))

	token, ok := startResp["RetrievalToken"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, token)

	listRec := doXrayRequest(t, h, "/ListRetrievedTraces", map[string]any{
		"RetrievalToken": token,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	assert.NotEmpty(t, listResp["RetrievalStatus"])
}

func TestAudit_TraceRetrieval_CancelIsIdempotent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		retrievalToken string
		wantStatus     int
	}{
		{name: "cancel known token", retrievalToken: "some-token-123", wantStatus: http.StatusOK},
		{name: "cancel unknown token is idempotent", retrievalToken: "not-a-real-token", wantStatus: http.StatusOK},
		{name: "cancel empty token rejected", retrievalToken: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{}
			if tt.retrievalToken != "" {
				body["RetrievalToken"] = tt.retrievalToken
			}

			rec := doXrayRequest(t, h, "/CancelTraceRetrieval", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit_TraceSegmentDestination_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		destination string
		wantStatus  int
	}{
		{name: "XRay destination", destination: "XRay", wantStatus: http.StatusOK},
		{name: "CloudWatchLogs destination", destination: "CloudWatchLogs", wantStatus: http.StatusOK},
		{name: "empty rejected", destination: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.destination != "" {
				updRec := doXrayRequest(t, h, "/UpdateTraceSegmentDestination", map[string]any{
					"Destination": tt.destination,
				})
				assert.Equal(t, tt.wantStatus, updRec.Code)
			} else {
				updRec := doXrayRequest(t, h, "/UpdateTraceSegmentDestination", map[string]any{})
				assert.Equal(t, tt.wantStatus, updRec.Code)
			}
		})
	}
}

func TestAudit_RetrievedTracesGraph_CompleteForUnknownToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		wantRetrievalSt string
		wantStatus      int
	}{
		{
			name:            "unknown token returns COMPLETE",
			body:            map[string]any{"RetrievalToken": "unknown"},
			wantStatus:      http.StatusOK,
			wantRetrievalSt: "COMPLETE",
		},
		{
			name:       "missing token rejected",
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

			if tt.wantRetrievalSt != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantRetrievalSt, resp["RetrievalStatus"])
			}
		})
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Additional: EvaluateFilter unit tests
// ─────────────────────────────────────────────────────────────────────────────

func TestAudit_EvaluateFilter_AllExpressionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		summary xray.TraceSummaryData
		want    bool
	}{
		{
			name:    "empty expr always true",
			expr:    "",
			summary: xray.TraceSummaryData{},
			want:    true,
		},
		{
			name:    "fault matches HasFault=true",
			expr:    "fault",
			summary: xray.TraceSummaryData{HasFault: true},
			want:    true,
		},
		{
			name:    "fault does not match HasFault=false",
			expr:    "fault",
			summary: xray.TraceSummaryData{HasFault: false},
			want:    false,
		},
		{
			name:    "error matches HasError=true",
			expr:    "error",
			summary: xray.TraceSummaryData{HasError: true},
			want:    true,
		},
		{
			name:    "throttle matches HasThrottle=true",
			expr:    "throttle",
			summary: xray.TraceSummaryData{HasThrottle: true},
			want:    true,
		},
		{
			name:    "responsetime > 1 matches 2s response",
			expr:    "responsetime > 1",
			summary: xray.TraceSummaryData{ResponseTime: 2.0},
			want:    true,
		},
		{
			name:    "responsetime > 1 does not match 0.5s response",
			expr:    "responsetime > 1",
			summary: xray.TraceSummaryData{ResponseTime: 0.5},
			want:    false,
		},
		{
			name:    "responsetime >= 1 matches exactly 1s",
			expr:    "responsetime >= 1",
			summary: xray.TraceSummaryData{ResponseTime: 1.0},
			want:    true,
		},
		{
			name:    "responsetime < 2 matches 1s",
			expr:    "responsetime < 2",
			summary: xray.TraceSummaryData{ResponseTime: 1.0},
			want:    true,
		},
		{
			name: "http.status = 200 matches",
			expr: "http.status = 200",
			summary: xray.TraceSummaryData{
				HTTP: &xray.TraceSummaryHTTP{HTTPStatus: 200},
			},
			want: true,
		},
		{
			name: "http.status = 500 does not match 200",
			expr: "http.status = 500",
			summary: xray.TraceSummaryData{
				HTTP: &xray.TraceSummaryHTTP{HTTPStatus: 200},
			},
			want: false,
		},
		{
			name:    "annotation filter matches",
			expr:    `annotation.env = "prod"`,
			summary: xray.TraceSummaryData{Annotations: map[string]any{"env": "prod"}},
			want:    true,
		},
		{
			name:    "annotation filter no match",
			expr:    `annotation.env = "prod"`,
			summary: xray.TraceSummaryData{Annotations: map[string]any{"env": "staging"}},
			want:    false,
		},
		{
			name:    "unknown filter returns false",
			expr:    "unknown_filter_expression",
			summary: xray.TraceSummaryData{},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := xray.EvaluateFilter(tt.expr, tt.summary)
			assert.Equal(t, tt.want, got)
		})
	}
}
