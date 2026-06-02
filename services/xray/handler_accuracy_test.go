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

// segJSON builds a minimal segment JSON string.
func segJSON(traceID, segID, parentID, name string, startTime, endTime float64, fault, hasError, throttle bool) string {
	d := fmt.Sprintf(
		`{"trace_id":%q,"id":%q,"name":%q,"start_time":%f,"end_time":%f,"fault":%v,"error":%v,"throttle":%v`,
		traceID, segID, name, startTime, endTime, fault, hasError, throttle,
	)

	if parentID != "" {
		d += fmt.Sprintf(`,"parent_id":%q`, parentID)
	}

	d += "}"

	return d
}

// segJSONWithHTTP builds a segment with HTTP fields.
func segJSONWithHTTP(traceID, segID, name string, startTime, endTime float64, status int, method, url string) string {
	return fmt.Sprintf(
		`{"trace_id":%q,"id":%q,"name":%q,"start_time":%f,"end_time":%f,`+
			`"http":{"request":{"method":%q,"url":%q},"response":{"status":%d}}}`,
		traceID, segID, name, startTime, endTime, method, url, status,
	)
}

// --- Test 1: PutTraceSegments + GetTraceSummaries round-trip ---

func TestAccuracy_PutAndGetTraceSummaries_Populated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	now := float64(time.Now().Unix())
	seg := segJSONWithHTTP("1-acc-001", "seg1", "my-service", now-1, now, 200, "GET", "https://example.com/api")

	putRec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{seg},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doXrayRequest(t, h, "/TraceSummaries", map[string]any{
		"StartTime": now - 60,
		"EndTime":   now + 60,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	summaries, ok := resp["TraceSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	s, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "1-acc-001", s["Id"])

	// Duration should be non-zero (endTime - startTime = ~1 second).
	dur, ok := s["Duration"].(float64)
	require.True(t, ok)
	assert.Greater(t, dur, 0.0)

	// HasFault / HasError should be false.
	assert.Equal(t, false, s["HasFault"])
	assert.Equal(t, false, s["HasError"])

	// Http fields should be populated.
	httpField, ok := s["Http"].(map[string]any)
	require.True(t, ok, "Http field should be present")
	assert.Equal(t, "GET", httpField["HttpMethod"])
	assert.Equal(t, "https://example.com/api", httpField["HttpURL"])
	assert.InDelta(t, 200.0, httpField["HttpStatus"], 0.001)
}

// --- Test 2: BatchGetTraces correct Segments shape + 5-trace cap ---

func TestAccuracy_BatchGetTraces_SegmentShape(t *testing.T) {
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

func TestAccuracy_BatchGetTraces_5TraceCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ids := []string{"1-cap-001", "1-cap-002", "1-cap-003", "1-cap-004", "1-cap-005", "1-cap-006"}

	rec := doXrayRequest(t, h, "/Traces", map[string]any{"TraceIds": ids})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Test 3: GetServiceGraph returns non-empty nodes after PutTraceSegments ---

func TestAccuracy_GetServiceGraph_NonEmpty(t *testing.T) {
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

// --- Test 4: GetTraceSummaries FilterExpression `fault` filter ---

func TestAccuracy_GetTraceSummaries_FaultFilter(t *testing.T) {
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

// --- Test 5: GetTraceSummaries TimeRangeType ---

func TestAccuracy_GetTraceSummaries_TimeRangeType(t *testing.T) {
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

// --- Test 6: GetInsightSummaries ALL state returns both ACTIVE and CLOSED ---

func TestAccuracy_GetInsightSummaries_ALLState(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddInsightInternal(xray.Insight{InsightID: "all-active", State: "ACTIVE", StartTime: time.Now()})
	b.AddInsightInternal(xray.Insight{InsightID: "all-closed", State: "CLOSED", StartTime: time.Now()})

	rec := doXrayRequest(t, h, "/GetInsightSummaries", map[string]any{
		"States": []any{"ALL"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries, ok := resp["InsightSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 2, "ALL should return both ACTIVE and CLOSED insights")
}

// --- Test 7: GetInsightSummaries unknown state returns error ---

func TestAccuracy_GetInsightSummaries_UnknownState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetInsightSummaries", map[string]any{
		"States": []any{"BOGUS"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Test 8: SamplingRule Priority validation (0 rejected, 10000 rejected) ---

func TestAccuracy_SamplingRule_PriorityValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		priority   int
		wantStatus int
	}{
		{name: "priority 0 rejected", priority: 0, wantStatus: http.StatusBadRequest},
		{name: "priority 1 accepted", priority: 1, wantStatus: http.StatusOK},
		{name: "priority 9999 accepted", priority: 9999, wantStatus: http.StatusOK},
		{name: "priority 10000 rejected", priority: 10000, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  fmt.Sprintf("prio-rule-%d", tt.priority),
					"Priority":  tt.priority,
					"FixedRate": 0.1,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Test 9: SamplingRule FixedRate validation (>1.0 rejected) ---

func TestAccuracy_SamplingRule_FixedRateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		fixedRate  float64
		wantStatus int
	}{
		{name: "fixedRate 0.0 accepted", fixedRate: 0.0, wantStatus: http.StatusOK},
		{name: "fixedRate 1.0 accepted", fixedRate: 1.0, wantStatus: http.StatusOK},
		{name: "fixedRate 1.1 rejected", fixedRate: 1.1, wantStatus: http.StatusBadRequest},
		{name: "fixedRate -0.1 rejected", fixedRate: -0.1, wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/CreateSamplingRule", map[string]any{
				"SamplingRule": map[string]any{
					"RuleName":  fmt.Sprintf("rate-rule-%.2f", tt.fixedRate+10),
					"Priority":  1,
					"FixedRate": tt.fixedRate,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// --- Test 10: GetSamplingTargets returns LastRuleModification and ReservoirQuotaTTL ---

func TestAccuracy_GetSamplingTargets_LastRuleModAndTTL(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "ttl-rule", FixedRate: 0.05, ReservoirSize: 5, Priority: 1})

	rec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
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

// --- Test 11: GetSamplingStatisticSummaries after PutSamplingTargets ---

func TestAccuracy_GetSamplingStatisticSummaries_AfterPut(t *testing.T) {
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

	// Now fetch summaries.
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

// --- Test 12: Encryption KeyId format validation (bad format rejected) ---

func TestAccuracy_Encryption_KeyIdFormatValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		keyID      string
		wantStatus int
	}{
		{
			name:       "alias format accepted",
			keyID:      "alias/my-key",
			wantStatus: http.StatusOK,
		},
		{
			name:       "ARN format accepted",
			keyID:      "arn:aws:kms:us-east-1:123456789012:key/abc-123",
			wantStatus: http.StatusOK,
		},
		{
			name:       "UUID format accepted",
			keyID:      "12345678-1234-1234-1234-123456789abc",
			wantStatus: http.StatusOK,
		},
		{
			name:       "random string rejected",
			keyID:      "not-a-valid-key-id",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty rejected for KMS",
			keyID:      "",
			wantStatus: http.StatusBadRequest,
		},
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

// --- Test 13: Encryption UPDATING status after PUT ---

func TestAccuracy_Encryption_UpdatingStatusAfterPut(t *testing.T) {
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
	assert.Equal(t, "UPDATING", enc["Status"], "status should be UPDATING immediately after PUT")

	// GET should advance to ACTIVE.
	getRec := doXrayGETRequest(t, h)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	enc2, ok := getResp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ACTIVE", enc2["Status"], "status should be ACTIVE after first GET")
}

// --- Test 14: Resource policy max 5 cap ---

func TestAccuracy_ResourcePolicy_MaxFiveCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create 5 policies — should all succeed.
	for i := 1; i <= 5; i++ {
		rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
			"PolicyName":     fmt.Sprintf("policy-%d", i),
			"PolicyDocument": `{"Version":"2012-10-17"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code, "policy %d should succeed", i)
	}

	// 6th should fail.
	rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "policy-6",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "6th policy should be rejected")
}

// --- Test 15: Resource policy revision-ID mismatch ---

func TestAccuracy_ResourcePolicy_RevisionIDMismatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a policy.
	createRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "rev-policy",
		"PolicyDocument": `{"Version":"2012-10-17"}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Update with wrong revision ID — should fail.
	updateRec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":       "rev-policy",
		"PolicyDocument":   `{"Version":"2012-10-17","Statement":[]}`,
		"PolicyRevisionId": "wrong-revision-id",
	})
	assert.Equal(t, http.StatusBadRequest, updateRec.Code)
}

// --- Test 16: Group ARN-based lookup ---

func TestAccuracy_Group_ARNBasedLookup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doXrayRequest(t, h, "/CreateGroup", map[string]any{
		"GroupName": "arn-lookup-group",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	group, ok := createResp["Group"].(map[string]any)
	require.True(t, ok)

	groupARN, ok := group["GroupARN"].(string)
	require.True(t, ok)
	require.NotEmpty(t, groupARN)

	// Get by ARN.
	getRec := doXrayRequest(t, h, "/GetGroup", map[string]any{
		"GroupARN": groupARN,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

	g2, ok := getResp["Group"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "arn-lookup-group", g2["GroupName"])
}

// --- Test 17: PutTelemetryRecords accepted (no error) ---

func TestAccuracy_PutTelemetryRecords_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TelemetryRecords", map[string]any{
		"TelemetryRecords": []map[string]any{
			{
				"Timestamp":              float64(time.Now().Unix()),
				"SegmentsReceivedCount":  100,
				"SegmentsSentCount":      95,
				"SegmentsSpilloverCount": 5,
				"SegmentsRejectedCount":  0,
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	// Response should be an empty JSON object.
	assert.Empty(t, resp)
}

// --- Test 18: GetTimeSeriesServiceStatistics returns bucketed data after PutTraceSegments ---

func TestAccuracy_GetTimeSeriesServiceStatistics_BucketedData(t *testing.T) {
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

// --- Test 19: GetTraceGraph returns nodes for listed trace IDs ---

func TestAccuracy_GetTraceGraph_ReturnsNodes(t *testing.T) {
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

// --- Test 20: UpdateSamplingRule can set FixedRate=0 (zero value applies) ---

func TestAccuracy_UpdateSamplingRule_ZeroFixedRate(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	b.AddSamplingRuleInternal(xray.SamplingRule{RuleName: "zero-rate-rule", FixedRate: 0.5, Priority: 1})

	rec := doXrayRequest(t, h, "/UpdateSamplingRule", map[string]any{
		"SamplingRuleUpdate": map[string]any{
			"RuleName":  "zero-rate-rule",
			"FixedRate": 0.0,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	record, ok := resp["SamplingRuleRecord"].(map[string]any)
	require.True(t, ok)

	rule, ok := record["SamplingRule"].(map[string]any)
	require.True(t, ok)

	fixedRate, ok := rule["FixedRate"].(float64)
	require.True(t, ok)
	assert.InDelta(t, 0.0, fixedRate, 1e-9, "FixedRate should have been updated to 0.0")
}

// --- Bonus test: HasFault and HasError propagation ---

func TestAccuracy_TraceSummary_FaultAndErrorPropagation(t *testing.T) {
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

// --- Bonus test: Group NotificationsEnabled requires InsightsEnabled ---

func TestAccuracy_Group_NotificationsEnabledRequiresInsightsEnabled(t *testing.T) {
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

// --- Bonus test: Resource policy JSON validation ---

func TestAccuracy_ResourcePolicy_JSONValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/PutResourcePolicy", map[string]any{
		"PolicyName":     "bad-json-policy",
		"PolicyDocument": "not-valid-json{",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Bonus test: GetTraceGraph returns empty for unknown trace IDs ---

func TestAccuracy_GetTraceGraph_EmptyForUnknownTraces(t *testing.T) {
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

// --- Bonus test: GetSamplingTargets returns UnprocessedStatistics for unknown rule ---

func TestAccuracy_GetSamplingTargets_UnprocessedWithErrorCode(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/GetSamplingTargets", map[string]any{
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

// --- Bonus test: GetTraceSummaries throttle filter ---

func TestAccuracy_GetTraceSummaries_ThrottleFilter(t *testing.T) {
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
