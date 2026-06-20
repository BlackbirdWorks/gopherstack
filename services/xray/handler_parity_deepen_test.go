package xray_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// seedInsight adds an insight to the backend for testing.
func seedInsight(b *xray.InMemoryBackend, id, groupName, groupARN string) {
	b.AddInsightInternal(xray.Insight{
		InsightID: id,
		GroupName: groupName,
		GroupARN:  groupARN,
		State:     "ACTIVE",
		StartTime: time.Now(),
	})
}

// seedInsightEvent adds an event for an insight.
func seedInsightEvent(b *xray.InMemoryBackend, insightID, summary string) {
	b.AddInsightEventInternal(xray.InsightEvent{
		InsightID: insightID,
		Summary:   summary,
		EventTime: time.Now(),
	})
}

// --- GetGroups pagination ---

func TestHandler_GetGroups_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		groupCount  int
		wantCount   int
		wantHasNext bool
		wantStatus  int
	}{
		{
			name:        "no groups returns empty list",
			groupCount:  0,
			wantCount:   0,
			wantStatus:  http.StatusOK,
			wantHasNext: false,
		},
		{
			name:        "returns all groups when under default page size",
			groupCount:  5,
			wantCount:   5,
			wantStatus:  http.StatusOK,
			wantHasNext: false,
		},
		{
			name:        "MaxResults limits results and sets NextToken",
			groupCount:  5,
			body:        map[string]any{"MaxResults": 2},
			wantCount:   2,
			wantStatus:  http.StatusOK,
			wantHasNext: true,
		},
		{
			name:       "zero MaxResults uses default page size",
			groupCount: 3,
			body:       map[string]any{"MaxResults": 0},
			wantCount:  3,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.groupCount {
				_, err := b.CreateGroup(fmt.Sprintf("group-%d", i), "")
				require.NoError(t, err)
			}

			rec := doXrayRequest(t, h, "/Groups", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			groups, ok := resp["Groups"].([]any)
			require.True(t, ok, "Groups field must be present and array")
			assert.Len(t, groups, tt.wantCount)

			nextToken, _ := resp["NextToken"].(string)
			if tt.wantHasNext {
				assert.NotEmpty(t, nextToken, "expected NextToken when paginating")
			} else {
				assert.Empty(t, nextToken, "expected empty NextToken when no more pages")
			}
		})
	}
}

func TestHandler_GetGroups_NextTokenContinuation(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	for i := range 5 {
		_, err := b.CreateGroup(fmt.Sprintf("pg-group-%d", i), "")
		require.NoError(t, err)
	}

	// First page: 3 groups
	rec1 := doXrayRequest(t, h, "/Groups", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))

	groups1 := resp1["Groups"].([]any)
	assert.Len(t, groups1, 3)
	nextToken := resp1["NextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Second page: remaining 2 groups
	rec2 := doXrayRequest(t, h, "/Groups", map[string]any{"MaxResults": 3, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	groups2 := resp2["Groups"].([]any)
	assert.Len(t, groups2, 2)
	assert.Empty(t, resp2["NextToken"])
}

func TestHandler_GetGroups_EmptyBodyAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/Groups", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "Groups")
	assert.Contains(t, resp, "NextToken")
}

// --- GetSamplingRules pagination ---

func TestHandler_GetSamplingRules_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nextToken   string
		extraRules  int
		wantMin     int
		wantHasNext bool
	}{
		{
			name:        "returns default rule with no extra rules",
			extraRules:  0,
			wantMin:     1,
			wantHasNext: false,
		},
		{
			name:        "returns all rules when under page limit",
			extraRules:  3,
			wantMin:     4,
			wantHasNext: false,
		},
		{
			name:        "empty body is accepted",
			extraRules:  0,
			wantMin:     1,
			wantHasNext: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandlerWithBackend(t)

			for i := range tt.extraRules {
				b.AddSamplingRuleInternal(xray.SamplingRule{
					RuleName:      fmt.Sprintf("rule-%d", i),
					ResourceARN:   "*",
					ServiceName:   "*",
					ServiceType:   "*",
					Host:          "*",
					HTTPMethod:    "*",
					URLPath:       "*",
					FixedRate:     0.05,
					Priority:      int32(i + 1),
					ReservoirSize: 1,
				})
			}

			var body map[string]any
			if tt.nextToken != "" {
				body = map[string]any{"NextToken": tt.nextToken}
			}

			rec := doXrayRequest(t, h, "/GetSamplingRules", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			records, ok := resp["SamplingRuleRecords"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(records), tt.wantMin)

			nextToken, _ := resp["NextToken"].(string)
			if tt.wantHasNext {
				assert.NotEmpty(t, nextToken)
			} else {
				assert.Empty(t, nextToken)
			}
		})
	}
}

// --- GetTraceSummaries TimeRangeType validation ---

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

// --- PutTraceSegments validation ---

func TestHandler_PutTraceSegments_CountValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		docCount   int
		wantStatus int
	}{
		{
			name:       "1 segment accepted",
			docCount:   1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "50 segments accepted (exact limit)",
			docCount:   50,
			wantStatus: http.StatusOK,
		},
		{
			name:       "51 segments rejected",
			docCount:   51,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "100 segments rejected",
			docCount:   100,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			docs := make([]string, tt.docCount)
			now := float64(time.Now().Unix())
			for i := range tt.docCount {
				docs[i] = fmt.Sprintf(
					`{"trace_id":"1-cnt-%d","id":"s%d","name":"svc","start_time":%f}`,
					i, i, now,
				)
			}

			rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
				"TraceSegmentDocuments": docs,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutTraceSegments_DocumentSizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		docSize    int
		wantStatus int
	}{
		{
			name:       "small document accepted",
			docSize:    100,
			wantStatus: http.StatusOK,
		},
		{
			name:       "exactly 64KB accepted",
			docSize:    64 * 1024,
			wantStatus: http.StatusOK,
		},
		{
			name:       "one byte over 64KB rejected",
			docSize:    64*1024 + 1,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Build a document padded to the target size.
			base := `{"trace_id":"1-sz-0","id":"s0","name":"svc","start_time":1234.0,"padding":""}`
			padLen := max(0, tt.docSize-len(base))
			doc := fmt.Sprintf(
				`{"trace_id":"1-sz-0","id":"s0","name":"svc","start_time":1234.0,"padding":"%s"}`,
				strings.Repeat("x", padLen),
			)
			// If the doc is still smaller than target, append JSON whitespace.
			for len(doc) < tt.docSize {
				doc += " "
			}

			rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
				"TraceSegmentDocuments": []string{doc},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "doc size=%d", len(doc))
		})
	}
}

func TestHandler_PutTraceSegments_EmptyDocsAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_PutTraceSegments_ExactLimitRespondsWithUnprocessedField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	now := float64(time.Now().Unix())

	docs := make([]string, 50)
	for i := range 50 {
		docs[i] = fmt.Sprintf(
			`{"trace_id":"1-lim-%d","id":"s%d","name":"svc","start_time":%f}`,
			i, i, now,
		)
	}

	rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": docs,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, hasField := resp["UnprocessedTraceSegments"]
	assert.True(t, hasField)
}

// --- PutEncryptionConfig Type validation ---

func TestHandler_PutEncryptionConfig_TypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "NONE type accepted",
			body:       map[string]any{"Type": "NONE"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "KMS type with KeyId accepted",
			body:       map[string]any{"Type": "KMS", "KeyId": "alias/my-key"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty type defaults to NONE",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid type rejected",
			body:       map[string]any{"Type": "INVALID"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "KMS without KeyId rejected",
			body:       map[string]any{"Type": "KMS"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "NONE with KeyId rejected",
			body:       map[string]any{"Type": "NONE", "KeyId": "alias/my-key"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "none (lowercase) rejected",
			body:       map[string]any{"Type": "none"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doXrayRequest(t, h, "/PutEncryptionConfig", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_EncryptionConfig_KMSRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Set KMS encryption
	rec := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{
		"Type":  "KMS",
		"KeyId": "alias/my-xray-key",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Read it back
	rec2 := doXrayRequest(t, h, "/EncryptionConfig", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	cfg, ok := resp["EncryptionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "KMS", cfg["Type"])
	assert.Equal(t, "alias/my-xray-key", cfg["KeyId"])

	// Revert to NONE
	rec3 := doXrayRequest(t, h, "/PutEncryptionConfig", map[string]any{"Type": "NONE"})
	require.Equal(t, http.StatusOK, rec3.Code)

	rec4 := doXrayRequest(t, h, "/EncryptionConfig", nil)
	require.Equal(t, http.StatusOK, rec4.Code)

	var resp4 map[string]any
	require.NoError(t, json.Unmarshal(rec4.Body.Bytes(), &resp4))
	cfg4, _ := resp4["EncryptionConfig"].(map[string]any)
	assert.Equal(t, "NONE", cfg4["Type"])
}

// --- GetInsightEvents pagination ---

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

			rec := doXrayRequest(t, h, "/GetInsightEvents", body)
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
	rec1 := doXrayRequest(t, h, "/GetInsightEvents", map[string]any{
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
	rec2 := doXrayRequest(t, h, "/GetInsightEvents", map[string]any{
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

// --- GetInsightSummaries pagination ---

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

			rec := doXrayRequest(t, h, "/GetInsightSummaries", tt.body)
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

// --- GetIndexingRules pagination ---

func TestHandler_GetIndexingRules_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantMin    int
	}{
		{
			name:       "returns default indexing rules",
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "MaxResults=1 limits results",
			body:       map[string]any{"MaxResults": 1},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "empty body accepted",
			body:       nil,
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
		{
			name:       "NextToken field accepted",
			body:       map[string]any{"NextToken": ""},
			wantStatus: http.StatusOK,
			wantMin:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doXrayRequest(t, h, "/GetIndexingRules", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			rules, _ := resp["IndexingRules"].([]any)
			assert.GreaterOrEqual(t, len(rules), tt.wantMin)
			assert.Contains(t, resp, "NextToken")
		})
	}
}

// --- Insight.EndTime field ---

func TestBackend_Insight_EndTime(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	insight := xray.Insight{
		InsightID: "end-time-test",
		GroupName: "my-group",
		GroupARN:  "arn:aws:xray:us-east-1:123456789012:group/default/my-group",
		State:     "ACTIVE",
		StartTime: time.Now(),
	}
	b.AddInsightInternal(insight)

	fetched, err := b.GetInsight("end-time-test")
	require.NoError(t, err)
	assert.True(t, fetched.EndTime.IsZero(), "EndTime should be zero when not set")
	assert.NotZero(t, fetched.StartTime)
}

func TestBackend_Insight_EndTimeSet(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	endTime := time.Now().Add(time.Hour)
	insight := xray.Insight{
		InsightID: "end-time-set-test",
		GroupName: "my-group",
		GroupARN:  "arn:aws:xray:us-east-1:123:group/default/my-group",
		State:     "CLOSED",
		StartTime: time.Now(),
		EndTime:   endTime,
	}
	b.AddInsightInternal(insight)

	fetched, err := b.GetInsight("end-time-set-test")
	require.NoError(t, err)
	assert.False(t, fetched.EndTime.IsZero())
	assert.Equal(t, endTime.Unix(), fetched.EndTime.Unix())
}

func TestHandler_GetInsight_ResponseShape(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	seedInsight(b, "shape-test-id", "my-group", "arn:aws:xray:us-east-1:123456789012:group/default/my-group")

	rec := doXrayRequest(t, h, "/GetInsight", map[string]any{"InsightId": "shape-test-id"})
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

// --- GetInsightSummaries response shape ---

func TestHandler_GetInsightSummaries_ResponseShape(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	seedInsight(b, "summary-shape-id", "my-group", "arn:aws:xray:us-east-1:123456789012:group/default/my-group")

	rec := doXrayRequest(t, h, "/GetInsightSummaries", nil)
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
}

// --- GetSamplingStatisticSummaries pagination ---

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
			rec := doXrayRequest(t, h, "/GetSamplingStatisticSummaries", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, resp, "SamplingStatisticSummaries")
			assert.Contains(t, resp, "NextToken")
		})
	}
}

// --- GetServiceGraph pagination ---

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

// --- GetTraceGraph pagination ---

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

// --- GetTimeSeriesServiceStatistics pagination ---

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

// --- ListTagsForResource pagination ---

func TestHandler_ListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantTags   int
	}{
		{
			name:       "missing ResourceARN rejected",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "resource with no tags returns empty list",
			body:       map[string]any{"ResourceARN": "arn:aws:xray:us-east-1:123:group/default/g1"},
			wantStatus: http.StatusOK,
			wantTags:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/ListTagsForResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags, _ := resp["Tags"].([]any)
				assert.Len(t, tags, tt.wantTags)
				assert.Contains(t, resp, "NextToken")
			}
		})
	}
}

func TestHandler_ListTagsForResource_WithTags(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	arn := "arn:aws:xray:us-east-1:123456789012:group/default/tagged"
	tags := map[string]string{
		"env":     "prod",
		"team":    "platform",
		"version": "v1",
		"owner":   "alice",
		"cost":    "high",
	}
	b.TagResource(arn, tags)

	rec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tagsList, _ := resp["Tags"].([]any)
	assert.Len(t, tagsList, len(tags))

	// Each tag should be a map with Key and Value
	for _, tagAny := range tagsList {
		tagMap, ok := tagAny.(map[string]any)
		require.True(t, ok)
		assert.Contains(t, tagMap, "Key")
		assert.Contains(t, tagMap, "Value")
	}
}
