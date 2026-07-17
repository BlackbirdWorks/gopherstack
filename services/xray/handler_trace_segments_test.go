package xray_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutTraceSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid segment",
			body: map[string]any{
				"TraceSegmentDocuments": []string{`{"trace_id":"1-abc","id":"s1","name":"test"}`},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty segments",
			body:       map[string]any{"TraceSegmentDocuments": []string{}},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/TraceSegments", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_PutTraceSegments_Unprocessed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayRequest(t, h, "/TraceSegments", map[string]any{
		"TraceSegmentDocuments": []string{"not-valid-json"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	unprocessed, ok := resp["UnprocessedTraceSegments"].([]any)
	require.True(t, ok)
	assert.Len(t, unprocessed, 1)
}

func TestPutAndGetTraceSummaries_Populated(t *testing.T) {
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

// TestPutTraceSegments_MalformedJSON verifies malformed JSON doesn't crash.
func TestPutTraceSegments_MalformedJSON(t *testing.T) {
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

// TestPutTraceSegments_MissingTraceID verifies missing trace_id → unprocessed.
func TestPutTraceSegments_MissingTraceID(t *testing.T) {
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

func TestPutTraceSegments_ParsesSegmentFields(t *testing.T) {
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

func TestPutTraceSegments_IndexesSegmentsForDownstreamAPIs(t *testing.T) {
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

func TestPutTraceSegments_MultipleTracesIndexedSeparately(t *testing.T) {
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
