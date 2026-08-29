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

// TestListRetrievedTraces_IncludesSegments verifies that ListRetrievedTraces
// returns trace views with non-empty Segments when segment data exists.
func TestListRetrievedTraces_IncludesSegments(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	h := xray.NewHandler(b)

	const traceID = "1-seg-trace-000000000000001"

	// Put a segment into the backend.
	seg := map[string]any{
		"trace_id":   traceID,
		"id":         "seg-abc123",
		"name":       "my-service",
		"start_time": 1700000000.0,
		"end_time":   1700000001.5,
	}
	segJSON, err := json.Marshal(seg)
	require.NoError(t, err)

	unprocessed := b.PutTraceSegments([]string{string(segJSON)})
	assert.Empty(t, unprocessed)

	// Start retrieval and list.
	startResp := doXrayRequest(t, h, "/StartTraceRetrieval", map[string]any{
		"TraceIds":  []string{traceID},
		"StartTime": 1699999999.0,
		"EndTime":   1700000100.0,
	})
	require.Equal(t, 200, startResp.Code)

	var startResult map[string]any
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &startResult))

	token, ok := startResult["RetrievalToken"].(string)
	require.True(t, ok, "expected RetrievalToken in response")

	listResp := doXrayRequest(t, h, "/ListRetrievedTraces", map[string]any{"RetrievalToken": token})
	require.Equal(t, 200, listResp.Code)

	var listResult map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listResult))

	traces, ok := listResult["Traces"].([]any)
	require.True(t, ok, "expected Traces array in response")
	require.NotEmpty(t, traces, "expected at least one trace")

	firstTrace, ok := traces[0].(map[string]any)
	require.True(t, ok)

	// The real RetrievedTrace shape's field is "Spans" (types.Span{Document,Id}), not
	// "Segments" -- a real SDK client's deserializer only recognizes "Spans".
	spans, ok := firstTrace["Spans"].([]any)
	require.True(t, ok, "expected Spans field")
	assert.NotEmpty(t, spans, "expected non-empty Spans in trace view")

	// Duration should be computed from segment timing.
	duration, ok := firstTrace["Duration"].(float64)
	assert.True(t, ok)
	assert.Greater(t, duration, 0.0, "expected non-zero Duration")
}

// startTestRetrieval creates a real trace retrieval via StartTraceRetrieval and
// returns its token, so Cancel/List/GetGraph tests exercise a token the backend
// actually recognizes.
func startTestRetrieval(t *testing.T, h *xray.Handler) string {
	t.Helper()

	rec := doXrayRequest(t, h, "/StartTraceRetrieval", map[string]any{
		"TraceIds":  []string{"1-real-000000000001"},
		"StartTime": float64(time.Now().Add(-time.Hour).Unix()),
		"EndTime":   float64(time.Now().Add(time.Hour).Unix()),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	token, ok := resp["RetrievalToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, token)

	return token
}

func TestHandler_CancelTraceRetrieval(t *testing.T) {
	t.Parallel()

	t.Run("cancels a real retrieval token", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		token := startTestRetrieval(t, h)

		rec := doXrayRequest(t, h, "/CancelTraceRetrieval", map[string]any{"RetrievalToken": token})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("missing RetrievalToken returns 400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doXrayRequest(t, h, "/CancelTraceRetrieval", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown token returns 400 ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()

		// CancelTraceRetrieval declares ResourceNotFoundException for a token that
		// was never created by StartTraceRetrieval -- not a silent idempotent no-op.
		h := newTestHandler(t)
		rec := doXrayRequest(t, h, "/CancelTraceRetrieval", map[string]any{"RetrievalToken": "does-not-exist"})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var resp map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "ResourceNotFoundException", resp["__type"])
	})
}

func TestHandler_GetRetrievedTracesGraph(t *testing.T) {
	t.Parallel()

	t.Run("returns status for a real retrieval token", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		token := startTestRetrieval(t, h)

		rec := doXrayRequest(t, h, "/GetRetrievedTracesGraph", map[string]any{"RetrievalToken": token})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		assert.Equal(t, "COMPLETE", resp["RetrievalStatus"])

		services, ok := resp["Services"].([]any)
		require.True(t, ok)
		assert.Empty(t, services)
	})

	t.Run("missing RetrievalToken returns 400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doXrayRequest(t, h, "/GetRetrievedTracesGraph", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown token returns 400 ResourceNotFoundException", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doXrayRequest(t, h, "/GetRetrievedTracesGraph", map[string]any{"RetrievalToken": "unknown-token"})
		require.Equal(t, http.StatusBadRequest, rec.Code)

		var resp map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "ResourceNotFoundException", resp["__type"])
	})
}

func TestTraceRetrieval_StartAndList(t *testing.T) {
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

// TestTraceRetrieval_CancelThenCancelAgainReturnsNotFound verifies a second cancel of
// the same token fails: unlike some AWS "delete" APIs, CancelTraceRetrieval is not
// idempotent -- the modeled ResourceNotFoundException applies once the token is gone.
func TestTraceRetrieval_CancelThenCancelAgainReturnsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	token := startTestRetrieval(t, h)

	firstRec := doXrayRequest(t, h, "/CancelTraceRetrieval", map[string]any{"RetrievalToken": token})
	require.Equal(t, http.StatusOK, firstRec.Code)

	secondRec := doXrayRequest(t, h, "/CancelTraceRetrieval", map[string]any{"RetrievalToken": token})
	assert.Equal(t, http.StatusBadRequest, secondRec.Code)
}

func TestTraceRetrieval_CancelEmptyTokenRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doXrayRequest(t, h, "/CancelTraceRetrieval", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRetrievedTracesGraph_NotFoundForUnknownToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "unknown token returns 400 ResourceNotFoundException",
			body:       map[string]any{"RetrievalToken": "unknown"},
			wantStatus: http.StatusBadRequest,
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
		})
	}
}
