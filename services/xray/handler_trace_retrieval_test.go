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
	startResp := doXrayRequest(t, h, "/StartTraceRetrieval", map[string]any{"TraceIds": []string{traceID}})
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

	segments, ok := firstTrace["Segments"].([]any)
	require.True(t, ok, "expected Segments field")
	assert.NotEmpty(t, segments, "expected non-empty Segments in trace view")

	// Duration should be computed from segment timing.
	duration, ok := firstTrace["Duration"].(float64)
	assert.True(t, ok)
	assert.Greater(t, duration, 0.0, "expected non-zero Duration")
}

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

func TestTraceRetrieval_CancelIsIdempotent(t *testing.T) {
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

func TestRetrievedTracesGraph_CompleteForUnknownToken(t *testing.T) {
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
