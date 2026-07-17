package sesv2_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestBatchGetMetricData tests the BatchGetMetricData operation.
func TestBatchGetMetricData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queries   []sesv2.MetricDataQuery
		wantCount int
		wantErr   bool
	}{
		{
			name:      "single_query",
			queries:   []sesv2.MetricDataQuery{{ID: "q1", Namespace: "VDM", Metric: "SEND"}},
			wantCount: 1,
		},
		{
			name:      "multiple_queries",
			queries:   []sesv2.MetricDataQuery{{ID: "q1"}, {ID: "q2"}, {ID: "q3"}},
			wantCount: 3,
		},
		{
			name:      "empty_queries",
			queries:   []sesv2.MetricDataQuery{},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			results, err := backend.BatchGetMetricData(tt.queries)

			require.NoError(t, err)
			assert.Len(t, results, tt.wantCount)

			for i, r := range results {
				assert.Equal(t, tt.queries[i].ID, r.ID)
				assert.NotEmpty(t, r.Timestamps, "BatchGetMetricData should return synthetic data")
				assert.NotEmpty(t, r.Values, "BatchGetMetricData should return synthetic data")
			}
		})
	}
}

// TestBatchGetMetricDataHTTP tests BatchGetMetricData via HTTP.
func TestBatchGetMetricDataHTTP(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{
		"Queries": []map[string]any{
			{"Id": "q1", "Namespace": "VDM", "Metric": "SEND"},
		},
	}
	rec := doReq(t, h, http.MethodPost, "/v2/email/metrics/batch", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestBatchGetMetricDataHTTPBadJSON tests bad JSON for BatchGetMetricData.
func TestBatchGetMetricDataHTTPBadJSON(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)

	req := httptest.NewRequest(
		http.MethodPost,
		"/v2/email/metrics/batch",
		bytes.NewReader([]byte("not-json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestGetMessageInsights tests the GetMessageInsights operation.
func TestGetMessageInsights(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/messages/msg-123", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestBatchGetMetricDataSynthetic verifies BatchGetMetricData returns synthetic data
// (previously returned empty arrays).
func TestBatchGetMetricDataSynthetic(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	queries := []sesv2.MetricDataQuery{
		{ID: "q1", Namespace: "ses.send", Metric: "Send"},
		{ID: "q2", Namespace: "ses.send", Metric: "Bounce"},
	}

	results, err := backend.BatchGetMetricData(queries)
	require.NoError(t, err)
	require.Len(t, results, 2)

	for _, r := range results {
		assert.NotEmpty(t, r.Timestamps, "query %s: expected synthetic timestamps", r.ID)
		assert.NotEmpty(t, r.Values, "query %s: expected synthetic values", r.ID)
	}
}
