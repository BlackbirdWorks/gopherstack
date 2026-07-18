package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_GetMetricConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Defaults to Enabled per AWS's documented default, not a bare "{}".
	rec := doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	summaryMetric, ok := getResp["SummaryMetric"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Enabled", summaryMetric["Status"])

	// Update must persist, not silently no-op.
	rec = doIoTWRequest(t, h, http.MethodPut, "/metric-configuration",
		`{"SummaryMetric":{"Status":"Disabled"}}`)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	rec = doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
	assert.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	summaryMetric, ok = getResp["SummaryMetric"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Disabled", summaryMetric["Status"])
}

// TestHandler_GetMetrics_MapsQueriesToResults verifies that each requested
// query produces a corresponding result entry, rather than a
// query-count-independent hardcoded empty list.
func TestHandler_GetMetrics_MapsQueriesToResults(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	body := `{"SummaryMetricQueries":[` +
		`{"QueryId":"q1","MetricName":"ConnectionCount"},` +
		`{"QueryId":"q2","MetricName":"UplinkCount"}]}`
	rec := doIoTWRequest(t, h, http.MethodPost, "/metrics", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results, ok := resp["SummaryMetricQueryResults"].([]any)
	require.True(t, ok)
	require.Len(t, results, 2)
	assert.Equal(t, "q1", results[0].(map[string]any)["QueryId"])
	assert.Equal(t, "Succeeded", results[0].(map[string]any)["QueryStatus"])
	assert.Equal(t, "q2", results[1].(map[string]any)["QueryId"])
}

func TestHandler_GetServiceEndpoint_ExactURLsAndValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	// Default (no serviceType query param) must be CUPS.
	rec := doIoTWRequest(t, h, http.MethodGet, "/service-endpoint", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "CUPS", resp["ServiceType"])
	assert.Equal(t, "https://cups.lorawan.us-east-1.amazonaws.com", resp["ServiceEndpoint"])

	// LNS must return a distinct, correctly-shaped endpoint, not the CUPS one.
	rec = doIoTWRequest(t, h, http.MethodGet, "/service-endpoint?serviceType=LNS", "")
	assert.Equal(t, http.StatusOK, rec.Code)

	var lnsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lnsResp))
	assert.Equal(t, "LNS", lnsResp["ServiceType"])
	assert.Equal(t, "https://lns.lorawan.us-east-1.amazonaws.com", lnsResp["ServiceEndpoint"])

	// An invalid serviceType must be rejected, not silently accepted.
	rec = doIoTWRequest(t, h, http.MethodGet, "/service-endpoint?serviceType=BOGUS", "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_MetricConfiguration_UpdateAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "get_metric_config"},
		{name: "update_metric_config"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			switch tt.name {
			case "get_metric_config":
				rec := doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
				assert.Equal(t, http.StatusOK, rec.Code)
			case "update_metric_config":
				rec := doIoTWRequest(t, h, http.MethodPut, "/metric-configuration", `{}`)
				assert.Equal(t, http.StatusNoContent, rec.Code)
			}
		})
	}
}

func TestHandler_GetMetrics_ReturnsEmptyResults(t *testing.T) {
	t.Parallel()

	h := newTestHandlerHTTP()

	rec := doIoTWRequest(t, h, http.MethodPost, "/metrics", `{"SummaryMetricQueries":[]}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["SummaryMetricQueryResults"].([]any)
	require.True(t, ok)
	assert.Empty(t, results)
}

func TestHandler_MetricConfiguration_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		body   string
		wantKV string // top-level key to check in GET response
	}{
		{
			name:   "enable_summary_metric",
			body:   `{"SummaryMetric":{"Status":"Enable"}}`,
			wantKV: "SummaryMetric",
		},
		{
			name:   "disable_summary_metric",
			body:   `{"SummaryMetric":{"Status":"Disable"}}`,
			wantKV: "SummaryMetric",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Update metric configuration (PUT /metric-configuration).
			rec := doIoTWRequest(t, h, http.MethodPut, "/metric-configuration", tt.body)
			require.Equal(t, http.StatusNoContent, rec.Code)

			// Retrieve and verify persisted.
			rec = doIoTWRequest(t, h, http.MethodGet, "/metric-configuration", "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			_, ok := resp[tt.wantKV]
			assert.True(t, ok, "expected key %q in response", tt.wantKV)
		})
	}
}

func TestHandler_GetServiceEndpoint_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		queryParam      string
		wantServiceType string
		wantSubstr      string
	}{
		{
			name:            "cups",
			queryParam:      "?serviceType=CUPS",
			wantServiceType: "CUPS",
			wantSubstr:      "cups.lorawan",
		},
		{
			name:            "lns",
			queryParam:      "?serviceType=LNS",
			wantServiceType: "LNS",
			wantSubstr:      "lns.lorawan",
		},
		{
			name:            "default_no_param",
			queryParam:      "",
			wantServiceType: "CUPS",
			wantSubstr:      "cups.lorawan",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			rec := doIoTWRequest(t, h, http.MethodGet, "/service-endpoint"+tt.queryParam, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantServiceType, resp["ServiceType"])
			endpoint, _ := resp["ServiceEndpoint"].(string)
			assert.Contains(t, endpoint, tt.wantSubstr)
			assert.Contains(t, endpoint, testRegion)
		})
	}
}
