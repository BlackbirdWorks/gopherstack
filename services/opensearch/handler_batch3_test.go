package opensearch_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// ---------------------------------------------------------------------------
// DataSourceType JSON serialisation (issue #17)
// ---------------------------------------------------------------------------

// TestBatch3_AddDataSource_DataSourceTypeJSON verifies that a structured
// DataSourceType sent as a JSON object is stored as JSON (not a Go map dump).
func TestBatch3_AddDataSource_DataSourceTypeJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createTestDomain(t, h, "ds-json-domain")

	body := map[string]any{
		"Name":        "my-ds",
		"Description": "test",
		"DataSourceType": map[string]any{
			"S3GlueDataCatalog": map[string]any{
				"RoleArn": "arn:aws:iam::123456789012:role/MyRole",
			},
		},
	}

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/ds-json-domain/dataSource", body)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Retrieve and verify that the DataSourceType is valid JSON, not a Go map dump.
	getResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/domain/ds-json-domain/dataSource/my-ds", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))

	ds, ok := out["DataSource"].(map[string]any)
	require.True(t, ok, "expected DataSource key in response")

	rawType, hasType := ds["dataSourceType"]
	require.True(t, hasType, "DataSourceType must be present")

	rawStr, ok := rawType.(string)
	require.True(t, ok, "DataSourceType must be a string in storage")

	// Must be parseable JSON, not a Go map representation.
	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(rawStr), &parsed),
		"DataSourceType must be stored as JSON, got: %s", rawStr)

	assert.Contains(t, parsed, "S3GlueDataCatalog")
}

// TestBatch3_AddDirectQueryDataSource_DataSourceTypeJSON verifies that
// DataSourceType is stored as JSON for direct query data sources.
func TestBatch3_AddDirectQueryDataSource_DataSourceTypeJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	body := map[string]any{
		"DataSourceName": "dq-ds",
		"Description":    "direct query test",
		"DataSourceType": map[string]any{
			"CloudWatchLog": map[string]any{},
		},
		"OpenSearchArns": []string{"arn:aws:es:us-east-1:123456789012:domain/my-domain"},
	}

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/directQueryDataSource", body)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// Get and verify.
	getResp := doRequest(t, h, http.MethodGet,
		"/2021-01-01/opensearch/directQueryDataSource/dq-ds", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))

	rawType, hasType := out["dataSourceType"]
	require.True(t, hasType, "dataSourceType must be present in response")

	rawStr, ok := rawType.(string)
	require.True(t, ok, "dataSourceType must be a string in storage")

	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(rawStr), &parsed),
		"dataSourceType must be stored as JSON, got: %s", rawStr)

	assert.Contains(t, parsed, "CloudWatchLog")
}

// ---------------------------------------------------------------------------
// StartServiceSoftwareUpdate ScheduleAt decoding (issue #28)
// ---------------------------------------------------------------------------

// TestBatch3_StartServiceSoftwareUpdate_ScheduleAt verifies that the handler
// correctly decodes ScheduleAt from the request body and the description in the
// response reflects the chosen schedule mode.
func TestBatch3_StartServiceSoftwareUpdate_ScheduleAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		scheduleAt  string
		wantDescSub string
	}{
		{
			scheduleAt:  "NOW",
			wantDescSub: "ready to install",
		},
		{
			scheduleAt:  "OFF_PEAK_WINDOW",
			wantDescSub: "off-peak window",
		},
		{
			scheduleAt:  "TIMESTAMP",
			wantDescSub: "requested time",
		},
		{
			scheduleAt:  "",
			wantDescSub: "ready to install",
		},
	}

	for _, tt := range tests {
		t.Run("scheduleAt_"+tt.scheduleAt, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			b := h.Backend.(*opensearch.InMemoryBackend)
			b.AddDomainInternal("sw-domain", "OpenSearch_2.11")

			body := map[string]any{
				"DomainName": "sw-domain",
				"ScheduleAt": tt.scheduleAt,
			}

			resp := doRequest(t, h, http.MethodPost,
				"/2021-01-01/opensearch/domain/sw-domain/serviceSoftwareUpdate", body)
			defer resp.Body.Close()

			require.Equal(t, http.StatusOK, resp.StatusCode)

			var out map[string]any
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

			swo, ok := out["ServiceSoftwareOptions"].(map[string]any)
			require.True(t, ok, "expected ServiceSoftwareOptions in response")
			assert.Equal(t, "PENDING_UPDATE", swo["UpdateStatus"])

			desc, _ := swo["Description"].(string)
			assert.Contains(t, desc, tt.wantDescSub,
				"description %q should contain %q for ScheduleAt=%q",
				desc, tt.wantDescSub, tt.scheduleAt)
		})
	}
}

// TestBatch3_StartServiceSoftwareUpdate_DomainNotFound verifies 404 when the
// domain does not exist.
func TestBatch3_StartServiceSoftwareUpdate_DomainNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost,
		"/2021-01-01/opensearch/domain/no-such/serviceSoftwareUpdate",
		map[string]any{"DomainName": "no-such", "ScheduleAt": "NOW"})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
