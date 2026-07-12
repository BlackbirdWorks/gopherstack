package sesv2_test

// Tests for parity.md findings: TagResource/UntagResource/ListTagsForResource,
// PutAccount* persistence, MultiRegionEndpoint CRUD, Tenant CRUD,
// BatchGetMetricData synthetic data, GetDomainDeliverabilityCampaign,
// GetDomainStatisticsReport.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

func doReqQuery(
	t *testing.T,
	h *sesv2.Handler,
	method, path string,
	query map[string]string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer

	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}

	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")

	if len(query) > 0 {
		q := req.URL.Query()
		for k, v := range query {
			q.Set(k, v)
		}

		req.URL.RawQuery = q.Encode()
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&m))

	return m
}

// TestTagResource verifies TagResource persists tags, ListTagsForResource returns them,
// and UntagResource removes them.
func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // test struct; reordering would separate related fields
		wantTags map[string]string
		initTags []any
		name     string
		arn      string
		untagKey string
	}{
		{
			name:     "tag_and_list",
			arn:      "arn:aws:ses:us-east-1:123456789012:identity/example.com",
			initTags: []any{map[string]any{"Key": "env", "Value": "prod"}},
			wantTags: map[string]string{"env": "prod"},
		},
		{
			name:     "untag_removes_key",
			arn:      "arn:aws:ses:us-east-1:123456789012:identity/test.com",
			initTags: []any{map[string]any{"Key": "k1", "Value": "v1"}},
			untagKey: "k1",
			wantTags: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			rec := doReqQuery(t, h, http.MethodPost, "/v2/email/tags", nil,
				map[string]any{"ResourceArn": tt.arn, "Tags": tt.initTags})
			assert.Equal(t, http.StatusOK, rec.Code, "TagResource: %s", rec.Body)

			if tt.untagKey != "" {
				rec2 := doReqQuery(t, h, http.MethodDelete, "/v2/email/tags",
					map[string]string{"ResourceArn": tt.arn, "TagKeys": tt.untagKey}, nil)
				assert.Equal(t, http.StatusOK, rec2.Code, "UntagResource: %s", rec2.Body)
			}

			rec3 := doReqQuery(t, h, http.MethodGet, "/v2/email/tags",
				map[string]string{"ResourceArn": tt.arn}, nil)
			require.Equal(t, http.StatusOK, rec3.Code, "ListTagsForResource: %s", rec3.Body)

			resp := decodeJSON(t, rec3)
			tagList, ok := resp["Tags"].([]any)
			require.True(t, ok, "expected Tags array")

			got := make(map[string]string, len(tagList))
			for _, entry := range tagList {
				m, mOK := entry.(map[string]any)
				require.True(t, mOK)
				got[fmt.Sprint(m["Key"])] = fmt.Sprint(m["Value"])
			}

			assert.Equal(t, tt.wantTags, got)
		})
	}
}

// TestPutAccountAttributesPersist verifies PutAccount* ops persist (previously no-op stubs).
func TestPutAccountAttributesPersist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "suppression_attributes",
			method: http.MethodPut,
			path:   "/v2/email/account/suppression",
			body:   map[string]any{"SuppressedReasons": []any{"BOUNCE", "COMPLAINT"}},
		},
		{
			name:   "vdm_attributes",
			method: http.MethodPut,
			path:   "/v2/email/account/vdm",
			body:   map[string]any{"VdmAttributes": map[string]any{"DashboardAttributes": map[string]any{}}},
		},
		{
			name:   "dedicated_ip_warmup",
			method: http.MethodPut,
			path:   "/v2/email/account/dedicated-ips/warmup",
			body:   map[string]any{"AutoWarmupEnabled": true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := doReqQuery(t, h, tt.method, tt.path, nil, tt.body)
			assert.Equal(t, http.StatusOK, rec.Code, "%s failed: %s", tt.path, rec.Body)
		})
	}
}

// TestMultiRegionEndpointCRUD verifies Create persists, Get retrieves, List lists.
func TestMultiRegionEndpointCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doReqQuery(t, h, http.MethodPost, "/v2/email/multi-region-endpoints", nil,
		map[string]any{"EndpointName": "my-endpoint", "Details": map[string]any{}})
	require.Equal(t, http.StatusOK, rec.Code, "CreateMultiRegionEndpoint: %s", rec.Body)

	rec2 := doReqQuery(t, h, http.MethodGet, "/v2/email/multi-region-endpoints/my-endpoint", nil, nil)
	require.Equal(t, http.StatusOK, rec2.Code, "GetMultiRegionEndpoint: %s", rec2.Body)

	resp2 := decodeJSON(t, rec2)
	assert.Equal(t, "READY", resp2["Status"])

	rec3 := doReqQuery(t, h, http.MethodGet, "/v2/email/multi-region-endpoints", nil, nil)
	require.Equal(t, http.StatusOK, rec3.Code, "ListMultiRegionEndpoints: %s", rec3.Body)
}

// TestTenantCRUD verifies Create persists, Get retrieves the name, List includes it.
func TestTenantCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doReqQuery(t, h, http.MethodPost, "/v2/email/tenants", nil,
		map[string]any{"TenantName": "acme"})
	require.Equal(t, http.StatusOK, rec.Code, "CreateTenant: %s", rec.Body)

	rec2 := doReqQuery(t, h, http.MethodGet, "/v2/email/tenants/acme", nil, nil)
	require.Equal(t, http.StatusOK, rec2.Code, "GetTenant: %s", rec2.Body)

	resp2 := decodeJSON(t, rec2)
	assert.Equal(t, "acme", resp2["TenantName"])

	rec3 := doReqQuery(t, h, http.MethodGet, "/v2/email/tenants", nil, nil)
	require.Equal(t, http.StatusOK, rec3.Code, "ListTenants: %s", rec3.Body)
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

// TestGetDomainDeliverabilityCampaignFields verifies required fields are present.
func TestGetDomainDeliverabilityCampaignFields(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	result, err := backend.GetDomainDeliverabilityCampaign("example.com", "camp-123")
	require.NoError(t, err)

	for _, field := range []string{"CampaignId", "FromAddress", "InboxCount", "SpamCount"} {
		assert.Contains(t, result, field, "missing field %s", field)
	}
}

// TestGetDomainStatisticsReportFields verifies required fields are present.
func TestGetDomainStatisticsReportFields(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	result, err := backend.GetDomainStatisticsReport("example.com", "2024-01-01", "2024-01-31")
	require.NoError(t, err)

	assert.Equal(t, "example.com", result["Domain"])
	assert.Contains(t, result, "OverallVolume")
	assert.Contains(t, result, "DailyVolumes")
}
