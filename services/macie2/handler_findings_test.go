package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMacie2_Findings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *macie2.Handler)
		check    func(t *testing.T, body []byte)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "CreateSampleFindings returns 200",
			method: http.MethodPost,
			path:   "/findings/sample",
			body: map[string]any{
				"findingTypes": []string{"SensitiveData:S3Object/Personal"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListFindings returns findingIds key",
			method: http.MethodPost,
			path:   "/findings",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"Policy:IAMUser/S3BucketPublic"},
				})
			},
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "findingIds")

				ids, ok := resp["findingIds"].([]any)
				require.True(t, ok)
				assert.Len(t, ids, 1)
			},
		},
		{
			name:   "GetFindings returns findings array",
			method: http.MethodPost,
			path:   "/findings/describe",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"SensitiveData:S3Object/Financial"},
				})
			},
			body:     nil, // will be set in test body below
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "findings")
			},
		},
		{
			name:   "GetFindingStatistics returns countsByGroup",
			method: http.MethodPost,
			path:   "/findings/statistics",
			setup: func(h *macie2.Handler) {
				doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
					"findingTypes": []string{"SensitiveData:S3Object/Credentials"},
				})
			},
			body:     map[string]any{"groupBy": "type"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp, "countsByGroup")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			// For GetFindings, get actual finding IDs first
			body := tt.body
			if tt.name == "GetFindings returns findings array" {
				listRec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{})
				var listResp map[string]any
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
				ids := listResp["findingIds"].([]any)
				body = map[string]any{"findingIds": ids}
			}

			rec := doRequest(t, h, tt.method, tt.path, body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestListFindingsEmptyNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["findingIds"]
	require.True(t, ok, "response must contain findingIds key")
	assert.NotNil(t, v, "findingIds must be [] not null when empty")

	arr, isArr := v.([]any)
	require.True(t, isArr, "findingIds must be an array")
	assert.Empty(t, arr)
}

func TestGetFindingsEmptyInputNotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/findings/describe",
		map[string]any{"findingIds": []string{}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	v, ok := resp["findings"]
	require.True(t, ok, "response must contain findings key")
	assert.NotNil(t, v, "findings must be [] not null for empty findingIds input")
}

func TestFindingsPublicationConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_put_publication_config",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				// GetFindingsPublicationConfiguration — defaults to zero value
				rec := doRequest(t, h, http.MethodGet, "/findings-publication-configuration", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// PutFindingsPublicationConfiguration
				rec = doRequest(t, h, http.MethodPut, "/findings-publication-configuration", map[string]any{
					"publishClassificationFindings": true,
					"publishPolicyFindings":         true,
					"securityHubConfiguration": map[string]any{
						"publishClassificationFindings": true,
						"publishPolicyFindings":         false,
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/findings-publication-configuration", nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.True(t, updated["publishClassificationFindings"].(bool))
				assert.True(t, updated["publishPolicyFindings"].(bool))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestHandler(t))
		})
	}
}
