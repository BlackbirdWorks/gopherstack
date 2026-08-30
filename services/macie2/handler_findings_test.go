package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/macie2"
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

// TestCreateSampleFindingsFieldShape locks the deferred Finding field audit:
// real AWS Finding carries category (CLASSIFICATION/POLICY -- not an
// invented "SENSITIVE_DATA" value), count, partition, sample,
// schemaVersion, and, for a sensitive-data finding,
// classificationDetails/resourcesAffected. Severity.score is an integer
// (real types.Severity.Score is *int64), not an arbitrary float.
func TestCreateSampleFindingsFieldShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/findings/sample", map[string]any{
		"findingTypes": []string{"SensitiveData:S3Object/Personal", "Policy:IAMUser/S3BucketPublic"},
	})

	listRec := doRequest(t, h, http.MethodPost, "/findings", map[string]any{})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	ids, ok := listResp["findingIds"].([]any)
	require.True(t, ok)
	require.Len(t, ids, 2)

	getRec := doRequest(t, h, http.MethodPost, "/findings/describe", map[string]any{"findingIds": ids})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	findings, ok := getResp["findings"].([]any)
	require.True(t, ok)
	require.Len(t, findings, 2)

	byType := make(map[string]map[string]any, 2)

	for _, raw := range findings {
		f, fOk := raw.(map[string]any)
		require.True(t, fOk)
		ft, tOk := f["type"].(string)
		require.True(t, tOk)
		byType[ft] = f

		// Fields common to every finding, regardless of category.
		assert.Equal(t, "aws", f["partition"])
		assert.Equal(t, "1.0", f["schemaVersion"])
		assert.Equal(t, true, f["sample"])
		assert.InDelta(t, float64(1), f["count"], 0.0001)

		severity, sOk := f["severity"].(map[string]any)
		require.True(t, sOk)
		// Real severity score is an integer 1-3, never a value like 5.
		score, scOk := severity["score"].(float64)
		require.True(t, scOk)
		assert.InDelta(t, float64(2), score, 0.0001)
	}

	sensitiveData := byType["SensitiveData:S3Object/Personal"]
	require.NotNil(t, sensitiveData)
	// Real FindingCategory enum is CLASSIFICATION/POLICY -- "SENSITIVE_DATA"
	// is not a valid value.
	assert.Equal(t, "CLASSIFICATION", sensitiveData["category"])

	resourcesAffected, ok := sensitiveData["resourcesAffected"].(map[string]any)
	require.True(t, ok)
	assert.NotNil(t, resourcesAffected["s3Bucket"])
	assert.NotNil(t, resourcesAffected["s3Object"])
	assert.NotNil(t, sensitiveData["classificationDetails"])

	policy := byType["Policy:IAMUser/S3BucketPublic"]
	require.NotNil(t, policy)
	assert.Equal(t, "POLICY", policy["category"])
	policyDetails, pOk := policy["policyDetails"].(map[string]any)
	require.True(t, pOk)
	assert.NotNil(t, policyDetails["action"])
	assert.NotNil(t, policyDetails["actor"])
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

				// PutFindingsPublicationConfiguration. Real
				// PutFindingsPublicationConfigurationInput has no top-level
				// publishClassificationFindings/publishPolicyFindings members
				// (confirmed against aws-sdk-go-v2/service/macie2's
				// api_op_PutFindingsPublicationConfiguration.go) -- only
				// nested under securityHubConfiguration.
				rec = doRequest(t, h, http.MethodPut, "/findings-publication-configuration", map[string]any{
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

				_, hasTopClassification := updated["publishClassificationFindings"]
				_, hasTopPolicy := updated["publishPolicyFindings"]
				assert.False(t, hasTopClassification,
					"publishClassificationFindings must not appear at the top level -- "+
						"GetFindingsPublicationConfigurationOutput has no such member")
				assert.False(t, hasTopPolicy,
					"publishPolicyFindings must not appear at the top level")

				shc, ok := updated["securityHubConfiguration"].(map[string]any)
				require.True(t, ok)
				assert.True(t, shc["publishClassificationFindings"].(bool))
				assert.False(t, shc["publishPolicyFindings"].(bool))
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
