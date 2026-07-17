package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindingsReportLifecycle(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	// Create
	rec := auditDo(t, h, http.MethodPost, "/reporting/create", map[string]any{
		"reportFormat":  "CSV",
		"s3Destination": map[string]any{"bucketName": "my-bucket", "keyPrefix": "reports/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	reportID, _ := createResp["reportId"].(string)
	require.NotEmpty(t, reportID)

	// Get status
	rec = auditDo(t, h, http.MethodPost, "/reporting/status/get", map[string]any{
		"reportId": reportID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &statusResp))
	assert.Equal(t, reportID, statusResp["reportId"])
	assert.Equal(t, "SUCCEEDED", statusResp["status"])

	// Cancel
	rec = auditDo(t, h, http.MethodPost, "/reporting/cancel", map[string]any{
		"reportId": reportID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Cancel unknown report returns 404
	rec = auditDo(t, h, http.MethodPost, "/reporting/cancel", map[string]any{
		"reportId": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFindingAggregations(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/findings/aggregation/list", map[string]any{
		"aggregationType": "ACCOUNT",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["responses"]
	assert.True(t, ok)
}

func TestSearchVulnerabilities(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/vulnerabilities/search", map[string]any{
		"filterCriteria": map[string]any{
			"vulnerabilityIds": []string{"CVE-2023-1234"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["vulnerabilities"]
	assert.True(t, ok)
}

func TestBatchGetCodeSnippetAndFindingDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		check  func(t *testing.T, code int, body []byte)
		name   string
		method string
		path   string
	}{
		{
			name:   "BatchGetCodeSnippet returns results and errors",
			method: http.MethodPost,
			path:   "/codesnippet/batchget",
			body:   map[string]any{"findingArns": []string{"arn:aws:inspector2:us-east-1:123456789012:finding/abc"}},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["codeSnippetResults"]
				assert.True(t, ok)
				_, ok = resp["errors"]
				assert.True(t, ok)
			},
		},
		{
			name:   "BatchGetFindingDetails returns results and errors",
			method: http.MethodPost,
			path:   "/findings/details/batch/get",
			body: map[string]any{
				"findingArns": []any{
					map[string]any{"findingArn": "arn:aws:inspector2:us-east-1:123456789012:finding/abc"},
				},
			},
			check: func(t *testing.T, code int, body []byte) {
				t.Helper()
				assert.Equal(t, http.StatusOK, code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				_, ok := resp["findingDetails"]
				assert.True(t, ok)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newAuditHandler(t)
			rec := auditDo(t, h, tc.method, tc.path, tc.body)
			tc.check(t, rec.Code, rec.Body.Bytes())
		})
	}
}
