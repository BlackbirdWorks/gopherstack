package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// singleS3BucketConfig is a well-formed single-entry Configurations map, the
// shape CreateAccessPreview requires (exactly one element, keyed by resource
// ARN, valued by a one-member Configuration union -- s3Bucket here).
func singleS3BucketConfig(resourceArn string) map[string]any {
	return map[string]any{
		resourceArn: map[string]any{
			"s3Bucket": map[string]any{"bucketPolicy": "{}"},
		},
	}
}

// TestAccessPreviewLifecycle verifies Create/Get/List/ListFindings for access previews.
func TestAccessPreviewLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
		name string
	}{
		{
			name: "create_and_get",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-create")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn":    arn,
					"configurations": singleS3BucketConfig("arn:aws:s3:::preview-create-bucket"),
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				previewID := created["id"]
				require.NotEmpty(t, previewID)

				rec2 := doRequest(t, h, http.MethodGet, "/access-preview/"+previewID+"?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec2.Code)

				var got map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &got))
				ap := got["accessPreview"].(map[string]any)
				assert.Equal(t, previewID, ap["id"])
				assert.Equal(t, "COMPLETED", ap["status"])

				configs, ok := ap["configurations"].(map[string]any)
				require.True(t, ok, "Configurations must be echoed back, not dropped")
				require.Contains(t, configs, "arn:aws:s3:::preview-create-bucket")
				s3Cfg, ok := configs["arn:aws:s3:::preview-create-bucket"].(map[string]any)
				require.True(t, ok)
				assert.Contains(t, s3Cfg, "s3Bucket")
			},
		},
		{
			name: "create_rejects_missing_configurations",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-no-config")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_rejects_multiple_configurations",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-multi-config")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn": arn,
					"configurations": map[string]any{
						"arn:aws:s3:::bucket-a": map[string]any{"s3Bucket": map[string]any{}},
						"arn:aws:s3:::bucket-b": map[string]any{"s3Bucket": map[string]any{}},
					},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "list_access_previews",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-list")

				doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn":    arn,
					"configurations": singleS3BucketConfig("arn:aws:s3:::preview-list-bucket"),
				})

				rec := doRequest(t, h, http.MethodGet, "/access-preview?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				previews, ok := resp["accessPreviews"].([]any)
				require.True(t, ok)
				require.Len(t, previews, 1)

				preview, ok := previews[0].(map[string]any)
				require.True(t, ok)
				_, hasConfigurations := preview["configurations"]
				assert.False(
					t,
					hasConfigurations,
					"AccessPreviewSummary (ListAccessPreviews) has no Configurations member, unlike GetAccessPreview's AccessPreview",
				)
			},
		},
		{
			name: "list_access_preview_findings",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-findings")
				mustFinding(t, b, "preview-findings")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn":    arn,
					"configurations": singleS3BucketConfig("arn:aws:s3:::preview-findings-bucket"),
				})
				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				previewID := created["id"]

				rec2 := doRequest(t, h, http.MethodPost, "/access-preview/"+previewID, map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec2.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
				findings := resp["findings"].([]any)
				require.Len(t, findings, 1)

				// types.AccessPreviewFinding has "id"/"changeType" and no
				// analyzerArn member -- it is NOT the v1 Finding/FindingSummary
				// shape, despite gopherstack modeling both from the same
				// underlying record.
				f, ok := findings[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "NEW", f["changeType"])
				assert.Equal(t, "000000000000", f["resourceOwnerAccount"])
				_, hasAnalyzerArn := f["analyzerArn"]
				assert.False(t, hasAnalyzerArn, "AccessPreviewFinding has no analyzerArn member")
			},
		},
		{
			name: "list_access_preview_findings_missing_analyzer_arn",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-findings-no-arn")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn":    arn,
					"configurations": singleS3BucketConfig("arn:aws:s3:::preview-findings-no-arn-bucket"),
				})
				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				previewID := created["id"]

				// ListAccessPreviewFindingsInput requires analyzerArn.
				rec2 := doRequest(t, h, http.MethodPost, "/access-preview/"+previewID, map[string]any{})
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
			},
		},
		{
			name: "get_missing_preview",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(t, h, http.MethodGet, "/access-preview/no-such-id", nil)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			tt.fn(t, b, h)
		})
	}
}
