package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

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
					"configurations": map[string]any{},
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
			},
		},
		{
			name: "list_access_previews",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-list")

				doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn": arn, "configurations": map[string]any{},
				})

				rec := doRequest(t, h, http.MethodGet, "/access-preview?analyzerArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp["accessPreviews"], 1)
			},
		},
		{
			name: "list_access_preview_findings",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "preview-findings")
				mustFinding(t, b, "preview-findings")

				rec := doRequest(t, h, http.MethodPut, "/access-preview", map[string]any{
					"analyzerArn": arn, "configurations": map[string]any{},
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
				assert.Len(t, findings, 1)
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
