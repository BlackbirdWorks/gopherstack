package accessanalyzer_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestAnalyzedResourceLifecycle verifies Get/List analyzed resources.
func TestAnalyzedResourceLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler)
		name string
	}{
		{
			name: "get_analyzed_resource",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "ar-get-analyzer")
				resourceArn := "arn:aws:s3:::my-bucket"

				_, err := b.AddAnalyzedResource(arn, resourceArn, "AWS::S3::Bucket", false)
				require.NoError(t, err)

				rec := doRequest(t, h, http.MethodGet,
					"/analyzed-resource?analyzerArn="+arn+"&resourceArn="+resourceArn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				resource := resp["resource"].(map[string]any)
				assert.Equal(t, resourceArn, resource["resourceArn"])
				// resourceOwnerAccount is a required AnalyzedResource member.
				assert.Equal(t, "000000000000", resource["resourceOwnerAccount"])
			},
		},
		{
			name: "get_missing_resource",
			fn: func(t *testing.T, _ *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				rec := doRequest(
					t,
					h,
					http.MethodGet,
					"/analyzed-resource?analyzerArn=arn:aws:access-analyzer:us-east-1:000000000000:analyzer/a&resourceArn=arn:aws:s3:::no-bucket", //nolint:lll // existing issue.
					nil,
				)
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "list_analyzed_resources",
			fn: func(t *testing.T, b *accessanalyzer.InMemoryBackend, h *accessanalyzer.Handler) {
				t.Helper()
				arn := mustAnalyzer(t, b, "ar-list-analyzer")

				_, _ = b.AddAnalyzedResource(arn, "arn:aws:s3:::bucket1", "AWS::S3::Bucket", false)
				_, _ = b.AddAnalyzedResource(arn, "arn:aws:s3:::bucket2", "AWS::S3::Bucket", true)

				rec := doRequest(t, h, http.MethodPost, "/analyzed-resource", map[string]any{
					"analyzerArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				resources := resp["analyzedResources"].([]any)
				assert.Len(t, resources, 2)

				// resourceOwnerAccount is a required AnalyzedResourceSummary member.
				first, ok := resources[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "000000000000", first["resourceOwnerAccount"])
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
