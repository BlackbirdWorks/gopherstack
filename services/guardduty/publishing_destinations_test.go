package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishingDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "create_describe_update_list_delete",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()

				id := createTestDetector(t, h)

				// CreatePublishingDestination
				rec := doRequest(t, h, http.MethodPost, "/detector/"+id+"/publishingDestination", map[string]any{
					"destinationType": "S3",
					"destinationProperties": map[string]any{
						"destinationArn": "arn:aws:s3:::my-bucket",
						"kmsKeyArn":      "arn:aws:kms:us-east-1:123456789012:key/abc",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				destID, _ := createResp["destinationId"].(string)
				require.NotEmpty(t, destID)

				// DescribePublishingDestination
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/publishingDestination/"+destID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var descResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				assert.Equal(t, "S3", descResp["destinationType"])

				// UpdatePublishingDestination
				rec = doRequest(t, h, http.MethodPost, "/detector/"+id+"/publishingDestination/"+destID, map[string]any{
					"destinationProperties": map[string]any{
						"destinationArn": "arn:aws:s3:::my-bucket-v2",
						"kmsKeyArn":      "arn:aws:kms:us-east-1:123456789012:key/def",
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// ListPublishingDestinations
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/publishingDestination", nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var listResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				dests, _ := listResp["destinations"].([]any)
				assert.Len(t, dests, 1)

				// DeletePublishingDestination
				rec = doRequest(t, h, http.MethodDelete, "/detector/"+id+"/publishingDestination/"+destID, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				// List after delete
				rec = doRequest(t, h, http.MethodGet, "/detector/"+id+"/publishingDestination", nil)
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
				destsAfter, _ := listResp["destinations"].([]any)
				assert.Empty(t, destsAfter)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.fn(t, h)
		})
	}
}
