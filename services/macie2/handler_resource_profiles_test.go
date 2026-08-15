package macie2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceProfiles(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *macie2.Handler)
		name string
	}{
		{
			name: "get_and_update_resource_profile",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				arn := "arn:aws:s3:::my-bucket"

				// GetResourceProfile — returns default for unknown ARN
				rec := doRequest(t, h, http.MethodGet, "/resource-profiles?resourceArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var profile map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))
				assert.Equal(t, arn, profile["resourceArn"])

				// UpdateResourceProfile
				rec = doRequest(t, h, http.MethodPatch, "/resource-profiles?resourceArn="+arn, map[string]any{
					"sensitivityScoreOverride": 75,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify
				rec = doRequest(t, h, http.MethodGet, "/resource-profiles?resourceArn="+arn, nil)
				var updated map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
				assert.EqualValues(t, 75, updated["sensitivityScore"])
				assert.True(t, updated["sensitivityScoreOverridden"].(bool))
			},
		},
		{
			name: "list_profile_artifacts_and_detections",
			fn: func(t *testing.T, h *macie2.Handler) {
				t.Helper()

				arn := "arn:aws:s3:::another-bucket"

				// ListResourceProfileArtifacts
				rec := doRequest(t, h, http.MethodGet, "/resource-profiles/artifacts?resourceArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var artResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &artResp))
				artifacts, _ := artResp["artifacts"].([]any)
				assert.Empty(t, artifacts)

				// ListResourceProfileDetections
				rec = doRequest(t, h, http.MethodGet, "/resource-profiles/detections?resourceArn="+arn, nil)
				assert.Equal(t, http.StatusOK, rec.Code)

				var detResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &detResp))
				detections, _ := detResp["detections"].([]any)
				assert.Empty(t, detections)

				// UpdateResourceProfileDetections
				rec = doRequest(
					t,
					h,
					http.MethodPatch,
					"/resource-profiles/detections?resourceArn="+arn,
					map[string]any{
						"suppressDataIdentifiers": []map[string]any{},
					},
				)
				assert.Equal(t, http.StatusOK, rec.Code)
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
