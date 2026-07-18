package mediatailor_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTags_FullCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a playback config to get an ARN
	rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
		"Name":                  "tagged-config",
		"AdDecisionServerUrl":   "https://ads.example.com",
		"VideoContentSourceUrl": "https://video.example.com",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	resourceARN, _ := putResp["PlaybackConfigurationArn"].(string)

	// TagResource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+resourceARN, map[string]any{
		"tags": map[string]any{"env": "prod", "team": "media"},
	})
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "media", tags["team"])

	// UntagResource
	req := httptest.NewRequest(http.MethodDelete, "/tags/"+resourceARN+"?tagKeys=env", nil)
	rec2 := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec2)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusNoContent, rec2.Code)

	// Verify tag removed
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceARN, nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, _ = listResp["tags"].(map[string]any)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "media", tags["team"])
}

func TestTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagBody  map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "tag existing resource succeeds",
			wantCode: http.StatusNoContent,
			tagBody:  map[string]any{"tags": map[string]any{"env": "prod"}},
		},
		{
			name:     "tag any arn succeeds (backend does not validate existence)",
			wantCode: http.StatusNoContent,
			tagBody:  map[string]any{"tags": map[string]any{"env": "prod"}},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if i == 0 {
				// Create a playback config and tag it
				rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
					"Name":                  "taggable",
					"AdDecisionServerUrl":   "https://ads.com",
					"VideoContentSourceUrl": "https://video.com",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn, _ = resp["PlaybackConfigurationArn"].(string)
			} else {
				arn = "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/nonexistent"
			}

			rec := doRequest(t, h, http.MethodPost, "/tags/"+arn, tt.tagBody)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "list tags on existing resource returns tags", wantCode: http.StatusOK},
		{name: "list tags on any arn returns ok (empty tags)", wantCode: http.StatusOK},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if i == 0 {
				rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
					"Name":                  "list-tags-cfg",
					"AdDecisionServerUrl":   "https://ads.com",
					"VideoContentSourceUrl": "https://video.com",
					"tags":                  map[string]any{"key1": "val1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn, _ = resp["PlaybackConfigurationArn"].(string)
			} else {
				arn = "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/nonexistent"
			}

			rec := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{name: "untag existing resource succeeds", wantCode: http.StatusNoContent},
		{name: "untag non-existent resource is idempotent", wantCode: http.StatusNoContent},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			var arn string
			if i == 0 {
				rec := doRequest(t, h, http.MethodPut, "/playbackConfiguration", map[string]any{
					"Name":                  "untag-cfg",
					"AdDecisionServerUrl":   "https://ads.com",
					"VideoContentSourceUrl": "https://video.com",
					"tags":                  map[string]any{"key1": "val1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				arn, _ = resp["PlaybackConfigurationArn"].(string)

				// Actually tag it first
				doRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
					"tags": map[string]any{"key1": "val1"},
				})
			} else {
				arn = "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/nonexistent"
			}

			// Use direct request builder to add query params
			rec := doRequestWithQuery(t, h, http.MethodDelete, "/tags/"+arn, "tagKeys=key1")
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestUntagResource_Idempotent verifies UntagResource is idempotent for
// unknown ARNs.
func TestUntagResource_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	unknownARN := "arn:aws:mediatailor:us-east-1:000000000000:playbackConfiguration/no-such-thing"

	rec := doRequestWithQuery(t, h, http.MethodDelete, "/tags/"+unknownARN, "tagKeys=any-key")
	assert.Equal(t, http.StatusNoContent, rec.Code, "untag on unknown ARN must be idempotent")
}
