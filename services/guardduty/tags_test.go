package guardduty_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fn   func(t *testing.T, h *guardduty.Handler)
		name string
	}{
		{
			name: "tag_and_list_resource",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				arn := "arn:aws:guardduty:us-east-1:123456789012:detector/abc123"

				rec := doRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
					"tags": map[string]string{"env": "prod", "owner": "alice"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
				assert.Equal(t, "alice", tags["owner"])
			},
		},
		{
			name: "untag_resource",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				arn := "arn:aws:guardduty:us-east-1:123456789012:detector/def456"

				doRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
					"tags": map[string]string{"k1": "v1", "k2": "v2"},
				})

				rec := doRequest(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=k1", nil)
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags := resp["tags"].(map[string]any)
				assert.NotContains(t, tags, "k1")
				assert.Equal(t, "v2", tags["k2"])
			},
		},
		{
			name: "list_tags_empty",
			fn: func(t *testing.T, h *guardduty.Handler) {
				t.Helper()
				arn := "arn:aws:guardduty:us-east-1:123456789012:detector/ghi789"

				rec := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Empty(t, tags)
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

func TestTagResource_Merges_Creation_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
		"tags":   map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["detectorId"].(string)

	arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s", id)

	rec = doRequest(t, h, http.MethodPost, "/tags/"+arn, map[string]any{
		"tags": map[string]string{"owner": "alice"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["tags"].(map[string]any)

	assert.Equal(t, "test", tags["env"], "creation-time tag must be preserved")
	assert.Equal(t, "alice", tags["owner"], "new tag must be added via TagResource")
}

func TestUntagResource_Removes_Only_Specified_Key(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
		"tags":   map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	id := createResp["detectorId"].(string)

	arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:123456789012:detector/%s", id)

	rec = doRequest(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=k2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["tags"].(map[string]any)

	assert.Equal(t, "v1", tags["k1"], "k1 must be preserved")
	assert.NotContains(t, tags, "k2", "k2 must be removed by UntagResource")
	assert.Equal(t, "v3", tags["k3"], "k3 must be preserved")
}
