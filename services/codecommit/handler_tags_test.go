package codecommit_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

func TestHandler_TagAndUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codecommit.Handler) string
		name       string
		action     string
		wantStatus int
	}{
		{
			name:   "tag_existing_resource",
			action: "TagResource",
			setup: func(t *testing.T, h *codecommit.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "tag-repo",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				metaRaw, ok := resp["repositoryMetadata"]
				require.True(t, ok)

				meta, ok := metaRaw.(map[string]any)
				require.True(t, ok)

				arnRaw, ok := meta["Arn"]
				require.True(t, ok)

				arn, ok := arnRaw.(string)
				require.True(t, ok)

				return arn
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "untag_existing_resource",
			action: "UntagResource",
			setup: func(t *testing.T, h *codecommit.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "untag-repo",
					"tags":           map[string]string{"key1": "val1"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				metaRaw, ok := resp["repositoryMetadata"]
				require.True(t, ok)

				meta, ok := metaRaw.(map[string]any)
				require.True(t, ok)

				arnRaw, ok := meta["Arn"]
				require.True(t, ok)

				arn, ok := arnRaw.(string)
				require.True(t, ok)

				return arn
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			var body map[string]any
			if tt.action == "TagResource" {
				body = map[string]any{
					"resourceArn": resourceARN,
					"tags":        map[string]string{"new-key": "new-val"},
				}
			} else {
				body = map[string]any{
					"resourceArn": resourceARN,
					"tagKeys":     []string{"key1"},
				}
			}

			rec := doRequest(t, h, tt.action, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *codecommit.Handler) string
		wantTags   map[string]string
		name       string
		wantStatus int
	}{
		{
			name: "repository_with_tags",
			setup: func(t *testing.T, h *codecommit.Handler) string {
				t.Helper()
				rec := doRequest(t, h, "CreateRepository", map[string]any{
					"repositoryName": "tagged-repo",
					"tags":           map[string]string{"env": "test"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				metaRaw, ok := resp["repositoryMetadata"]
				require.True(t, ok)

				meta, ok := metaRaw.(map[string]any)
				require.True(t, ok)

				arnRaw, ok := meta["Arn"]
				require.True(t, ok)

				arn, ok := arnRaw.(string)
				require.True(t, ok)

				return arn
			},
			wantStatus: http.StatusOK,
			wantTags:   map[string]string{"env": "test"},
		},
		{
			name: "missing_arn",
			setup: func(_ *testing.T, _ *codecommit.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			rec := doRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": resourceARN,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				tagsRaw, ok := resp["tags"].(map[string]any)
				require.True(t, ok)

				for k, v := range tt.wantTags {
					assert.Equal(t, v, tagsRaw[k])
				}
			}
		})
	}
}

func TestHandler_ARN_TagOps_O1(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create repo and get its ARN
	rec := doRequest(t, h, "CreateRepository", map[string]any{"repositoryName": "repo"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	meta := createResp["repositoryMetadata"].(map[string]any)
	repoARN := meta["Arn"].(string)
	require.NotEmpty(t, repoARN)

	// Tag using ARN
	rec = doRequest(t, h, "TagResource", map[string]any{
		"resourceArn": repoARN,
		"tags":        map[string]string{"env": "prod"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List tags to verify
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": repoARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))

	tagMap, ok := tagsResp["tags"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "prod", tagMap["env"])

	// Untag
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"resourceArn": repoARN,
		"tagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify removed
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": repoARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tagMap, _ = tagsResp["tags"].(map[string]any)
	assert.Empty(t, tagMap)
}
