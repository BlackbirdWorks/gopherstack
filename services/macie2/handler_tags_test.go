package macie2_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMacie2_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		setup    func(h *macie2.Handler) string
		pathFn   func(arn string) string
		check    func(t *testing.T, body []byte)
		name     string
		method   string
		query    string
		wantCode int
	}{
		{
			name:     "TagResource returns 200",
			setup:    func(h *macie2.Handler) string { return createTestAllowListARN(t, h) },
			method:   http.MethodPost,
			pathFn:   func(arn string) string { return "/tags/" + arn },
			body:     map[string]any{"tags": map[string]string{"env": "test"}},
			wantCode: http.StatusOK,
		},
		{
			name: "ListTagsForResource returns tags",
			setup: func(h *macie2.Handler) string {
				arn := createTestAllowListARN(t, h)
				doRequest(t, h, http.MethodPost, "/tags/"+arn,
					map[string]any{"tags": map[string]string{"env": "prod", "team": "security"}})

				return arn
			},
			method:   http.MethodGet,
			pathFn:   func(arn string) string { return "/tags/" + arn },
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				tags, ok := resp["tags"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "prod", tags["env"])
				assert.Equal(t, "security", tags["team"])
			},
		},
		{
			name: "UntagResource removes tags",
			setup: func(h *macie2.Handler) string {
				arn := createTestAllowListARN(t, h)
				doRequest(t, h, http.MethodPost, "/tags/"+arn,
					map[string]any{"tags": map[string]string{"env": "prod", "team": "security"}})

				return arn
			},
			method:   http.MethodDelete,
			pathFn:   func(arn string) string { return "/tags/" + arn },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arn := ""
			if tt.setup != nil {
				arn = tt.setup(h)
			}

			path := tt.pathFn(arn)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil {
				tt.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestMacie2_TagOperations(t *testing.T) {
	t.Parallel()

	const unknownARN = "arn:aws:macie2:us-east-1:000000000000:allow-list/nonexistent-id"

	tests := []struct {
		body      any
		setup     func(h *macie2.Handler) string
		pathFn    func(arn string) string
		name      string
		method    string
		wantError string
		wantCode  int
	}{
		{
			name:      "TagResource on unknown ARN returns 404",
			pathFn:    func(_ string) string { return "/tags/" + unknownARN },
			method:    http.MethodPost,
			body:      map[string]any{"tags": map[string]string{"k": "v"}},
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "UntagResource on unknown ARN returns 404",
			pathFn:    func(_ string) string { return "/tags/" + unknownARN },
			method:    http.MethodDelete,
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name:      "ListTagsForResource on unknown ARN returns 404",
			pathFn:    func(_ string) string { return "/tags/" + unknownARN },
			method:    http.MethodGet,
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
		{
			name: "TagResource with key exceeding 128 chars returns 400",
			setup: func(h *macie2.Handler) string {
				return createTestAllowListARN(t, h)
			},
			method: http.MethodPost,
			pathFn: func(arn string) string { return "/tags/" + arn },
			body: map[string]any{
				"tags": map[string]string{
					strings.Repeat("k", 129): "val",
				},
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name: "TagResource with value exceeding 256 chars returns 400",
			setup: func(h *macie2.Handler) string {
				return createTestAllowListARN(t, h)
			},
			method: http.MethodPost,
			pathFn: func(arn string) string { return "/tags/" + arn },
			body: map[string]any{
				"tags": map[string]string{
					"key": strings.Repeat("v", 257),
				},
			},
			wantCode:  http.StatusBadRequest,
			wantError: "ValidationException",
		},
		{
			name: "TagResource on deleted custom-data-identifier returns 404",
			setup: func(h *macie2.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/custom-data-identifiers", map[string]any{
					"name":  "temp-cdi",
					"regex": `\d+`,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				id := resp["customDataIdentifierId"]
				doRequest(t, h, http.MethodDelete, "/custom-data-identifiers/"+id, nil)

				return "arn:aws:macie2:us-east-1:000000000000:custom-data-identifier/" + id
			},
			method:    http.MethodPost,
			pathFn:    func(arn string) string { return "/tags/" + arn },
			body:      map[string]any{"tags": map[string]string{"k": "v"}},
			wantCode:  http.StatusNotFound,
			wantError: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			arn := ""
			if tt.setup != nil {
				arn = tt.setup(h)
			}

			path := tt.pathFn(arn)
			rec := doRequest(t, h, tt.method, path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestCreationTagsVisibleViaTagAPI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "tag-create",
		"name":        "tagged-at-create",
		"criteria":    map[string]any{"regex": "\\w+"},
		"tags":        map[string]string{"env": "test", "team": "security"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	arn := created["arn"]

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagResp))

	tags, ok := tagResp["tags"].(map[string]any)
	require.True(t, ok, "tags must be an object")
	assert.Equal(t, "test", tags["env"], "creation-time tag env must be visible via ListTagsForResource")
	assert.Equal(t, "security", tags["team"], "creation-time tag team must be visible via ListTagsForResource")
}

func TestUntagResourceRemovesSpecificKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "untag-test",
		"name":        "untag-list",
		"criteria":    map[string]any{"regex": "\\d+"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	arn := created["arn"]

	doRequest(t, h, http.MethodPost, "/tags/"+arn,
		map[string]any{"tags": map[string]string{"keep": "yes", "remove": "me"}})

	doRequest(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=remove", nil)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagResp))
	tags, ok := tagResp["tags"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "yes", tags["keep"], "key 'keep' must remain after untagging 'remove'")
	_, hasRemove := tags["remove"]
	assert.False(t, hasRemove, "key 'remove' must be absent after UntagResource")
}

// 12. TagResource merges with creation-time tags

func TestTagResourceMergesWithCreationTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/allow-lists", map[string]any{
		"clientToken": "merge-test",
		"name":        "merge-list",
		"criteria":    map[string]any{"regex": "\\d+"},
		"tags":        map[string]string{"origin": "create"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	arn := created["arn"]

	doRequest(t, h, http.MethodPost, "/tags/"+arn,
		map[string]any{"tags": map[string]string{"added": "later"}})

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var tagResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagResp))
	tags, ok := tagResp["tags"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, "create", tags["origin"], "creation-time tag must be present after TagResource call")
	assert.Equal(t, "later", tags["added"], "explicitly-added tag must be present")
}
