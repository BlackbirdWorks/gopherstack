package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Tag tests ----

func TestQuickSight_Tags(t *testing.T) {
	t.Parallel()

	arn := "arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"
	tagPath := fmt.Sprintf("/resources/%s/tags", arn)

	tests := []struct {
		body     any
		setup    func(h *quicksight.Handler)
		check    func(t *testing.T, body map[string]any)
		name     string
		method   string
		path     string
		wantCode int
	}{
		{
			name:   "TagResource adds tags",
			method: http.MethodPost,
			path:   tagPath,
			body: map[string]any{
				"Tags": []any{
					map[string]any{"Key": "env", "Value": "prod"},
					map[string]any{"Key": "team", "Value": "eng"},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForResource returns added tags",
			method: http.MethodGet,
			path:   tagPath,
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"Tags": []any{
						map[string]any{"Key": "k1", "Value": "v1"},
					},
				})
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				tags, ok := body["Tags"].([]any)
				require.True(t, ok)
				assert.Len(t, tags, 1)
				tag, ok := tags[0].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "k1", tag["Key"])
				assert.Equal(t, "v1", tag["Value"])
			},
		},
		{
			name:   "UntagResource removes tags",
			method: http.MethodDelete,
			path:   tagPath + "?keys=env",
			setup: func(h *quicksight.Handler) {
				doRequest(t, h, http.MethodPost, tagPath, map[string]any{
					"Tags": []any{
						map[string]any{"Key": "env", "Value": "prod"},
						map[string]any{"Key": "keep", "Value": "yes"},
					},
				})
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "ListTagsForResource empty ARN returns empty list",
			method:   http.MethodGet,
			path:     tagPath,
			wantCode: http.StatusOK,
			check: func(t *testing.T, body map[string]any) {
				t.Helper()
				tags, ok := body["Tags"].([]any)
				require.True(t, ok)
				assert.Empty(t, tags)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(h)
			}
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
			if tc.check != nil {
				tc.check(t, parseBody(t, rec))
			}
		})
	}
}
