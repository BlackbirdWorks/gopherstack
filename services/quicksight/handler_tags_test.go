package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// ---- Tag tests ----

// dashboardTagARN is the ARN of the dashboard every TestQuickSight_Tags case
// creates via createTaggableDashboard before exercising Tag/Untag/ListTags --
// real AWS's TagResource/UntagResource/ListTagsForResource all return
// ResourceNotFoundException for an ARN that doesn't identify an existing
// resource (see TestQuickSight_Tags_UnknownARN), so these tests must tag a
// resource that genuinely exists rather than a bare string.
const dashboardTagARN = "arn:aws:quicksight:us-east-1:000000000000:dashboard/dash1"

// createTaggableDashboard creates the dashboard behind dashboardTagARN so
// tag operations against it succeed.
func createTaggableDashboard(t *testing.T, h *quicksight.Handler) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, accountPath("/dashboards/dash1"), map[string]any{"Name": "dash1"})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestQuickSight_Tags(t *testing.T) {
	t.Parallel()

	tagPath := fmt.Sprintf("/resources/%s/tags", dashboardTagARN)

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
			name:     "ListTagsForResource untagged existing resource returns empty list",
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
			createTaggableDashboard(t, h)
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

// TestQuickSight_Tags_UnknownARN locks the fix for the gap noted in
// PARITY.md: TagResource/UntagResource/ListTagsForResource used to accept
// any ARN string with no check that the resource actually exists. Real AWS
// returns ResourceNotFoundException for an ARN that isn't a real resource;
// this backend now checks resource existence (InMemoryBackend.arnExists)
// before mutating/reading b.tags.
func TestQuickSight_Tags_UnknownARN(t *testing.T) {
	t.Parallel()

	unknownARN := "arn:aws:quicksight:us-east-1:000000000000:dashboard/does-not-exist"
	tagPath := fmt.Sprintf("/resources/%s/tags", unknownARN)

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "TagResource unknown ARN returns 404 ResourceNotFoundException",
			method: http.MethodPost,
			path:   tagPath,
			body: map[string]any{
				"Tags": []any{map[string]any{"Key": "env", "Value": "prod"}},
			},
		},
		{
			name:   "UntagResource unknown ARN returns 404 ResourceNotFoundException",
			method: http.MethodDelete,
			path:   tagPath + "?keys=env",
		},
		{
			name:   "ListTagsForResource unknown ARN returns 404 ResourceNotFoundException",
			method: http.MethodGet,
			path:   tagPath,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			require.Equal(t, http.StatusNotFound, rec.Code)
			body := parseBody(t, rec)
			assert.Equal(t, "ResourceNotFoundException", body["Code"])
		})
	}
}
