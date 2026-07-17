package xray_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListTagsForResource_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantTags   int
	}{
		{
			name:       "missing ResourceARN rejected",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "resource with no tags returns empty list",
			body:       map[string]any{"ResourceARN": "arn:aws:xray:us-east-1:123:group/default/g1"},
			wantStatus: http.StatusOK,
			wantTags:   0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doXrayRequest(t, h, "/ListTagsForResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				tags, _ := resp["Tags"].([]any)
				assert.Len(t, tags, tt.wantTags)
				assert.Contains(t, resp, "NextToken")
			}
		})
	}
}

func TestHandler_ListTagsForResource_WithTags(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	arn := "arn:aws:xray:us-east-1:123456789012:group/default/tagged"
	tags := map[string]string{
		"env":     "prod",
		"team":    "platform",
		"version": "v1",
		"owner":   "alice",
		"cost":    "high",
	}
	b.TagResource(arn, tags)

	rec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tagsList, _ := resp["Tags"].([]any)
	assert.Len(t, tagsList, len(tags))

	// Each tag should be a map with Key and Value
	for _, tagAny := range tagsList {
		tagMap, ok := tagAny.(map[string]any)
		require.True(t, ok)
		assert.Contains(t, tagMap, "Key")
		assert.Contains(t, tagMap, "Value")
	}
}

func TestTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "single tag",
			tags: map[string]string{"env": "prod"},
		},
		{
			name: "multiple tags",
			tags: map[string]string{"env": "prod", "team": "platform", "cost-center": "eng"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := "arn:aws:xray:us-east-1:123456789012:group/default/test-group"

			tagRec := doXrayRequest(t, h, "/TagResource", map[string]any{
				"ResourceARN": arn,
				"Tags":        tt.tags,
			})
			require.Equal(t, http.StatusOK, tagRec.Code)

			listRec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{
				"ResourceARN": arn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			tags, ok := resp["Tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tags, len(tt.tags))
		})
	}
}

func TestTags_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:xray:us-east-1:123456789012:group/default/untag-group"

	tagRec := doXrayRequest(t, h, "/TagResource", map[string]any{
		"ResourceARN": arn,
		"Tags":        map[string]string{"key1": "val1", "key2": "val2"},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	untagRec := doXrayRequest(t, h, "/UntagResource", map[string]any{
		"ResourceARN": arn,
		"TagKeys":     []string{"key1"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doXrayRequest(t, h, "/ListTagsForResource", map[string]any{"ResourceARN": arn})
	require.Equal(t, http.StatusOK, listRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

	tags, _ := resp["Tags"].([]any)
	assert.Len(t, tags, 1, "only key2 should remain after untagging key1")
}
