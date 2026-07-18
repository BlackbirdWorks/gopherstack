package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTags_UnknownResourceNotFound verifies that TagResource, UntagResource,
// and ListTagsForResource return ResourceNotFoundException for an ARN that
// was never created, matching the real Amazon Forecast API (which models
// ResourceNotFoundException on all three operations). Previously these
// operations accepted any ARN and silently wrote/read an orphaned tag map
// entry instead of validating the resource exists.
func TestTags_UnknownResourceNotFound(t *testing.T) {
	t.Parallel()

	unknownARN := "arn:aws:forecast:us-east-1:000000000000:dataset-group/never-created"

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "tag_resource",
			action: "TagResource",
			body: map[string]any{
				"ResourceArn": unknownARN,
				"Tags":        []any{map[string]any{"Key": "env", "Value": "test"}},
			},
		},
		{
			name:   "untag_resource",
			action: "UntagResource",
			body:   map[string]any{"ResourceArn": unknownARN, "TagKeys": []any{"env"}},
		},
		{
			name:   "list_tags_for_resource",
			action: "ListTagsForResource",
			body:   map[string]any{"ResourceArn": unknownARN},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			code, resp := request(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, code)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

// TestTags_RoundTrip verifies TagResource/ListTagsForResource/UntagResource
// Key+Value shape across a tag/list/untag/list cycle.
func TestTags_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, created := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "tag-group", "Domain": "RETAIL",
	})
	require.Equal(t, http.StatusOK, code)
	arn := created["DatasetGroupArn"].(string)

	// Tag
	code, _ = request(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "test"},
			map[string]any{"Key": "owner", "Value": "audit"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	// List tags
	code, m := request(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, code)
	tags, ok := m["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 2)

	tagMap := make(map[string]string)
	for _, tag := range tags {
		t2 := tag.(map[string]any)
		tagMap[t2["Key"].(string)] = t2["Value"].(string)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "audit", tagMap["owner"])

	// Untag
	code, _ = request(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []any{"env"},
	})
	require.Equal(t, http.StatusOK, code)

	_, m = request(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	tags = m["Tags"].([]any)
	assert.Len(t, tags, 1)
	assert.Equal(t, "owner", tags[0].(map[string]any)["Key"])
}
