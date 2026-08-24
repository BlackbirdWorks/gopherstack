package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataSync_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	agentArn := createTestAgent(t, h)

	// TagResource
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": agentArn,
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "prod"},
			map[string]any{"Key": "team", "Value": "infra"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// UntagResource
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": agentArn,
		"Keys":        []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify untag
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	tags = listResp["Tags"].([]any)
	assert.Len(t, tags, 1)

	// TagResource unknown resource returns 400
	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": "arn:aws:datasync:us-east-1:000000000000:agent/notexist",
		"Tags":        []any{map[string]any{"Key": "k", "Value": "v"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestDataSync_TagsRoundTrip verifies that tags applied via TagResource are visible
// on ListTagsForResource, and that UntagResource removes them.
func TestDataSync_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	agentArn := createTestAgent(t, h)

	// Apply tags.
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": agentArn,
		"Tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "data"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify tags appear.
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// Verify tags are sorted by key.
	if len(tags) == 2 {
		tag0 := tags[0].(map[string]any)
		tag1 := tags[1].(map[string]any)
		assert.Equal(t, "Env", tag0["Key"])
		assert.Equal(t, "Team", tag1["Key"])
	}

	// Remove one tag.
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": agentArn,
		"Keys":        []string{"Env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": agentArn})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	tags, ok = listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 1)
	assert.Equal(t, "Team", tags[0].(map[string]any)["Key"])
}
