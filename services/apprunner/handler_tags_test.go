package apprunner_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	// TagResource
	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": svcArn,
		"Tags": []any{
			map[string]any{"Key": "env", "Value": "prod"},
			map[string]any{"Key": "team", "Value": "platform"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp, jsonx.V1()))
	tags, ok := listResp["Tags"].([]any)
	require.True(t, ok)
	assert.Len(t, tags, 2)

	// UntagResource
	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": svcArn,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify untag
	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": svcArn})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp, jsonx.V1()))
	tags = listResp["Tags"].([]any)
	assert.Len(t, tags, 1)
}
