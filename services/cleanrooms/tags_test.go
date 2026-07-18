package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagsLifecycle verifies tag/untag/list on a collaboration ARN.
func TestTagsLifecycle(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "tag-collab", "creatorDisplayName": "Kate",
		"creatorMemberAbilities": []string{},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var colResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	arn := colResp["collaboration"].(map[string]any)["arn"].(string)

	// Tag
	recTag := doRequest(t, e, "POST", "/tags/"+arn, map[string]any{
		"tags": map[string]string{"env": "prod", "team": "data"},
	})
	require.Equal(t, http.StatusOK, recTag.Code)

	// List
	recList := doRequest(t, e, "GET", "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, recList.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	tags := listResp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "data", tags["team"])

	// Untag
	recUntag := doRequest(t, e, "DELETE", "/tags/"+arn+"?tagKeys=env", nil)
	require.Equal(t, http.StatusOK, recUntag.Code)

	// Verify removal
	recList2 := doRequest(t, e, "GET", "/tags/"+arn, nil)
	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listResp2))
	tags2 := listResp2["tags"].(map[string]any)
	assert.NotContains(t, tags2, "env")
	assert.Equal(t, "data", tags2["team"])
}
