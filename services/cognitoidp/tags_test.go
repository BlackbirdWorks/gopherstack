package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := "arn:aws:cognito-idp:us-east-1:000000000000:userpool/us-east-1_abc"

	// TagResource
	rec := doCognitoRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": arn,
		"Tags":        map[string]string{"env": "prod", "team": "platform"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doCognitoRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Tags map[string]string `json:"Tags,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Equal(t, "prod", listResp.Tags["env"])
	assert.Equal(t, "platform", listResp.Tags["team"])

	// UntagResource
	rec = doCognitoRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": arn,
		"TagKeys":     []string{"env"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// List after untag — fresh variable to avoid JSON merge artefact
	rec = doCognitoRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceArn": arn,
	})
	var afterUntagResp struct {
		Tags map[string]string `json:"Tags,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterUntagResp))
	assert.NotContains(t, afterUntagResp.Tags, "env")
	assert.Equal(t, "platform", afterUntagResp.Tags["team"])
}
