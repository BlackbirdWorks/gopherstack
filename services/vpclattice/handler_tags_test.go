package vpclattice_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagging tests tagging operations.
func TestTagging(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	// create a service to tag
	recSvc := doRequest(t, h, http.MethodPost, "/services", map[string]any{
		"name": "svc-tag",
		"tags": map[string]any{"env": "dev"},
	})
	require.Equal(t, http.StatusCreated, recSvc.Code)
	svcData := parseBody(t, recSvc)
	svcArn, _ := svcData["arn"].(string)

	// list tags
	rec := doRequest(t, h, http.MethodGet, "/tags/"+svcArn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	tagsResp := parseBody(t, rec)
	tags, _ := tagsResp["tags"].(map[string]any)
	assert.Equal(t, "dev", tags["env"])

	// tag resource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+svcArn, map[string]any{
		"tags": map[string]any{"team": "platform"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// verify tag was added
	rec = doRequest(t, h, http.MethodGet, "/tags/"+svcArn, nil)
	tagsResp = parseBody(t, rec)
	tags, _ = tagsResp["tags"].(map[string]any)
	assert.Equal(t, "dev", tags["env"])
	assert.Equal(t, "platform", tags["team"])
}
