package securityhub_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Batch-1 accuracy gap: TagResource is POST /tags/{ResourceArn}.
func TestTagResourcePath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	testArn := "arn:aws:securityhub:us-east-1:000000000000:hub/default"

	rec := doRequest(t, h, http.MethodPost, "/tags/"+testArn, map[string]any{
		"Tags": map[string]string{"env": "test", "team": "security"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// Batch-1 accuracy gap: ListTagsForResource is GET /tags/{ResourceArn}.
func TestListTagsForResourceIsGETTagsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	testArn := "arn:aws:securityhub:us-east-1:000000000000:hub/default"

	doRequest(t, h, http.MethodPost, "/tags/"+testArn, map[string]any{
		"Tags": map[string]string{"env": "prod", "cost-center": "security"},
	})

	rec := doRequest(t, h, http.MethodGet, "/tags/"+testArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tags, _ := resp["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "security", tags["cost-center"])
}

// Batch-1 accuracy gap: UntagResource is DELETE /tags/{ResourceArn}?tagKeys=k1&tagKeys=k2.
func TestUntagResourceIsDELETETagsArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	testArn := "arn:aws:securityhub:us-east-1:000000000000:hub/default"

	doRequest(t, h, http.MethodPost, "/tags/"+testArn, map[string]any{
		"Tags": map[string]string{"env": "test", "team": "security"},
	})

	req := httptest.NewRequest(http.MethodDelete, "/tags/"+testArn+"?tagKeys=env", nil)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify tag removed
	getRec := doRequest(t, h, http.MethodGet, "/tags/"+testArn, nil)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &resp))

	tags, _ := resp["Tags"].(map[string]any)
	_, hasEnv := tags["env"]
	assert.False(t, hasEnv, "env tag must be removed")
	assert.Equal(t, "security", tags["team"], "team tag must remain")
}
