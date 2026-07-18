package codedeploy_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGitHubTokens_ListAddDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// List should be empty initially.
	rec := doRequest(t, h, "ListGitHubAccountTokenNames", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		TokenNameList []string `json:"tokenNameList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.TokenNameList)

	// Add token via internal method and verify it appears.
	h.Backend.AddGitHubAccountTokenInternal("my-token")

	rec = doRequest(t, h, "ListGitHubAccountTokenNames", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Equal(t, []string{"my-token"}, listResp.TokenNameList)

	// Delete the token.
	rec = doRequest(t, h, "DeleteGitHubAccountToken", map[string]any{"tokenName": "my-token"})
	require.Equal(t, http.StatusOK, rec.Code)

	// List should be empty again.
	rec = doRequest(t, h, "ListGitHubAccountTokenNames", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Empty(t, listResp.TokenNameList)
}

func TestGitHubTokens_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteGitHubAccountToken", map[string]any{"tokenName": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "GitHubAccountTokenDoesNotExistException", resp["__type"])
}

func TestGitHubTokens_DeleteMissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteGitHubAccountToken", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
