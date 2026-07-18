package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromptVersionCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create prompt
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "pv-prompt"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var pb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pb))
	promptID := pb["prompt"].(map[string]any)["promptId"].(string)

	// Create version
	rec = doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/prompts/%s/versions", promptID), nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var vb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vb))
	version := vb["promptVersion"].(map[string]any)["version"].(string)
	assert.Equal(t, "1", version)

	// Get version
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/prompts/%s/versions/%s", promptID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List versions
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/prompts/%s/versions", promptID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["promptVersionSummaries"], 1)

	// Delete version
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/prompts/%s/versions/%s", promptID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAccuracy_Prompt_ListVersionsReturnsCreatedVersions(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "multi-version-prompt"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	promptID := createBody["prompt"].(map[string]any)["promptId"].(string)

	for range 3 {
		vRec := doAgentRequest(t, h, http.MethodPost, "/prompts/"+promptID+"/versions", nil)
		require.Equal(t, http.StatusCreated, vRec.Code)
	}

	listRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID+"/versions", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	versions := listBody["promptVersionSummaries"].([]any)
	assert.Len(t, versions, 3)
}
