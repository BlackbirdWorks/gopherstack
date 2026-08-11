package bedrock_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPromptCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{
		"name": "my-prompt",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	promptID, _ := body["id"].(string)
	assert.NotEmpty(t, promptID)

	// Get
	rec = doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doAgentRequest(t, h, http.MethodGet, "/prompts", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["promptSummaries"], 1)

	// Update
	rec = doAgentRequest(t, h, http.MethodPut, "/prompts/"+promptID, map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doAgentRequest(t, h, http.MethodDelete, "/prompts/"+promptID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get after delete → 404
	rec = doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestCreatePrompt_ConflictOnDuplicate(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "dup"})
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "dup"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestAccuracy_Prompt_VariantPreserved(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{
		"name":        "variant-prompt",
		"description": "a prompt with variants",
		"variants": []map[string]any{
			{
				"name":         "default",
				"templateType": "TEXT",
				"modelId":      "amazon.titan-text-express-v1",
				"templateConfiguration": map[string]any{
					"text": map[string]any{
						"text": "You are a helpful assistant. {{user_input}}",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	promptID, _ := createBody["id"].(string)
	assert.NotEmpty(t, promptID)

	// GET preserves name and description
	getRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var gotPrompt map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &gotPrompt))
	assert.Equal(t, "variant-prompt", gotPrompt["name"])
	assert.Equal(t, "a prompt with variants", gotPrompt["description"])
}

func TestAccuracy_Prompt_VersionPreservesContent(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create prompt
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{
		"name": "versioned-prompt",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	promptID, _ := createBody["id"].(string)

	// Create version
	verRec := doAgentRequest(t, h, http.MethodPost, "/prompts/"+promptID+"/versions", nil)
	require.Equal(t, http.StatusCreated, verRec.Code)

	var verBody map[string]any
	require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verBody))
	pv := verBody["promptVersion"].(map[string]any)
	version := pv["version"].(string)
	assert.NotEmpty(t, version)

	// GET version preserves prompt ID
	getVerRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID+"/versions/"+version, nil)
	require.Equal(t, http.StatusOK, getVerRec.Code)

	var getVerBody map[string]any
	require.NoError(t, json.Unmarshal(getVerRec.Body.Bytes(), &getVerBody))
	gotVer := getVerBody["promptVersion"].(map[string]any)
	assert.Equal(t, promptID, gotVer["promptId"])
}

func TestAccuracy_Prompt_UpdateNameAndDescription(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts",
		map[string]any{"name": "original-name", "description": "original desc"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	promptID, _ := createBody["id"].(string)

	// Update
	updateRec := doAgentRequest(t, h, http.MethodPut, "/prompts/"+promptID,
		map[string]any{"name": "updated-name", "description": "updated desc"})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Verify
	getRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var p map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &p))
	assert.Equal(t, "updated-name", p["name"])
	assert.Equal(t, "updated desc", p["description"])
}
