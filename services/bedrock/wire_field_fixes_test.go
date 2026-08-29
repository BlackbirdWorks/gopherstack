package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeletePromptVersion_NoInventedStatusKey_RealClient guards against
// handleDeletePromptVersion fabricating a "status" key and emitting the
// prompt's id under "promptId" instead of "id". DeletePromptOutput
// (bedrockagent@v1.58.4 deserializers.go's
// awsRestjson1_deserializeOpDocumentDeletePromptOutput) declares only "id"
// and "version" -- no status member, and the wire key for the identifier is
// "id", not "promptId". A typed client silently discards unknown/misnamed
// keys, so the raw body is the only way to prove the fabricated key is gone
// and the real one is present.
func TestDeletePromptVersion_NoInventedStatusKey_RealClient(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	createRec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "wire-fix-prompt"})
	require.Equal(t, http.StatusCreated, createRec.Code, createRec.Body.String())

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	promptID, _ := created["id"].(string)
	require.NotEmpty(t, promptID)

	versionRec := doAgentRequest(t, h, http.MethodPost, fmt.Sprintf("/prompts/%s/versions", promptID), nil)
	require.Equal(t, http.StatusCreated, versionRec.Code, versionRec.Body.String())

	var versionBody map[string]any
	require.NoError(t, json.Unmarshal(versionRec.Body.Bytes(), &versionBody))
	version, _ := versionBody["promptVersion"].(map[string]any)["version"].(string)
	require.NotEmpty(t, version)

	rec := doAgentRequest(t, h, http.MethodDelete, fmt.Sprintf("/prompts/%s/versions/%s", promptID, version), nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"status"`, "DeletePromptOutput has no status member")
	assert.NotContains(t, body, `"promptId"`, "DeletePromptOutput's identifier key is \"id\", not \"promptId\"")
	assert.Contains(t, body, `"id"`, "DeletePromptOutput's real identifier member is \"id\"")

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, promptID, out["id"])
	assert.Equal(t, version, out["version"])
}
