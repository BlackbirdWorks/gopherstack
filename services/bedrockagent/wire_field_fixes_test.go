package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteFlow_NoInventedStatusKey_RealClient guards against
// handleDeleteFlow fabricating a "status" key. DeleteFlowOutput
// (bedrockagent@v1.58.4 deserializers.go's
// awsRestjson1_deserializeOpDocumentDeleteFlowOutput) declares only "id" --
// a typed client silently discards an unknown key, so the raw body is the
// only way to prove the fabricated key is gone.
func TestDeleteFlow_NoInventedStatusKey_RealClient(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createRec := doRequest(t, h, e, http.MethodPost, "/flows", map[string]any{
		"name":             "wire-fix-flow",
		"executionRoleArn": "arn:aws:iam::123456789012:role/FlowRole",
		"definition":       map[string]any{"nodes": []any{}, "connections": []any{}},
	})
	require.Equal(t, http.StatusCreated, createRec.Code, createRec.Body.String())

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	flowID, _ := created["id"].(string)
	require.NotEmpty(t, flowID)

	rec := doRequest(t, h, e, http.MethodDelete, "/flows/"+flowID, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"status"`, "DeleteFlowOutput has no status member")
	assert.Contains(t, body, `"id"`)
}

// TestDeleteFlowVersion_NoInventedStatusKey_RealClient guards against
// handleDeleteFlowVersion fabricating a "status" key. DeleteFlowVersionOutput
// (bedrockagent@v1.58.4 deserializers.go's
// awsRestjson1_deserializeOpDocumentDeleteFlowVersionOutput) declares only
// "id" and "version".
func TestDeleteFlowVersion_NoInventedStatusKey_RealClient(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createRec := doRequest(t, h, e, http.MethodPost, "/flows", map[string]any{
		"name":             "wire-fix-flow-version",
		"executionRoleArn": "arn:aws:iam::123456789012:role/FlowRole",
		"definition":       map[string]any{"nodes": []any{}, "connections": []any{}},
	})
	require.Equal(t, http.StatusCreated, createRec.Code, createRec.Body.String())

	var created map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	flowID, _ := created["id"].(string)
	require.NotEmpty(t, flowID)

	versionRec := doRequest(t, h, e, http.MethodPost, "/flows/"+flowID+"/versions", nil)
	require.Equal(t, http.StatusCreated, versionRec.Code, versionRec.Body.String())

	var versionBody map[string]any
	require.NoError(t, json.Unmarshal(versionRec.Body.Bytes(), &versionBody))
	version, _ := versionBody["version"].(string)
	require.NotEmpty(t, version)

	rec := doRequest(t, h, e, http.MethodDelete, "/flows/"+flowID+"/versions/"+version, nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	body := rec.Body.String()
	assert.NotContains(t, body, `"status"`, "DeleteFlowVersionOutput has no status member")
	assert.Contains(t, body, `"id"`)
	assert.Contains(t, body, `"version"`)
}
