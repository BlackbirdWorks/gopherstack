package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlowVersionCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create flow
	rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "fv-flow"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var fb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fb))
	flowID, _ := fb["id"].(string)

	// Create version
	rec = doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/flows/%s/versions", flowID), nil)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var vb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &vb))
	version, _ := vb["version"].(string)
	assert.Equal(t, "1", version)

	// Get version
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/versions/%s", flowID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List versions
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/versions", flowID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["flowVersionSummaries"], 1)

	// Delete version
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/flows/%s/versions/%s", flowID, version), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
