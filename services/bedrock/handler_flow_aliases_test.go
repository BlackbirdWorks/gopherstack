package bedrock_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlowAliasCRUD(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create flow
	rec := doAgentRequest(t, h, http.MethodPost, "/flows", map[string]any{"name": "fa-flow"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var fb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fb))
	flowID, _ := fb["id"].(string)

	// Create alias
	rec = doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/flows/%s/aliases", flowID),
		map[string]any{"name": "my-alias"},
	)
	assert.Equal(t, http.StatusCreated, rec.Code)

	var ab map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ab))
	aliasID, _ := ab["id"].(string)

	// Get alias
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/aliases/%s", flowID, aliasID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List aliases
	rec = doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/flows/%s/aliases", flowID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var lb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &lb))
	assert.Len(t, lb["flowAliasSummaries"], 1)

	// Update alias
	rec = doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/flows/%s/aliases/%s", flowID, aliasID),
		map[string]any{"name": "updated-alias"},
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete alias
	rec = doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/flows/%s/aliases/%s", flowID, aliasID), nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
