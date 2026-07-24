package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAnalysisTemplateHasIDKeys verifies AnalysisTemplate canonical ID keys.
func TestAnalysisTemplateHasIDKeys(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	// Create collaboration and membership
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "at-collab", "creatorDisplayName": "Dave",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	rec := doRequest(t, e, "GET", "/collaborations", nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)

	rec2 := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID, "queryLogStatus": "DISABLED",
	})
	var memResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &memResp))
	mID := memResp["membership"].(map[string]any)["id"].(string)

	// Create analysis template
	rec3 := doRequest(t, e, "POST", "/memberships/"+mID+"/analysistemplates", map[string]any{
		"name":   "my-template",
		"format": "SQL",
		"source": map[string]any{"text": "SELECT 1"},
	})
	require.Equal(t, http.StatusOK, rec3.Code, rec3.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp))
	at := resp["analysisTemplate"].(map[string]any)

	assert.Contains(t, at, "id", "analysisTemplate must have 'id' key")
	assert.Contains(t, at, "membershipId", "analysisTemplate must have 'membershipId' key")
	assert.Contains(t, at, "collaborationId", "analysisTemplate must have 'collaborationId' key")

	for _, invented := range []string{"analysisTemplateIdentifier", "membershipIdentifier", "collaborationIdentifier"} {
		assert.NotContains(t, at, invented, "analysisTemplate must not have invented %q key", invented)
	}
}
