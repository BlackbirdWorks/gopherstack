package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProtectedQueryInitialStatusIsSubmitted verifies that newly started
// protected queries have status "SUBMITTED" (not "STARTED").
func TestProtectedQueryInitialStatusIsSubmitted(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "pq-collab", "creatorDisplayName": "Eve",
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

	rec3 := doRequest(t, e, "POST", "/memberships/"+mID+"/protectedQueries", map[string]any{
		"sqlParameters":       map[string]any{"queryString": "SELECT 1"},
		"resultConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec3.Code, rec3.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp))
	pq := resp["protectedQuery"].(map[string]any)

	assert.Equal(t, "SUBMITTED", pq["status"],
		"newly started protected query must have status SUBMITTED, not STARTED")
}

// TestProtectedQueryHasMembershipID verifies the canonical "membershipId" key.
func TestProtectedQueryHasMembershipID(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "pq2-collab", "creatorDisplayName": "Frank",
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

	rec3 := doRequest(t, e, "POST", "/memberships/"+mID+"/protectedQueries", map[string]any{
		"sqlParameters":       map[string]any{"queryString": "SELECT 1"},
		"resultConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp))
	pq := resp["protectedQuery"].(map[string]any)

	assert.Contains(t, pq, "membershipId", "protectedQuery must have 'membershipId' key (AWS canonical)")
	assert.Contains(t, pq, "membershipIdentifier", "protectedQuery must have legacy 'membershipIdentifier'")
	assert.Equal(t, mID, pq["membershipId"])
	assert.Equal(t, pq["membershipId"], pq["membershipIdentifier"])
}
