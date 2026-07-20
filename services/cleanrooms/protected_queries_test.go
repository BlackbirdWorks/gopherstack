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

func TestHTTP_ProtectedQueryEndpoints(t *testing.T) {
	t.Parallel()
	e := newTestServer(t)

	// Create Collaboration
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "collab", "creatorDisplayName": "user",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	rec := doRequest(t, e, "GET", "/collaborations", nil)
	var colResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)

	// Create Membership
	rec2 := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID, "queryLogStatus": "DISABLED",
	})
	var memResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &memResp))
	mID := memResp["membership"].(map[string]any)["id"].(string)

	// Create/Start
	startRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedQueries", map[string]any{
		"sqlParameters": map[string]any{
			"queryString": "SELECT * FROM t",
		},
		"resultConfiguration": map[string]any{},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	id := startResp["protectedQuery"]["id"].(string)

	getRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedQueries/"+id, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	listRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/protectedQueries", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	upRec := doRequest(t, e, http.MethodPatch, "/memberships/"+mID+"/protectedQueries/"+id, map[string]any{
		"targetStatus": "CANCELLED",
	})
	assert.Contains(t, []int{http.StatusOK, http.StatusNotFound, http.StatusConflict}, upRec.Code)
}
