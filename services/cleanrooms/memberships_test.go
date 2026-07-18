package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMembershipHasBothIDKeys verifies that a Membership response
// includes both "id" (AWS canonical) and "membershipIdentifier" (legacy).
func TestMembershipHasBothIDKeys(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "m-collab", "creatorDisplayName": "Bob",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var colResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaboration"].(map[string]any)["id"].(string)

	rec2 := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID,
		"queryLogStatus":          "DISABLED",
		"memberAbilities":         []string{"CAN_QUERY", "CAN_RECEIVE_RESULTS"},
	})
	require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	mem := resp["membership"].(map[string]any)

	id, hasID := mem["id"]
	legacyID, hasLegacy := mem["membershipIdentifier"]
	collabID, hasCollabID := mem["collaborationId"]
	legacyCollabID, hasLegacyCollab := mem["collaborationIdentifier"]

	assert.True(t, hasID, "membership must have 'id' key (AWS canonical)")
	assert.True(t, hasLegacy, "membership must have 'membershipIdentifier' (backward compat)")
	assert.True(t, hasCollabID, "membership must have 'collaborationId' key (AWS canonical)")
	assert.True(t, hasLegacyCollab, "membership must have 'collaborationIdentifier' (backward compat)")
	assert.Equal(t, id, legacyID)
	assert.Equal(t, collabID, legacyCollabID)
	assert.Equal(t, colID, collabID)

	// MemberAbilities must be present and populated
	abilities, ok := mem["memberAbilities"].([]any)
	assert.True(t, ok, "memberAbilities must be present")
	assert.Len(t, abilities, 2)
}

// TestMembershipMemberAbilitiesRoundtrip verifies that memberAbilities
// sent during CreateMembership are returned in the response.
func TestMembershipMemberAbilitiesRoundtrip(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "ab-collab", "creatorDisplayName": "Carol",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	rec := doRequest(t, e, "GET", "/collaborations", nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)

	rec2 := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID,
		"queryLogStatus":          "ENABLED",
		"memberAbilities":         []string{"CAN_QUERY", "CAN_RECEIVE_RESULTS"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	mem := resp["membership"].(map[string]any)

	abilities, ok := mem["memberAbilities"].([]any)
	require.True(t, ok, "memberAbilities must be present")
	assert.Len(t, abilities, 2, "all provided abilities should be returned")
}

// TestMembershipARNFormat verifies ARN format for memberships.
func TestMembershipARNFormat(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "arn-m-collab", "creatorDisplayName": "Hank",
		"creatorMemberAbilities": []string{},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	rec := doRequest(t, e, "GET", "/collaborations", nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)

	rec2 := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	mem := resp["membership"].(map[string]any)

	arn, ok := mem["arn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:cleanrooms:")
	assert.Contains(t, arn, ":membership/")
}

// TestListMembershipsReturnsSummaryWithIDKeys verifies that
// ListMemberships responses include the canonical "id" and "collaborationId" keys.
func TestListMembershipsReturnsSummaryWithIDKeys(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "list-m-collab", "creatorDisplayName": "Liam",
		"creatorMemberAbilities": []string{},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	rec := doRequest(t, e, "GET", "/collaborations", nil)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)

	doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID, "queryLogStatus": "DISABLED",
	})

	recList := doRequest(t, e, "GET", "/memberships", nil)
	require.Equal(t, http.StatusOK, recList.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	summaries := listResp["membershipSummaries"].([]any)
	require.Len(t, summaries, 1)
	summary := summaries[0].(map[string]any)

	assert.Contains(t, summary, "id", "membership summary must have canonical 'id' key")
	assert.Contains(t, summary, "collaborationId", "membership summary must have canonical 'collaborationId' key")
	assert.Equal(t, colID, summary["collaborationId"])
}
