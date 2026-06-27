package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CollaborationHasBothIDKeys verifies that a Collaboration response
// includes both "id" (AWS canonical) and "collaborationIdentifier" (legacy).
func TestParity_CollaborationHasBothIDKeys(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name":                   "dual-key-collab",
		"creatorDisplayName":     "Alice",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{},
		"queryLogStatus":         "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	collab := resp["collaboration"].(map[string]any)

	id, hasID := collab["id"]
	legacyID, hasLegacy := collab["collaborationIdentifier"]

	assert.True(t, hasID, "collaboration must have 'id' key (AWS canonical)")
	assert.True(t, hasLegacy, "collaboration must have 'collaborationIdentifier' key (backward compat)")
	assert.Equal(t, id, legacyID, "id and collaborationIdentifier must have the same value")
	assert.NotEmpty(t, id)
}

// TestParity_MembershipHasBothIDKeys verifies that a Membership response
// includes both "id" (AWS canonical) and "membershipIdentifier" (legacy).
func TestParity_MembershipHasBothIDKeys(t *testing.T) {
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

// TestParity_MembershipMemberAbilitiesRoundtrip verifies that memberAbilities
// sent during CreateMembership are returned in the response.
func TestParity_MembershipMemberAbilitiesRoundtrip(t *testing.T) {
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

// TestParity_ConfiguredTableHasIDKey verifies that ConfiguredTable responses
// include the canonical "id" key.
func TestParity_ConfiguredTableHasIDKey(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/configuredTables", map[string]any{
		"name":           "id-test-table",
		"allowedColumns": []string{"col1"},
		"analysisMethod": "DIRECT_QUERY",
		"tableReference": map[string]any{"glue": map[string]any{
			"databaseName": "db", "tableName": "tbl",
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ct := resp["configuredTable"].(map[string]any)

	id, hasID := ct["id"]
	legacyID, hasLegacy := ct["configuredTableIdentifier"]

	assert.True(t, hasID, "configuredTable must have 'id' key (AWS canonical)")
	assert.True(t, hasLegacy, "configuredTable must have 'configuredTableIdentifier' (backward compat)")
	assert.Equal(t, id, legacyID)
	assert.NotEmpty(t, id)
}

// TestParity_AnalysisTemplateHasIDKeys verifies AnalysisTemplate canonical ID keys.
func TestParity_AnalysisTemplateHasIDKeys(t *testing.T) {
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
	assert.Contains(
		t,
		at,
		"analysisTemplateIdentifier",
		"analysisTemplate must have legacy 'analysisTemplateIdentifier'",
	)
	assert.Contains(t, at, "membershipId", "analysisTemplate must have 'membershipId' key")
	assert.Contains(t, at, "membershipIdentifier", "analysisTemplate must have legacy 'membershipIdentifier'")
	assert.Contains(t, at, "collaborationId", "analysisTemplate must have 'collaborationId' key")
	assert.Equal(t, at["id"], at["analysisTemplateIdentifier"])
	assert.Equal(t, at["membershipId"], at["membershipIdentifier"])
	assert.Equal(t, at["collaborationId"], at["collaborationIdentifier"])
}

// TestParity_ProtectedQueryInitialStatusIsSubmitted verifies that newly started
// protected queries have status "SUBMITTED" (not "STARTED").
func TestParity_ProtectedQueryInitialStatusIsSubmitted(t *testing.T) {
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

// TestParity_ProtectedQueryHasMembershipID verifies the canonical "membershipId" key.
func TestParity_ProtectedQueryHasMembershipID(t *testing.T) {
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

// TestParity_CollaborationARNFormat verifies ARN format for collaborations.
func TestParity_CollaborationARNFormat(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "arn-test", "creatorDisplayName": "Grace",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	collab := resp["collaboration"].(map[string]any)

	arn, ok := collab["arn"].(string)
	require.True(t, ok, "collaboration must have 'arn' field")
	assert.Contains(t, arn, "arn:aws:cleanrooms:")
	assert.Contains(t, arn, ":collaboration/")
}

// TestParity_MembershipARNFormat verifies ARN format for memberships.
func TestParity_MembershipARNFormat(t *testing.T) {
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

// TestParity_CollaborationQueryLogStatusRoundtrip verifies queryLogStatus roundtrip.
func TestParity_CollaborationQueryLogStatusRoundtrip(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "qls-test", "creatorDisplayName": "Iris",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "ENABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	collab := resp["collaboration"].(map[string]any)
	assert.Equal(t, "ENABLED", collab["queryLogStatus"])
}

// TestParity_CollaborationCreatorMemberAbilities verifies creator abilities roundtrip.
func TestParity_CollaborationCreatorMemberAbilities(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "abilities-test", "creatorDisplayName": "Jake",
		"creatorMemberAbilities": []string{"CAN_QUERY", "CAN_RECEIVE_RESULTS"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	collab := resp["collaboration"].(map[string]any)
	abilities, ok := collab["memberAbilities"].([]any)
	assert.True(t, ok, "collaboration must have memberAbilities")
	assert.Len(t, abilities, 2)
}

// TestParity_ConfiguredTableAnalysisRuleHasConfiguredTableID verifies the
// canonical "configuredTableId" key on ConfiguredTableAnalysisRule.
func TestParity_ConfiguredTableAnalysisRuleHasConfiguredTableID(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/configuredTables", map[string]any{
		"name": "ar-table", "allowedColumns": []string{"col1"},
		"analysisMethod": "DIRECT_QUERY",
		"tableReference": map[string]any{"glue": map[string]any{
			"databaseName": "db", "tableName": "tbl",
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var ctResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ctResp))
	ctID := ctResp["configuredTable"].(map[string]any)["id"].(string)

	// Create analysis rule
	rec2 := doRequest(t, e, "POST", "/configuredTables/"+ctID+"/analysisRule", map[string]any{
		"type":   "LIST",
		"policy": map[string]any{"v1": map[string]any{"list": map[string]any{}}},
	})
	require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	ar := resp["analysisRule"].(map[string]any)
	assert.Contains(t, ar, "configuredTableId",
		"analysisRule must have canonical 'configuredTableId' key")
	assert.Equal(t, ctID, ar["configuredTableId"])
}

// TestParity_TagsLifecycle verifies tag/untag/list on a collaboration ARN.
func TestParity_TagsLifecycle(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "tag-collab", "creatorDisplayName": "Kate",
		"creatorMemberAbilities": []string{},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var colResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	arn := colResp["collaboration"].(map[string]any)["arn"].(string)

	// Tag
	recTag := doRequest(t, e, "POST", "/tags/"+arn, map[string]any{
		"tags": map[string]string{"env": "prod", "team": "data"},
	})
	require.Equal(t, http.StatusOK, recTag.Code)

	// List
	recList := doRequest(t, e, "GET", "/tags/"+arn, nil)
	require.Equal(t, http.StatusOK, recList.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listResp))
	tags := listResp["tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "data", tags["team"])

	// Untag
	recUntag := doRequest(t, e, "DELETE", "/tags/"+arn+"?tagKeys=env", nil)
	require.Equal(t, http.StatusOK, recUntag.Code)

	// Verify removal
	recList2 := doRequest(t, e, "GET", "/tags/"+arn, nil)
	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listResp2))
	tags2 := listResp2["tags"].(map[string]any)
	assert.NotContains(t, tags2, "env")
	assert.Equal(t, "data", tags2["team"])
}

// TestParity_ListMembershipsReturnsSummaryWithIDKeys verifies that
// ListMemberships responses include the canonical "id" and "collaborationId" keys.
func TestParity_ListMembershipsReturnsSummaryWithIDKeys(t *testing.T) {
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
