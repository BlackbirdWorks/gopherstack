package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollaborationWireShape verifies a Collaboration response uses only the
// real AWS keys, with one documented exception. Verified against
// aws-sdk-go-v2/service/cleanrooms's
// awsRestjson1_deserializeDocumentCollaboration: there is no
// "collaborationIdentifier", "memberAbilities", or "tags" key on this shape
// -- "collaborationIdentifier" is exclusively a *request* parameter name,
// and tags come only from ListTagsForResource. "members" is also not a real
// key (members come only from ListMembers) but is kept on the wire
// deliberately -- see the Collaboration.Members doc comment in models.go --
// since it is the only persisted backing store for ListMembers/DeleteMember
// and real AWS clients ignore unrecognized JSON fields.
func TestCollaborationWireShape(t *testing.T) {
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
	assert.True(t, hasID, "collaboration must have 'id' key (AWS canonical)")
	assert.NotEmpty(t, id)

	for _, invented := range []string{"collaborationIdentifier", "memberAbilities", "tags"} {
		_, present := collab[invented]
		assert.False(t, present, "collaboration must not have invented %q key", invented)
	}

	// Real AWS auto-creates a membership for the creator at CreateCollaboration
	// time, reflected as membershipArn/membershipId on the Collaboration.
	assert.NotEmpty(t, collab["membershipArn"])
	assert.NotEmpty(t, collab["membershipId"])
}

// TestCollaborationARNFormat verifies ARN format for collaborations.
func TestCollaborationARNFormat(t *testing.T) {
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

// TestCollaborationQueryLogStatusRoundtrip verifies queryLogStatus roundtrip.
func TestCollaborationQueryLogStatusRoundtrip(t *testing.T) {
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

// TestCollaborationCreatorMemberAbilities verifies creator abilities
// roundtrip. Real AWS has no top-level "memberAbilities" key on
// Collaboration (see TestCollaborationWireShape) -- abilities are per-member,
// so this checks the creator's entry in the (deliberately-kept, see
// Collaboration.Members doc comment) "members" array instead.
func TestCollaborationCreatorMemberAbilities(t *testing.T) {
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
	members, ok := collab["members"].([]any)
	require.True(t, ok, "collaboration must have members")
	require.Len(t, members, 1)
	creator := members[0].(map[string]any)
	abilities, ok := creator["abilities"].([]any)
	assert.True(t, ok, "creator member must have abilities")
	assert.Len(t, abilities, 2)
}

// TestDeleteMember_MarksRemoved verifies DeleteMember places the member in
// REMOVED status rather than deleting them from the collaboration's member
// list. Real AWS documents: "The removed member is placed in the Removed
// status and can't interact with the collaboration" (see
// api_op_DeleteMember.go's doc comment) -- ListMembers/GetCollaboration must
// still show the member afterward.
func TestDeleteMember_MarksRemoved(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "delete-member-test", "creatorDisplayName": "Owner",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members": []any{map[string]any{
			"accountId": "222222222222", "displayName": "Invitee",
			"memberAbilities": []string{"CAN_RECEIVE_RESULTS"},
		}},
		"queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	collabID := resp["collaboration"].(map[string]any)["id"].(string)

	del := doRequest(t, e, "DELETE", "/collaborations/"+collabID+"/member/222222222222", nil)
	require.Equal(t, http.StatusOK, del.Code, del.Body.String())

	listRec := doRequest(t, e, "GET", "/collaborations/"+collabID+"/members", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	members := listResp["memberSummaries"].([]any)
	require.Len(t, members, 2, "removed member must still appear in the member list")

	var removed map[string]any
	for _, m := range members {
		mm := m.(map[string]any)
		if mm["accountId"] == "222222222222" {
			removed = mm
		}
	}
	require.NotNil(t, removed, "removed member must still be present")
	assert.Equal(t, "REMOVED", removed["status"])
}

// TestDeleteCollaboration_CascadesMembershipToCollaborationDeleted verifies
// that deleting a collaboration transitions its active memberships to
// COLLABORATION_DELETED (a real MembershipStatus enum value) instead of
// deleting the membership rows -- GetMembership must keep working after the
// parent collaboration is gone (real AWS retains memberships for
// history/audit).
func TestDeleteCollaboration_CascadesMembershipToCollaborationDeleted(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "cascade-test", "creatorDisplayName": "Owner",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	collab := resp["collaboration"].(map[string]any)
	collabID := collab["id"].(string)
	membershipID := collab["membershipId"].(string)

	del := doRequest(t, e, "DELETE", "/collaborations/"+collabID, nil)
	require.Equal(t, http.StatusOK, del.Code, del.Body.String())

	getRec := doRequest(t, e, "GET", "/memberships/"+membershipID, nil)
	require.Equal(t, http.StatusOK, getRec.Code, "membership must still be retrievable after collaboration delete")
	var memResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &memResp))
	assert.Equal(t, "COLLABORATION_DELETED", memResp["membership"].(map[string]any)["status"])
}

// TestCollaborationChangeRequest_Lifecycle verifies the real
// Create/Update(action) state machine: PENDING -> APPROVE -> APPROVED ->
// COMMIT -> COMMITTED, and that re-approving a COMMITTED (terminal) request
// is rejected with ConflictException. Verified against
// CreateCollaborationChangeRequestInput (changes, not type/details) and
// UpdateCollaborationChangeRequestInput (action, not status).
func TestCollaborationChangeRequest_Lifecycle(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "change-request-test", "creatorDisplayName": "Owner",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	collabID := resp["collaboration"].(map[string]any)["id"].(string)

	createRec := doRequest(t, e, "POST", "/collaborations/"+collabID+"/changeRequests", map[string]any{
		"changes": []any{map[string]any{
			"specificationType": "MEMBER",
			"types":             []any{"ADD_MEMBER"},
			"specification": map[string]any{
				"member": map[string]any{
					"accountId":       "222222222222",
					"memberAbilities": []any{"CAN_QUERY"},
				},
			},
		}},
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())
	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cr := createResp["collaborationChangeRequest"].(map[string]any)
	assert.Equal(t, "PENDING", cr["status"])
	assert.NotEmpty(t, cr["changes"])
	crID := cr["id"].(string)

	approveRec := doRequest(
		t, e, "PATCH", "/collaborations/"+collabID+"/changeRequests/"+crID,
		map[string]any{"action": "APPROVE"},
	)
	require.Equal(t, http.StatusOK, approveRec.Code, approveRec.Body.String())
	var approveResp map[string]any
	require.NoError(t, json.Unmarshal(approveRec.Body.Bytes(), &approveResp))
	assert.Equal(t, "APPROVED", approveResp["collaborationChangeRequest"].(map[string]any)["status"])

	commitRec := doRequest(
		t, e, "PATCH", "/collaborations/"+collabID+"/changeRequests/"+crID,
		map[string]any{"action": "COMMIT"},
	)
	require.Equal(t, http.StatusOK, commitRec.Code, commitRec.Body.String())
	var commitResp map[string]any
	require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
	assert.Equal(t, "COMMITTED", commitResp["collaborationChangeRequest"].(map[string]any)["status"])

	// COMMIT applies the change's real semantic effect: the ADD_MEMBER change
	// must actually add the member to the collaboration, not just flip status.
	getRec := doRequest(t, e, "GET", "/collaborations/"+collabID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	members, _ := getResp["collaboration"].(map[string]any)["members"].([]any)
	var found bool
	for _, m := range members {
		if m.(map[string]any)["accountId"] == "222222222222" {
			found = true
		}
	}
	assert.True(t, found, "expected ADD_MEMBER change to add the new member on commit")

	// A COMMITTED request is terminal -- re-approving it must fail.
	conflictRec := doRequest(
		t, e, "PATCH", "/collaborations/"+collabID+"/changeRequests/"+crID,
		map[string]any{"action": "APPROVE"},
	)
	assert.Equal(t, http.StatusConflict, conflictRec.Code, conflictRec.Body.String())
}
