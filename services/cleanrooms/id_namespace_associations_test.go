package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP_IDNamespaceAssociationEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"HTTP_IDNamespaceAssociationEndpoints"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// Create
			createRec := doRequest(
				t,
				e,
				http.MethodPost,
				"/memberships/"+mID+"/idnamespaceassociations",
				map[string]any{
					"name": "test-ns",
					"inputReferenceConfig": map[string]any{
						"inputReferenceArn":      "arn:aws:cleanrooms:us-east-1:123456789012:membership/" + mID,
						"manageResourcePolicies": true,
					},
				},
			)
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			id := createResp["idNamespaceAssociation"]["id"].(string)

			// Get
			getRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/idnamespaceassociations/"+id, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			// List
			listRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/idnamespaceassociations", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			// Update
			upRec := doRequest(
				t,
				e,
				http.MethodPatch,
				"/memberships/"+mID+"/idnamespaceassociations/"+id,
				map[string]any{
					"description": "updated desc",
				},
			)
			require.Equal(t, http.StatusOK, upRec.Code)

			// GetCollaborationIDNamespaceAssociation
			gCollab := doRequest(t, e, http.MethodGet, "/collaborations/"+colID+"/idnamespaceassociations/"+id, nil)
			assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, gCollab.Code)

			// ListCollaborationIDNamespaceAssociations
			lCollab := doRequest(t, e, http.MethodGet, "/collaborations/"+colID+"/idnamespaceassociations", nil)
			assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, lCollab.Code)

			// Delete
			delRec := doRequest(t, e, http.MethodDelete, "/memberships/"+mID+"/idnamespaceassociations/"+id, nil)
			require.Equal(t, http.StatusOK, delRec.Code)
		})
	}
}
