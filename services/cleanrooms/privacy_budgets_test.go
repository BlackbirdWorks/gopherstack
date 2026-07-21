package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP_PrivacyBudgetEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"HTTP_PrivacyBudgetEndpoints"},
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

			// Create PrivacyBudgetTemplate
			createRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/privacybudgettemplates", map[string]any{
				"privacyBudgetType": "DIFFERENTIAL_PRIVACY",
				"autoRefresh":       "CALENDAR_MONTH",
				"parameters": map[string]any{
					"differentialPrivacy": map[string]any{
						"epsilon":            1.0,
						"usersNoisePerQuery": 100,
					},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			var createResp map[string]map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			id := createResp["privacyBudgetTemplate"]["id"].(string)

			// Get
			getRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/privacybudgettemplates/"+id, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			// List
			listRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/privacybudgettemplates", nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			// Update
			upRec := doRequest(
				t,
				e,
				http.MethodPatch,
				"/memberships/"+mID+"/privacybudgettemplates/"+id,
				map[string]any{
					"autoRefresh": "NONE",
				},
			)
			require.Equal(t, http.StatusOK, upRec.Code)

			// PreviewPrivacyImpact
			previewRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/previewprivacyimpact", map[string]any{
				"parameters": map[string]any{
					"differentialPrivacy": map[string]any{
						"epsilon":            1.0,
						"usersNoisePerQuery": 100,
					},
				},
			})
			require.Equal(t, http.StatusOK, previewRec.Code)

			// ListPrivacyBudgets
			lbRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/privacybudgets", nil)
			require.Equal(t, http.StatusOK, lbRec.Code)

			// GetCollaborationPrivacyBudgetTemplate
			gctRec := doRequest(t, e, http.MethodGet, "/collaborations/"+colID+"/privacybudgettemplates/"+id, nil)
			assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, gctRec.Code)

			// ListCollaborationPrivacyBudgetTemplates
			lctRec := doRequest(t, e, http.MethodGet, "/collaborations/"+colID+"/privacybudgettemplates", nil)
			assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, lctRec.Code)

			// ListCollaborationPrivacyBudgets
			lcbRec := doRequest(t, e, http.MethodGet, "/collaborations/"+colID+"/privacybudgets", nil)
			assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, lcbRec.Code)

			// Delete
			delRec := doRequest(t, e, http.MethodDelete, "/memberships/"+mID+"/privacybudgettemplates/"+id, nil)
			require.Equal(t, http.StatusOK, delRec.Code)
		})
	}
}
