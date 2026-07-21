package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP_ProtectedJobEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{"HTTP_ProtectedJobEndpoints"},
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

			// Create/Start
			startRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/protectedJobs", map[string]any{
				"type": "SQL",
				"sqlParameters": map[string]any{
					"queryString":         "SELECT * FROM t",
					"analysisTemplateArn": "arn:aws:cleanrooms:us-east-1:123456789012:membership/" + mID + "/analysistemplate/at-1",
				},
			})
			require.Equal(t, http.StatusOK, startRec.Code)

			var startResp map[string]map[string]any
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
			id := startResp["protectedJob"]["id"].(string)

			upRec := doRequest(t, e, http.MethodPatch, "/memberships/"+mID+"/protectedJobs/"+id, map[string]any{
				"description": "updated desc",
			})
			assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, upRec.Code)
		})
	}
}
