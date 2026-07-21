package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestProtectedJobs_Handlers(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)

	// Create a collab and membership
	res := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "collab-pj", "creatorDisplayName": "Me",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	json.Unmarshal(res.Body.Bytes(), &colResp)
	collabID := colResp["collaboration"].(map[string]any)["id"].(string)

	rec2 := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": collabID, "queryLogStatus": "DISABLED",
	})
	var memResp map[string]any
	json.Unmarshal(rec2.Body.Bytes(), &memResp)
	memID := memResp["membership"].(map[string]any)["id"].(string)

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "GetProtectedJob NotFound",
			method:     "GET",
			path:       "/memberships/" + memID + "/protectedJobs/invalid",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "ListProtectedJobs",
			method:     "GET",
			path:       "/memberships/" + memID + "/protectedJobs",
			body:       nil,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := doRequest(t, e, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, r.Code)
		})
	}
}
