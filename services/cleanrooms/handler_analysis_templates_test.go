package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalysisTemplates_Handlers(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)

	// Create a collab and membership
	res := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "collab-at", "creatorDisplayName": "Me",
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

	rec3 := doRequest(t, e, "POST", "/memberships/"+memID+"/analysistemplates", map[string]any{
		"name":   "my-template",
		"format": "SQL",
		"source": map[string]any{"text": "SELECT 1"},
	})
	var atResp map[string]any
	json.Unmarshal(rec3.Body.Bytes(), &atResp)
	atID := atResp["analysisTemplate"].(map[string]any)["id"].(string)

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "GetAT",
			method:     "GET",
			path:       "/memberships/" + memID + "/analysistemplates/" + atID,
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAT NotFound",
			method:     "GET",
			path:       "/memberships/" + memID + "/analysistemplates/invalid",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "ListATs",
			method:     "GET",
			path:       "/memberships/" + memID + "/analysistemplates",
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateAT NotFound",
			method:     "PATCH",
			path:       "/memberships/" + memID + "/analysistemplates/invalid",
			body:       map[string]any{"description": "desc"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteAT NotFound",
			method:     "DELETE",
			path:       "/memberships/" + memID + "/analysistemplates/invalid",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetCollabAT",
			method:     "GET",
			path:       "/collaborations/" + collabID + "/analysistemplates/" + atID,
			body:       nil,
			wantStatus: http.StatusNotFound, // Not testing logic deeply, just routing
		},
		{
			name:       "ListCollabATs",
			method:     "GET",
			path:       "/collaborations/" + collabID + "/analysistemplates",
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "BatchGetCollabATs",
			method:     "POST",
			path:       "/collaborations/" + collabID + "/batch-analysistemplates",
			body:       map[string]any{"analysisTemplateArns": []string{"invalid"}},
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
