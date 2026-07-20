package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfiguredTableAssociations_Handlers(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)

	// Create a collab to get an ID
	res := doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "collab-cta", "creatorDisplayName": "Me",
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

	rec3 := doRequest(t, e, "POST", "/configuredTables", map[string]any{
		"name": "ct-test", "description": "desc",
		"tableReference": map[string]any{"glue": map[string]any{"databaseName": "db", "tableName": "t"}},
		"allowedColumns": []string{"id"}, "analysisMethod": "DIRECT_QUERY",
	})
	var ctResp map[string]any
	json.Unmarshal(rec3.Body.Bytes(), &ctResp)
	ctID := ctResp["configuredTable"].(map[string]any)["id"].(string)

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:   "CreateCTA",
			method: "POST",
			path:   "/memberships/" + memID + "/configuredTableAssociations",
			body: map[string]any{
				"name":                      "cta",
				"configuredTableIdentifier": ctID,
				"roleArn":                   "arn:aws:iam::123:role/foo",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetCTA NotFound",
			method:     "GET",
			path:       "/memberships/" + memID + "/configuredTableAssociations/invalid",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "ListCTAs",
			method:     "GET",
			path:       "/memberships/" + memID + "/configuredTableAssociations",
			body:       nil,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateCTA NotFound",
			method:     "PATCH",
			path:       "/memberships/" + memID + "/configuredTableAssociations/invalid",
			body:       map[string]any{"description": "desc"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteCTA NotFound",
			method:     "DELETE",
			path:       "/memberships/" + memID + "/configuredTableAssociations/invalid",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "CreateCTARule NotFound",
			method: "POST",
			path:   "/memberships/" + memID + "/configuredTableAssociations/invalid/analysisRule",
			body: map[string]any{
				"analysisRuleType":   "AGGREGATION",
				"analysisRulePolicy": map[string]any{"v1": map[string]any{}},
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetCTARule NotFound",
			method:     "GET",
			path:       "/memberships/" + memID + "/configuredTableAssociations/invalid/analysisRule/AGGREGATION",
			body:       nil,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateCTARule NotFound",
			method:     "PATCH",
			path:       "/memberships/" + memID + "/configuredTableAssociations/invalid/analysisRule/AGGREGATION",
			body:       map[string]any{"analysisRulePolicy": map[string]any{"v1": map[string]any{}}},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteCTARule NotFound",
			method:     "DELETE",
			path:       "/memberships/" + memID + "/configuredTableAssociations/invalid/analysisRule/AGGREGATION",
			body:       nil,
			wantStatus: http.StatusNotFound,
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
