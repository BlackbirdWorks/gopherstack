package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerAgentVersionCRUD(t *testing.T) {
	t.Parallel()

	type tc struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}

	h, e := setupHandler(t)

	// Create an agent first
	agentBody := map[string]any{
		"agentName":            "ver-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}
	rAgent := doRequest(t, h, e, http.MethodPut, "/agents", agentBody)
	var agentResp map[string]map[string]any
	_ = json.Unmarshal(rAgent.Body.Bytes(), &agentResp)
	agentID := agentResp["agent"]["agentId"].(string)

	// Prepare agent to generate a version
	doRequest(t, h, e, http.MethodPost, "/agents/"+agentID+"/prepare", nil)

	// Create an alias without routing to trigger auto-version creation
	aliasBody := map[string]any{
		"agentAliasName": "prod",
	}
	rAlias := doRequest(t, h, e, http.MethodPut, "/agents/"+agentID+"/agentaliases", aliasBody)
	var aliasResp map[string]map[string]any
	_ = json.Unmarshal(rAlias.Body.Bytes(), &aliasResp)

	routing := aliasResp["agentAlias"]["routingConfiguration"].([]any)
	routingConfig := routing[0].(map[string]any)
	versionID := routingConfig["agentVersion"].(string)

	basePath := "/agents/" + agentID + "/agentversions"

	cases := []tc{
		{
			name:       "GetAgentVersion",
			method:     http.MethodGet,
			path:       basePath + "/" + versionID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAgentVersion_NotFound",
			method:     http.MethodGet,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteAgentVersion",
			method:     http.MethodDelete,
			path:       basePath + "/" + versionID,
			wantStatus: http.StatusOK, // returns 200 with status deleting
		},
		{
			name:       "DeleteAgentVersion_NotFound",
			method:     http.MethodDelete,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hLocal, eLocal := setupHandler(t)
			rA := doRequest(t, hLocal, eLocal, http.MethodPut, "/agents", agentBody)
			var aResp map[string]map[string]any
			_ = json.Unmarshal(rA.Body.Bytes(), &aResp)
			aID := aResp["agent"]["agentId"].(string)

			doRequest(t, hLocal, eLocal, http.MethodPost, "/agents/"+aID+"/prepare", nil)

			rAl := doRequest(t, hLocal, eLocal, http.MethodPut, "/agents/"+aID+"/agentaliases", aliasBody)
			var alResp map[string]map[string]any
			_ = json.Unmarshal(rAl.Body.Bytes(), &alResp)
			rt := alResp["agentAlias"]["routingConfiguration"].([]any)
			rtc := rt[0].(map[string]any)
			vID := rtc["agentVersion"].(string)

			bP := "/agents/" + aID + "/agentversions"

			path := tt.path
			if versionID != "" && vID != "" {
				switch path {
				case basePath + "/" + versionID:
					path = bP + "/" + vID
				case basePath + "/notfound":
					path = bP + "/notfound"
				}
			}
			r := doRequest(t, hLocal, eLocal, tt.method, path, tt.body)
			if r.Code != tt.wantStatus {
				t.Errorf("got %d want %d", r.Code, tt.wantStatus)
			}
		})
	}
}
