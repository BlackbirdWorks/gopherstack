package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerAgentAliases(t *testing.T) {
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
		"agentName":            "alias-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}
	rAgent := doRequest(t, h, e, http.MethodPut, "/agents", agentBody)
	var agentResp map[string]map[string]any
	_ = json.Unmarshal(rAgent.Body.Bytes(), &agentResp)
	agentID := agentResp["agent"]["agentId"].(string)

	basePath := "/agents/" + agentID + "/agentaliases"

	aliasBody := map[string]any{
		"agentAliasName": "test-alias",
	}

	rAlias := doRequest(t, h, e, http.MethodPut, basePath, aliasBody)
	var aliasResp map[string]map[string]any
	_ = json.Unmarshal(rAlias.Body.Bytes(), &aliasResp)
	aliasID := aliasResp["agentAlias"]["agentAliasId"].(string)

	cases := []tc{
		{
			name:       "ListAgentAliases",
			method:     http.MethodGet,
			path:       basePath,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAgentAlias",
			method:     http.MethodGet,
			path:       basePath + "/" + aliasID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAgentAlias_NotFound",
			method:     http.MethodGet,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateAgentAlias",
			method:     http.MethodPut,
			path:       basePath + "/" + aliasID,
			body:       aliasBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateAgentAlias_NotFound",
			method:     http.MethodPut,
			path:       basePath + "/notfound",
			body:       aliasBody,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteAgentAlias",
			method:     http.MethodDelete,
			path:       basePath + "/" + aliasID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "DeleteAgentAlias_NotFound",
			method:     http.MethodDelete,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// fresh setup for deletes
			hLocal, eLocal := setupHandler(t)
			rA := doRequest(t, hLocal, eLocal, http.MethodPut, "/agents", agentBody)
			var aResp map[string]map[string]any
			_ = json.Unmarshal(rA.Body.Bytes(), &aResp)
			aID := aResp["agent"]["agentId"].(string)

			bP := "/agents/" + aID + "/agentaliases"
			rAlias2 := doRequest(t, hLocal, eLocal, http.MethodPut, bP, aliasBody)
			var alias2Resp map[string]map[string]any
			_ = json.Unmarshal(rAlias2.Body.Bytes(), &alias2Resp)
			aLID := alias2Resp["agentAlias"]["agentAliasId"].(string)

			path := tt.path
			if aliasID != "" && aLID != "" {
				switch path {
				case basePath:
					path = bP
				case basePath + "/" + aliasID:
					path = bP + "/" + aLID
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
