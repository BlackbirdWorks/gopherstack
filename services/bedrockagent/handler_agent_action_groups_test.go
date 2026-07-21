package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerAgentActionGroups(t *testing.T) {
	t.Parallel()

	type tc struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}

	h, e := setupHandler(t)

	// Create an agent first to have an agentId
	agentBody := map[string]any{
		"agentName":            "ag-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}
	rAgent := doRequest(t, h, e, http.MethodPut, "/agents", agentBody)
	var agentResp map[string]map[string]any
	_ = json.Unmarshal(rAgent.Body.Bytes(), &agentResp)
	agentID := agentResp["agent"]["agentId"].(string)

	basePath := "/agents/" + agentID + "/agentversions/DRAFT/actiongroups"

	agBody := map[string]any{
		"actionGroupName": "test-ag",
		"actionGroupExecutor": map[string]any{
			"lambda": "arn:aws:lambda:us-east-1:123456789012:function:my-func",
		},
	}

	rAG := doRequest(t, h, e, http.MethodPut, basePath, agBody)
	var agResp map[string]map[string]any
	_ = json.Unmarshal(rAG.Body.Bytes(), &agResp)
	agID := agResp["agentActionGroup"]["actionGroupId"].(string)

	cases := []tc{
		{
			name:       "ListAgentActionGroups",
			method:     http.MethodGet,
			path:       basePath,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAgentActionGroup",
			method:     http.MethodGet,
			path:       basePath + "/" + agID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetAgentActionGroup_NotFound",
			method:     http.MethodGet,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateAgentActionGroup",
			method:     http.MethodPut,
			path:       basePath + "/" + agID,
			body:       agBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateAgentActionGroup_NotFound",
			method:     http.MethodPut,
			path:       basePath + "/notfound",
			body:       agBody,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeleteAgentActionGroup",
			method:     http.MethodDelete,
			path:       basePath + "/" + agID,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "DeleteAgentActionGroup_NotFound",
			method:     http.MethodDelete,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// need fresh setup for deletes so we don't interfere with other tests
			hLocal, eLocal := setupHandler(t)
			rA := doRequest(t, hLocal, eLocal, http.MethodPut, "/agents", agentBody)
			var aResp map[string]map[string]any
			_ = json.Unmarshal(rA.Body.Bytes(), &aResp)
			aID := aResp["agent"]["agentId"].(string)

			bP := "/agents/" + aID + "/agentversions/DRAFT/actiongroups"
			rAG2 := doRequest(t, hLocal, eLocal, http.MethodPut, bP, agBody)
			var ag2Resp map[string]map[string]any
			_ = json.Unmarshal(rAG2.Body.Bytes(), &ag2Resp)
			aGID := ag2Resp["agentActionGroup"]["actionGroupId"].(string)

			path := tt.path
			if agID != "" && aGID != "" {
				switch path {
				case basePath:
					path = bP
				case basePath + "/" + agID:
					path = bP + "/" + aGID
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
