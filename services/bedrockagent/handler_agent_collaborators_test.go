package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerAgentCollaborators(t *testing.T) {
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
		"agentName":            "collab-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}
	rAgent := doRequest(t, h, e, http.MethodPut, "/agents", agentBody)
	var agentResp map[string]map[string]any
	_ = json.Unmarshal(rAgent.Body.Bytes(), &agentResp)
	agentID := agentResp["agent"]["agentId"].(string)

	basePath := "/agents/" + agentID + "/agentversions/DRAFT/agentcollaborators"

	collabBody := map[string]any{
		"collaboratorName": "test-collab",
		"agentDescriptor": map[string]any{
			"aliasArn": "arn:aws:bedrock:us-east-1:123456789012:agent-alias/ABC1234567/ALIAS12345",
		},
		"collaborationInstruction": "help the user",
		"relayConversationHistory": "TO_COLLABORATOR",
	}

	rCollab := doRequest(t, h, e, http.MethodPut, basePath, collabBody)
	var collabResp map[string]map[string]any
	_ = json.Unmarshal(rCollab.Body.Bytes(), &collabResp)
	collabID := collabResp["agentCollaborator"]["collaboratorId"].(string)

	cases := []tc{
		{
			name:       "ListCollaborators",
			method:     http.MethodGet,
			path:       basePath,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetCollaborator",
			method:     http.MethodGet,
			path:       basePath + "/" + collabID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetCollaborator_NotFound",
			method:     http.MethodGet,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdateCollaborator",
			method:     http.MethodPut,
			path:       basePath + "/" + collabID,
			body:       collabBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateCollaborator_NotFound",
			method:     http.MethodPut,
			path:       basePath + "/notfound",
			body:       collabBody,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DisassociateCollaborator",
			method:     http.MethodDelete,
			path:       basePath + "/" + collabID,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "DisassociateCollaborator_NotFound",
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

			bP := "/agents/" + aID + "/agentversions/DRAFT/agentcollaborators"
			rC := doRequest(t, hLocal, eLocal, http.MethodPut, bP, collabBody)
			var cResp map[string]map[string]any
			_ = json.Unmarshal(rC.Body.Bytes(), &cResp)
			cID := cResp["agentCollaborator"]["collaboratorId"].(string)

			path := tt.path
			if collabID != "" && cID != "" {
				switch path {
				case basePath:
					path = bP
				case basePath + "/" + collabID:
					path = bP + "/" + cID
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
