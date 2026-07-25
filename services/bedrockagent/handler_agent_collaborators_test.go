package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestAssociateAgentCollaboratorVersion locks in real AWS's constraint on
// the AssociateAgentCollaborator {agentVersion} URI path parameter: the
// real API reference documents it as Pattern: `DRAFT`, Length Constraints:
// Fixed length of 5, matching CreateAgentActionGroup's constraint.
func TestAssociateAgentCollaboratorVersion(t *testing.T) {
	t.Parallel()

	t.Run("DRAFT succeeds", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		agentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "collab-version-agent",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		if agentRec.Code != http.StatusOK {
			t.Fatalf("create agent: %d %s", agentRec.Code, agentRec.Body.String())
		}

		collabAgentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "collab-version-collaborator",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		if collabAgentRec.Code != http.StatusOK {
			t.Fatalf("create collaborator agent: %d %s", collabAgentRec.Code, collabAgentRec.Body.String())
		}

		var agentResp map[string]map[string]any

		_ = json.Unmarshal(agentRec.Body.Bytes(), &agentResp)
		agentID, _ := agentResp["agent"]["agentId"].(string)

		rec := doRequest(t, h, e, http.MethodPut,
			"/agents/"+agentID+"/agentversions/DRAFT/agentcollaborators", map[string]any{
				"collaboratorName":         "test-collaborator",
				"collaborationInstruction": "help out",
				"agentDescriptor": map[string]any{
					"aliasArn": "arn:aws:bedrock:us-east-1:123456789012:agent-alias/AGENT1234X/ALIAS12345",
				},
			})
		if rec.Code != http.StatusOK {
			t.Fatalf("associate collaborator: %d %s", rec.Code, rec.Body.String())
		}

		var resp map[string]map[string]any

		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if got := resp["agentCollaborator"]["agentVersion"]; got != "DRAFT" {
			t.Errorf("agentVersion = %v, want DRAFT", got)
		}
	})

	t.Run("non-DRAFT version rejected with ValidationException", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		agentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "collab-version-agent-2",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		if agentRec.Code != http.StatusOK {
			t.Fatalf("create agent: %d %s", agentRec.Code, agentRec.Body.String())
		}

		var agentResp map[string]map[string]any

		_ = json.Unmarshal(agentRec.Body.Bytes(), &agentResp)
		agentID, _ := agentResp["agent"]["agentId"].(string)

		rec := doRequest(t, h, e, http.MethodPut,
			"/agents/"+agentID+"/agentversions/1/agentcollaborators", map[string]any{
				"collaboratorName":         "test-collaborator",
				"collaborationInstruction": "help out",
				"agentDescriptor": map[string]any{
					"aliasArn": "arn:aws:bedrock:us-east-1:123456789012:agent-alias/AGENT1234X/ALIAS12345",
				},
			})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("got %d want 400: %s", rec.Code, rec.Body.String())
		}

		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Errorf("X-Amzn-Errortype = %q, want ValidationException", got)
		}
	})
}
