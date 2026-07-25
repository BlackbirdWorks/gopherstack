package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestAssociateAgentKnowledgeBaseVersion locks in real AWS's constraint on
// the AssociateAgentKnowledgeBase {agentVersion} URI path parameter: the
// real API reference documents it as Pattern: `DRAFT`, Length Constraints:
// Fixed length of 5, matching CreateAgentActionGroup's constraint.
func TestAssociateAgentKnowledgeBaseVersion(t *testing.T) {
	t.Parallel()

	t.Run("DRAFT succeeds", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		agentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "agent-kb-version-agent",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		if agentRec.Code != http.StatusOK {
			t.Fatalf("create agent: %d %s", agentRec.Code, agentRec.Body.String())
		}

		var agentResp map[string]map[string]any

		_ = json.Unmarshal(agentRec.Body.Bytes(), &agentResp)
		agentID, _ := agentResp["agent"]["agentId"].(string)

		kbRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", map[string]any{
			"name":                       "agent-kb-version-kb",
			"roleArn":                    "arn:aws:iam::123456789012:role/KBRole",
			"knowledgeBaseConfiguration": map[string]any{"type": "VECTOR"},
			"storageConfiguration":       map[string]any{"type": "OPENSEARCH_SERVERLESS"},
		})
		if kbRec.Code != http.StatusOK {
			t.Fatalf("create kb: %d %s", kbRec.Code, kbRec.Body.String())
		}

		var kbResp map[string]map[string]any

		_ = json.Unmarshal(kbRec.Body.Bytes(), &kbResp)
		kbID, _ := kbResp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec := doRequest(t, h, e, http.MethodPut,
			"/agents/"+agentID+"/agentversions/DRAFT/knowledgebases", map[string]any{
				"knowledgeBaseId": kbID,
				"description":     "test association",
			})
		if rec.Code != http.StatusOK {
			t.Fatalf("associate kb: %d %s", rec.Code, rec.Body.String())
		}

		var resp map[string]map[string]any

		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if got := resp["agentKnowledgeBase"]["agentVersion"]; got != "DRAFT" {
			t.Errorf("agentVersion = %v, want DRAFT", got)
		}
	})

	t.Run("non-DRAFT version rejected with ValidationException", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		agentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "agent-kb-version-agent-2",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		if agentRec.Code != http.StatusOK {
			t.Fatalf("create agent: %d %s", agentRec.Code, agentRec.Body.String())
		}

		var agentResp map[string]map[string]any

		_ = json.Unmarshal(agentRec.Body.Bytes(), &agentResp)
		agentID, _ := agentResp["agent"]["agentId"].(string)

		kbRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", map[string]any{
			"name":                       "agent-kb-version-kb-2",
			"roleArn":                    "arn:aws:iam::123456789012:role/KBRole",
			"knowledgeBaseConfiguration": map[string]any{"type": "VECTOR"},
			"storageConfiguration":       map[string]any{"type": "OPENSEARCH_SERVERLESS"},
		})
		if kbRec.Code != http.StatusOK {
			t.Fatalf("create kb: %d %s", kbRec.Code, kbRec.Body.String())
		}

		var kbResp map[string]map[string]any

		_ = json.Unmarshal(kbRec.Body.Bytes(), &kbResp)
		kbID, _ := kbResp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec := doRequest(t, h, e, http.MethodPut,
			"/agents/"+agentID+"/agentversions/1/knowledgebases", map[string]any{
				"knowledgeBaseId": kbID,
				"description":     "test association",
			})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("got %d want 400: %s", rec.Code, rec.Body.String())
		}

		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Errorf("X-Amzn-Errortype = %q, want ValidationException", got)
		}
	})
}
