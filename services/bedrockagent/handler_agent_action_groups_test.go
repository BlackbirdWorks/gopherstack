package bedrockagent_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// TestCreateAgentActionGroupVersion locks in real AWS's constraint on the
// CreateAgentActionGroup {agentVersion} URI path parameter: the real API
// reference documents it as Pattern: `DRAFT`, Length Constraints: Fixed
// length of 5 -- action groups can only be created against the mutable
// DRAFT version. A request path with any other value must fail validation
// rather than silently creating the action group under DRAFT anyway.
func TestCreateAgentActionGroupVersion(t *testing.T) {
	t.Parallel()

	t.Run("DRAFT succeeds and stores under DRAFT", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		agentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "ag-version-agent",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		if agentRec.Code != http.StatusOK {
			t.Fatalf("create agent: %d %s", agentRec.Code, agentRec.Body.String())
		}

		var agentResp map[string]map[string]any

		_ = json.Unmarshal(agentRec.Body.Bytes(), &agentResp, jsonx.V1())
		agentID, _ := agentResp["agent"]["agentId"].(string)

		rec := doRequest(t, h, e, http.MethodPut,
			"/agents/"+agentID+"/agentversions/DRAFT/actiongroups", map[string]any{
				"actionGroupName": "test-action-group",
			})
		if rec.Code != http.StatusOK {
			t.Fatalf("create action group: %d %s", rec.Code, rec.Body.String())
		}

		var resp map[string]map[string]any

		if err := json.Unmarshal(rec.Body.Bytes(), &resp, jsonx.V1()); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		if got := resp["agentActionGroup"]["agentVersion"]; got != "DRAFT" {
			t.Errorf("agentVersion = %v, want DRAFT", got)
		}
	})

	t.Run("non-DRAFT version rejected with ValidationException", func(t *testing.T) {
		t.Parallel()

		h, e := setupHandler(t)

		agentRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
			"agentName":            "ag-version-agent-2",
			"foundationModel":      "anthropic.claude-v2",
			"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		})
		if agentRec.Code != http.StatusOK {
			t.Fatalf("create agent: %d %s", agentRec.Code, agentRec.Body.String())
		}

		var agentResp map[string]map[string]any

		_ = json.Unmarshal(agentRec.Body.Bytes(), &agentResp, jsonx.V1())
		agentID, _ := agentResp["agent"]["agentId"].(string)

		rec := doRequest(t, h, e, http.MethodPut,
			"/agents/"+agentID+"/agentversions/1/actiongroups", map[string]any{
				"actionGroupName": "test-action-group",
			})
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("got %d want 400: %s", rec.Code, rec.Body.String())
		}

		if got := rec.Header().Get("X-Amzn-Errortype"); got != "ValidationException" {
			t.Errorf("X-Amzn-Errortype = %q, want ValidationException", got)
		}
	})
}
