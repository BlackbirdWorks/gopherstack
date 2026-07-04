package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

func newParitySetup(t *testing.T) (*bedrockagent.Handler, *echo.Echo) {
	t.Helper()

	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")
	h := bedrockagent.NewTestHandler(b)
	h.AccountID = "123456789012"
	h.DefaultRegion = "us-east-1"

	return h, echo.New()
}

func TestParity_CreateAgent_DefaultIdleSessionTTL(t *testing.T) {
	t.Parallel()

	h, e := newParitySetup(t)

	rec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":            "ttl-default-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("create agent got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	ttl, _ := resp["agent"]["idleSessionTTLInSeconds"].(float64)
	if ttl != 600 {
		t.Errorf("idleSessionTTLInSeconds = %v, want 600 (AWS default)", ttl)
	}
}

func TestParity_CreateAgent_CustomIdleSessionTTL(t *testing.T) {
	t.Parallel()

	h, e := newParitySetup(t)

	rec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":               "ttl-custom-agent",
		"foundationModel":         "anthropic.claude-v2",
		"agentResourceRoleArn":    "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		"idleSessionTTLInSeconds": 1800,
	})

	if rec.Code != http.StatusOK {
		t.Fatalf("create agent got %d: %s", rec.Code, rec.Body.String())
	}

	var resp map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	ttl, _ := resp["agent"]["idleSessionTTLInSeconds"].(float64)
	if ttl != 1800 {
		t.Errorf("idleSessionTTLInSeconds = %v, want 1800", ttl)
	}
}

func TestParity_UpdateAgent_IdleSessionTTL(t *testing.T) {
	t.Parallel()

	h, e := newParitySetup(t)

	createRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":            "ttl-update-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	})

	var createResp map[string]map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	agentID, _ := createResp["agent"]["agentId"].(string)

	updateRec := doRequest(t, h, e, http.MethodPut, "/agents/"+agentID, map[string]any{
		"agentName":               "ttl-update-agent",
		"foundationModel":         "anthropic.claude-v2",
		"agentResourceRoleArn":    "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		"idleSessionTTLInSeconds": 3600,
	})

	if updateRec.Code != http.StatusOK {
		t.Fatalf("update agent got %d: %s", updateRec.Code, updateRec.Body.String())
	}

	var updateResp map[string]map[string]any
	if err := json.Unmarshal(updateRec.Body.Bytes(), &updateResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	ttl, _ := updateResp["agent"]["idleSessionTTLInSeconds"].(float64)
	if ttl != 3600 {
		t.Errorf("idleSessionTTLInSeconds after update = %v, want 3600", ttl)
	}
}

func TestParity_CreateAgentVersion_Returns202(t *testing.T) {
	t.Parallel()

	h, e := newParitySetup(t)

	createRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":            "version-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	})

	var createResp map[string]map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	agentID, _ := createResp["agent"]["agentId"].(string)

	rec := doRequest(t, h, e, http.MethodPost, "/agents/"+agentID+"/agentversions", map[string]any{
		"description": "v1",
	})

	if rec.Code != http.StatusAccepted {
		t.Errorf("CreateAgentVersion got %d, want 202 Accepted (AWS spec)", rec.Code)
	}
}

func TestParity_PrepareAgent_ResponseIncludesPreparedAt(t *testing.T) {
	t.Parallel()

	h, e := newParitySetup(t)

	createRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":            "prepare-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	})

	var createResp map[string]map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	agentID, _ := createResp["agent"]["agentId"].(string)

	rec := doRequest(t, h, e, http.MethodPost, "/agents/"+agentID+"/prepare", nil)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("PrepareAgent got %d, want 202", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if resp["preparedAt"] == nil {
		t.Errorf("PrepareAgent response missing preparedAt field (required by AWS API)")
	}

	if resp["agentId"] == nil {
		t.Errorf("PrepareAgent response missing agentId")
	}

	if resp["agentStatus"] == nil {
		t.Errorf("PrepareAgent response missing agentStatus")
	}

	if resp["agentVersion"] == nil {
		t.Errorf("PrepareAgent response missing agentVersion")
	}
}
