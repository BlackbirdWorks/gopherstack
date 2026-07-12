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

// TestParity_AgentVersionsPOST_IsListNotCreate pins the real AWS wire shape:
// bedrockagent has no CreateAgentVersion operation. POST to the
// /agents/{agentId}/agentversions collection path is ListAgentVersions (see
// the awsRestjson1_serializeOpListAgentVersions path/method in
// aws-sdk-go-v2/service/bedrockagent/serializers.go -- CreateAgentAlias is
// the only wire-visible way to create a numbered version, see
// TestParity_CreateAgentAlias_NoRoutingConfig_AutoCreatesVersion below).
func TestParity_AgentVersionsPOST_IsListNotCreate(t *testing.T) {
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

	rec := doRequest(t, h, e, http.MethodPost, "/agents/"+agentID+"/agentversions", nil)

	if rec.Code != http.StatusOK {
		t.Errorf("POST .../agentversions got %d, want 200 (ListAgentVersions, AWS spec)", rec.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if _, ok := resp["agentVersionSummaries"]; !ok {
		t.Errorf("response missing agentVersionSummaries (ListAgentVersions shape): %s", rec.Body.String())
	}
}

// TestParity_CreateAgentAlias_NoRoutingConfig_AutoCreatesVersion pins real
// AWS CreateAgentAlias behavior: when routingConfiguration is omitted,
// Amazon Bedrock automatically creates a new agent version and routes the
// alias to it.
func TestParity_CreateAgentAlias_NoRoutingConfig_AutoCreatesVersion(t *testing.T) {
	t.Parallel()

	h, e := newParitySetup(t)

	createRec := doRequest(t, h, e, http.MethodPut, "/agents", map[string]any{
		"agentName":            "autoversion-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	})

	var createResp map[string]map[string]any
	if err := json.Unmarshal(createRec.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	agentID, _ := createResp["agent"]["agentId"].(string)

	aliasRec := doRequest(t, h, e, http.MethodPut, "/agents/"+agentID+"/agentaliases", map[string]any{
		"agentAliasName": "prod",
	})

	if aliasRec.Code != http.StatusOK {
		t.Fatalf("create alias: got %d: %s", aliasRec.Code, aliasRec.Body.String())
	}

	var aliasResp map[string]map[string]any
	if err := json.Unmarshal(aliasRec.Body.Bytes(), &aliasResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	routing, _ := aliasResp["agentAlias"]["routingConfiguration"].([]any)
	if len(routing) != 1 {
		t.Fatalf("routingConfiguration = %v, want exactly 1 auto-created entry", routing)
	}

	listRec := doRequest(t, h, e, http.MethodGet, "/agents/"+agentID+"/agentversions", nil)

	var listResp map[string]any
	if err := json.Unmarshal(listRec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	summaries, _ := listResp["agentVersionSummaries"].([]any)
	if len(summaries) != 1 {
		t.Errorf("agentVersionSummaries = %v, want exactly 1 auto-created version", summaries)
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
