package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerAgentCRUD(t *testing.T) {
	t.Parallel()

	type tc struct {
		body           any
		name           string
		method         string
		path           string
		expectedStatus int
	}

	h, e := setupHandler(t)

	createBody := map[string]any{
		"agentName":            "test-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}

	rec := doRequest(t, h, e, http.MethodPut, "/agents", createBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("create agent got %d want 200: %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &createResp); err != nil {
		t.Fatalf("unmarshal create response: %v", err)
	}

	agentID, _ := createResp["agent"]["agentId"].(string)
	if agentID == "" {
		t.Fatal("no agentId in response")
	}

	cases := []tc{
		{name: "list agents", method: http.MethodGet, path: "/agents", expectedStatus: http.StatusOK},
		{name: "get agent", method: http.MethodGet, path: "/agents/" + agentID, expectedStatus: http.StatusOK},
		{
			name:   "update agent",
			method: http.MethodPut,
			path:   "/agents/" + agentID,
			body: map[string]any{
				"agentName":            "updated-agent",
				"foundationModel":      "anthropic.claude-v2",
				"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "prepare agent",
			method:         http.MethodPost,
			path:           "/agents/" + agentID + "/prepare",
			expectedStatus: http.StatusAccepted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			hLocal, eLocal := setupHandler(t)
			// pre-create the agent for each sub-test
			r := doRequest(t, hLocal, eLocal, http.MethodPut, "/agents", createBody)
			if r.Code != http.StatusOK {
				t.Fatalf("setup create: %d", r.Code)
			}

			var sr map[string]map[string]any
			_ = json.Unmarshal(r.Body.Bytes(), &sr)
			aid := sr["agent"]["agentId"].(string)

			path := tc.path
			if agentID != "" {
				// substitute if path contains our original agentID
				path = "/agents"
				switch tc.name {
				case "get agent", "update agent":
					path = "/agents/" + aid
				case "prepare agent":
					path = "/agents/" + aid + "/prepare"
				}
			}

			result := doRequest(t, hLocal, eLocal, tc.method, path, tc.body)
			if result.Code != tc.expectedStatus {
				t.Errorf("got %d want %d: %s", result.Code, tc.expectedStatus, result.Body.String())
			}
		})
	}
}

func TestHandlerAgentNotFound(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	rec := doRequest(t, h, e, http.MethodGet, "/agents/nonexistent", nil)
	if rec.Code != http.StatusNotFound {
		t.Errorf("got %d want 404", rec.Code)
	}
}

func TestHandlerAgentVersions(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"agentName":            "version-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}

	rec := doRequest(t, h, e, http.MethodPut, "/agents", createBody)

	var createResp map[string]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	agentID := createResp["agent"]["agentId"].(string)

	// Prepare first so we can create a version
	doRequest(t, h, e, http.MethodPost, "/agents/"+agentID+"/prepare", nil)

	// POST to the agentversions collection path is ListAgentVersions on the
	// real wire (there is no CreateAgentVersion SDK operation) -- it must
	// return 200 with agentVersionSummaries, not create anything.
	t.Run("list versions via POST", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/agents", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		aid := resp["agent"]["agentId"].(string)
		doRequest(t, h2, e2, http.MethodPost, "/agents/"+aid+"/prepare", nil)

		rec2 := doRequest(t, h2, e2, http.MethodPost, "/agents/"+aid+"/agentversions", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200: %s", rec2.Code, rec2.Body.String())
		}

		var listResp map[string]any
		_ = json.Unmarshal(rec2.Body.Bytes(), &listResp)

		if _, ok := listResp["agentVersionSummaries"]; !ok {
			t.Errorf("response missing agentVersionSummaries: %s", rec2.Body.String())
		}
	})

	t.Run("list versions", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/agents/"+agentID+"/agentversions", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})
}

func TestCreateAgent_DefaultIdleSessionTTL(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

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

func TestCreateAgent_CustomIdleSessionTTL(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

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

func TestUpdateAgent_IdleSessionTTL(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

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

// TestAgentVersionsPOST_IsListNotCreate pins the real AWS wire shape:
// bedrockagent has no CreateAgentVersion operation. POST to the
// /agents/{agentId}/agentversions collection path is ListAgentVersions (see
// the awsRestjson1_serializeOpListAgentVersions path/method in
// aws-sdk-go-v2/service/bedrockagent/serializers.go -- CreateAgentAlias is
// the only wire-visible way to create a numbered version, see
// TestCreateAgentAlias_NoRoutingConfig_AutoCreatesVersion below).
func TestAgentVersionsPOST_IsListNotCreate(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

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

// TestCreateAgentAlias_NoRoutingConfig_AutoCreatesVersion pins real
// AWS CreateAgentAlias behavior: when routingConfiguration is omitted,
// Amazon Bedrock automatically creates a new agent version and routes the
// alias to it.
func TestCreateAgentAlias_NoRoutingConfig_AutoCreatesVersion(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

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

func TestPrepareAgent_ResponseIncludesPreparedAt(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

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
