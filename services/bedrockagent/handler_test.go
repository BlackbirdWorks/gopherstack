package bedrockagent_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

func setupHandler(t *testing.T) (*bedrockagent.Handler, *echo.Echo) {
	t.Helper()

	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")
	h := bedrockagent.NewTestHandler(b)
	h.AccountID = "123456789012"
	h.DefaultRegion = "us-east-1"

	e := echo.New()

	return h, e
}

func doRequest(
	t *testing.T, h *bedrockagent.Handler, e *echo.Echo, method, path string, body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.Handler()(c); err != nil {
		t.Logf("handler returned error: %v", err)
	}

	return rec
}

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

func TestHandlerKnowledgeBaseCRUD(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":    "test-kb",
		"roleArn": "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{
			"type": "VECTOR",
		},
		"storageConfiguration": map[string]any{
			"type": "OPENSEARCH_SERVERLESS",
		},
	}

	rec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", createBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("create kb: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	kbID := createResp["knowledgeBase"]["knowledgeBaseId"].(string)

	t.Run("get kb", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/knowledgebases", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodGet, "/knowledgebases/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("list kbs", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/knowledgebases", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("delete kb", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/knowledgebases", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["knowledgeBase"]["knowledgeBaseId"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodDelete, "/knowledgebases/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	_ = kbID
}

func TestHandlerFlowCRUD(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":             "test-flow",
		"executionRoleArn": "arn:aws:iam::123456789012:role/FlowRole",
		"definition": map[string]any{
			"nodes":       []any{},
			"connections": []any{},
		},
	}

	rec := doRequest(t, h, e, http.MethodPost, "/flows", createBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create flow: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	flowID, _ := createResp["id"].(string)

	if flowID == "" {
		t.Fatal("no id in flow response")
	}

	t.Run("get flow", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/flows", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodGet, "/flows/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("list flows", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/flows", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("prepare flow", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/flows", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodPost, "/flows/"+id+"/prepare", nil)
		if rec2.Code != http.StatusAccepted {
			t.Errorf("got %d want 202", rec2.Code)
		}
	})
}

func TestHandlerPromptCRUD(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":           "test-prompt",
		"defaultVariant": "v1",
		"variants": []any{
			map[string]any{
				"name":         "v1",
				"templateType": "TEXT",
			},
		},
	}

	rec := doRequest(t, h, e, http.MethodPost, "/prompts", createBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create prompt: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	promptID, _ := createResp["id"].(string)

	if promptID == "" {
		t.Fatal("no id in prompt response")
	}

	t.Run("get prompt", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/prompts", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodGet, "/prompts/"+id, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("list prompts", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/prompts", nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec2.Code)
		}
	})

	t.Run("create prompt version", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPost, "/prompts", createBody)

		var resp map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		id := resp["id"].(string)

		rec2 := doRequest(t, h2, e2, http.MethodPost, "/prompts/"+id+"/versions", map[string]any{
			"description": "v1",
		})
		if rec2.Code != http.StatusCreated {
			t.Errorf("got %d want 201: %s", rec2.Code, rec2.Body.String())
		}
	})
}

func TestHandlerTagging(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	createBody := map[string]any{
		"agentName":            "tagging-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}

	rec := doRequest(t, h, e, http.MethodPut, "/agents", createBody)

	var createResp map[string]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	arn := createResp["agent"]["agentArn"].(string)

	t.Run("tag resource", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodPost, "/tags/"+arn, map[string]any{
			"tags": map[string]string{"env": "test"},
		})
		if rec2.Code != http.StatusNoContent {
			t.Errorf("tag: got %d want 204", rec2.Code)
		}
	})

	t.Run("list tags", func(t *testing.T) {
		t.Parallel()

		rec2 := doRequest(t, h, e, http.MethodGet, "/tags/"+arn, nil)
		if rec2.Code != http.StatusOK {
			t.Errorf("list tags: got %d want 200", rec2.Code)
		}
	})
}

func TestHandlerDataSourceAndIngestion(t *testing.T) {
	t.Parallel()

	h, e := setupHandler(t)

	kbBody := map[string]any{
		"name":                       "ingestion-kb",
		"roleArn":                    "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{"type": "VECTOR"},
		"storageConfiguration":       map[string]any{"type": "OPENSEARCH_SERVERLESS"},
	}

	kbRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", kbBody)
	if kbRec.Code != http.StatusOK {
		t.Fatalf("create kb: %d", kbRec.Code)
	}

	var kbResp map[string]map[string]any
	_ = json.Unmarshal(kbRec.Body.Bytes(), &kbResp)
	kbID := kbResp["knowledgeBase"]["knowledgeBaseId"].(string)

	dsBody := map[string]any{
		"name":                    "test-ds",
		"dataSourceConfiguration": map[string]any{"type": "S3"},
	}

	dsRec := doRequest(t, h, e, http.MethodPut, "/knowledgebases/"+kbID+"/datasources", dsBody)
	if dsRec.Code != http.StatusOK {
		t.Fatalf("create ds: %d %s", dsRec.Code, dsRec.Body.String())
	}

	var dsResp map[string]map[string]any
	_ = json.Unmarshal(dsRec.Body.Bytes(), &dsResp)
	dsID := dsResp["dataSource"]["dataSourceId"].(string)

	t.Run("start ingestion job", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, e, http.MethodPut,
			"/knowledgebases/"+kbID+"/datasources/"+dsID+"/ingestionjobs", nil)
		if rec.Code != http.StatusAccepted {
			t.Errorf("got %d want 202: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("list ingestion jobs", func(t *testing.T) {
		t.Parallel()

		rec := doRequest(t, h, e, http.MethodGet,
			"/knowledgebases/"+kbID+"/datasources/"+dsID+"/ingestionjobs", nil)
		if rec.Code != http.StatusOK {
			t.Errorf("got %d want 200", rec.Code)
		}
	})
}

func TestHandlerClassifyPath(t *testing.T) {
	t.Parallel()

	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")
	h := bedrockagent.NewTestHandler(b)
	h.AccountID = "123456789012"
	h.DefaultRegion = "us-east-1"
	e := echo.New()

	cases := []struct {
		method string
		path   string
		wantOp string
	}{
		{http.MethodPut, "/agents", "CreateAgent"},
		{http.MethodGet, "/agents", "ListAgents"},
		{http.MethodGet, "/agents/abc123", "GetAgent"},
		{http.MethodDelete, "/agents/abc123", "DeleteAgent"},
		{http.MethodPut, "/knowledgebases", "CreateKnowledgeBase"},
		{http.MethodGet, "/knowledgebases", "ListKnowledgeBases"},
		{http.MethodPost, "/flows", "CreateFlow"},
		{http.MethodGet, "/flows", "ListFlows"},
		{http.MethodPost, "/prompts", "CreatePrompt"},
		{http.MethodGet, "/prompts", "ListPrompts"},
		{http.MethodGet, "/tags/arn:aws:bedrock:us-east-1::agent/abc", "ListTagsForResource"},
		{http.MethodPost, "/tags/arn:aws:bedrock:us-east-1::agent/abc", "TagResource"},
		{http.MethodDelete, "/tags/arn:aws:bedrock:us-east-1::agent/abc", "UntagResource"},
	}

	for _, tc := range cases {
		t.Run(tc.method+":"+tc.path, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			if got != tc.wantOp {
				t.Errorf("got %q want %q", got, tc.wantOp)
			}
		})
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

	t.Run("create version", func(t *testing.T) {
		t.Parallel()

		h2, e2 := setupHandler(t)
		r := doRequest(t, h2, e2, http.MethodPut, "/agents", createBody)

		var resp map[string]map[string]any
		_ = json.Unmarshal(r.Body.Bytes(), &resp)
		aid := resp["agent"]["agentId"].(string)
		doRequest(t, h2, e2, http.MethodPost, "/agents/"+aid+"/prepare", nil)

		rec2 := doRequest(t, h2, e2, http.MethodPost, "/agents/"+aid+"/agentversions", map[string]any{
			"description": "initial version",
		})
		if rec2.Code != http.StatusAccepted {
			t.Errorf("got %d want 202: %s", rec2.Code, rec2.Body.String())
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

func TestHandlerBackendReset(t *testing.T) {
	t.Parallel()

	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")
	h := bedrockagent.NewTestHandler(b)
	h.AccountID = "123456789012"
	h.DefaultRegion = "us-east-1"
	e := echo.New()

	createBody := map[string]any{
		"agentName":            "reset-agent",
		"foundationModel":      "anthropic.claude-v2",
		"agentResourceRoleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
	}

	doRequest(t, h, e, http.MethodPut, "/agents", createBody)

	ctx := context.Background()
	agents, _, err := b.ListAgents(ctx, 10, "")
	if err != nil {
		t.Fatal(err)
	}

	if len(agents) == 0 {
		t.Fatal("expected agent after create")
	}

	h.Reset()

	agents, _, err = b.ListAgents(ctx, 10, "")
	if err != nil {
		t.Fatal(err)
	}

	if len(agents) != 0 {
		t.Fatalf("expected empty after reset, got %d", len(agents))
	}
}
