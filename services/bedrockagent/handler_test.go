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

		{http.MethodGet, "/knowledgebases/1/datasources/2/documents", "ListKnowledgeBaseDocuments"},
		{http.MethodPost, "/knowledgebases/1/datasources/2/documents", "IngestKnowledgeBaseDocuments"},
		{http.MethodPost, "/knowledgebases/1/datasources/2/documents/getDocuments", "GetKnowledgeBaseDocuments"},
		{http.MethodPost, "/knowledgebases/1/datasources/2/documents/deleteDocuments", "DeleteKnowledgeBaseDocuments"},
		{http.MethodGet, "/knowledgebases/1", "GetKnowledgeBase"},
		{http.MethodPut, "/knowledgebases/1", "UpdateKnowledgeBase"},
		{http.MethodDelete, "/knowledgebases/1", "DeleteKnowledgeBase"},
		{http.MethodGet, "/knowledgebases/1/datasources/2/ingestionjobs", "ListIngestionJobs"},
		{http.MethodPut, "/knowledgebases/1/datasources/2/ingestionjobs", "StartIngestionJob"},
		{http.MethodGet, "/knowledgebases/1/datasources/2/ingestionjobs/3", "GetIngestionJob"},
		{http.MethodPost, "/knowledgebases/1/datasources/2/ingestionjobs/3/stop", "StopIngestionJob"},
		{http.MethodPut, "/knowledgebases/1/datasources", "CreateDataSource"},
		{http.MethodGet, "/knowledgebases/1/datasources", "ListDataSources"},
		{http.MethodGet, "/knowledgebases/1/datasources/2", "GetDataSource"},
		{http.MethodPut, "/knowledgebases/1/datasources/2", "UpdateDataSource"},
		{http.MethodDelete, "/knowledgebases/1/datasources/2", "DeleteDataSource"},
		{http.MethodGet, "/flows/1", "GetFlow"},
		{http.MethodPut, "/flows/1", "UpdateFlow"},
		{http.MethodDelete, "/flows/1", "DeleteFlow"},
		{http.MethodPost, "/flows/1/prepare", "PrepareFlow"},
		{http.MethodPost, "/flows/validate-definition", "ValidateFlowDefinition"},
		{http.MethodGet, "/flows/1/versions", "ListFlowVersions"},
		{http.MethodPost, "/flows/1/versions", "CreateFlowVersion"},
		{http.MethodGet, "/flows/1/versions/2", "GetFlowVersion"},
		{http.MethodDelete, "/flows/1/versions/2", "DeleteFlowVersion"},
		{http.MethodGet, "/flows/1/aliases", "ListFlowAliases"},
		{http.MethodPost, "/flows/1/aliases", "CreateFlowAlias"},
		{http.MethodGet, "/flows/1/aliases/2", "GetFlowAlias"},
		{http.MethodPut, "/flows/1/aliases/2", "UpdateFlowAlias"},
		{http.MethodDelete, "/flows/1/aliases/2", "DeleteFlowAlias"},
		{http.MethodGet, "/prompts/1", "GetPrompt"},
		{http.MethodPut, "/prompts/1", "UpdatePrompt"},
		{http.MethodDelete, "/prompts/1", "DeletePrompt"},
		{http.MethodGet, "/prompts/1/versions", "ListPromptVersions"},
		{http.MethodPost, "/prompts/1/versions", "CreatePromptVersion"},
		{http.MethodGet, "/prompts/1/versions/2", "GetPromptVersion"},
		{http.MethodDelete, "/prompts/1/versions/2", "DeletePromptVersion"},
		{http.MethodPut, "/agents/1", "UpdateAgent"},
		{http.MethodDelete, "/agents/1", "DeleteAgent"},
		{http.MethodPost, "/agents/1/prepare", "PrepareAgent"},
		{http.MethodGet, "/agents/1/agentversions", "ListAgentVersions"},
		{http.MethodPost, "/agents/1/agentversions", "CreateAgentVersion"},
		{http.MethodGet, "/agents/1/agentversions/2", "GetAgentVersion"},
		{http.MethodDelete, "/agents/1/agentversions/2", "DeleteAgentVersion"},
		{http.MethodGet, "/agents/1/agentversions/2/actiongroups", "ListAgentActionGroups"},
		{http.MethodPut, "/agents/1/agentversions/2/actiongroups", "CreateAgentActionGroup"},
		{http.MethodGet, "/agents/1/agentversions/2/actiongroups/3", "GetAgentActionGroup"},
		{http.MethodPut, "/agents/1/agentversions/2/actiongroups/3", "UpdateAgentActionGroup"},
		{http.MethodDelete, "/agents/1/agentversions/2/actiongroups/3", "DeleteAgentActionGroup"},
		{http.MethodGet, "/agents/1/agentaliases", "ListAgentAliases"},
		{http.MethodPost, "/agents/1/agentaliases", "CreateAgentAlias"},
		{http.MethodGet, "/agents/1/agentaliases/2", "GetAgentAlias"},
		{http.MethodPut, "/agents/1/agentaliases/2", "UpdateAgentAlias"},
		{http.MethodDelete, "/agents/1/agentaliases/2", "DeleteAgentAlias"},
		{http.MethodGet, "/agents/1/agentversions/2/collaborators", "ListAgentCollaborators"},
		{http.MethodPut, "/agents/1/agentversions/2/collaborators", "AssociateAgentCollaborator"},
		{http.MethodGet, "/agents/1/agentversions/2/collaborators/3", "GetAgentCollaborator"},
		{http.MethodPut, "/agents/1/agentversions/2/collaborators/3", "UpdateAgentCollaborator"},
		{http.MethodDelete, "/agents/1/agentversions/2/collaborators/3", "DisassociateAgentCollaborator"},
		{http.MethodGet, "/agents/1/agentversions/2/knowledgebases", "ListAgentKnowledgeBases"},
		{http.MethodPut, "/agents/1/agentversions/2/knowledgebases", "AssociateAgentKnowledgeBase"},
		{http.MethodGet, "/agents/1/agentversions/2/knowledgebases/3", "GetAgentKnowledgeBase"},
		{http.MethodPut, "/agents/1/agentversions/2/knowledgebases/3", "UpdateAgentKnowledgeBase"},
		{http.MethodDelete, "/agents/1/agentversions/2/knowledgebases/3", "DisassociateAgentKnowledgeBase"},
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
