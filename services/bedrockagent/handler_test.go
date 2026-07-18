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
