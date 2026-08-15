package bedrockagent_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real BedrockAgent
// operation, extracted from bedrockagent@v1.58.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands
// in for any {agentId}/{knowledgeBaseId}/{flowIdentifier}/{...} URI label --
// classifyPath (handler_helpers.go) strips a trailing slash before matching,
// so the router does not care about ID shape, only that the path matches
// Op. 75 real ops here, matching bedrockagent's real op count exactly.
//
// This service's static templates keep AWS's literal trailing slash after
// several path labels (e.g. "/agents/{agentId}/", "/flows/{flowIdentifier}/
// versions/{flowVersion}/") -- kept verbatim here since classifyPath's own
// first line (strings.TrimSuffix(path, "/")) makes the trailing slash
// immaterial to routing either way, so this table matches the SDK's own
// template rather than second-guessing it.
//
// A systematic check for a shared method+path across all 75 ops found zero
// collisions, so no *required dynamic* (non-template) member -- the
// s3/glacier vacuity-trap class -- was needed to disambiguate any route in
// this table. Several path families deliberately overload one collection
// path across a Create-style PUT and a List-style POST (e.g. "/agents/
// {agentId}/agentversions/{agentVersion}/actiongroups/" serves both
// CreateAgentActionGroup and ListAgentActionGroups), each already
// distinguished purely by method in dispatchActionGroups and its siblings
// (handler.go) -- kept as separate cases here so a future method-handling
// regression in any of those dispatch* functions is caught directly.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{
			"AssociateAgentCollaborator",
			"PUT",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/",
		},
		{
			"AssociateAgentKnowledgeBase",
			"PUT",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/",
		},
		{"CreateAgent", "PUT", "/agents/"},
		{
			"CreateAgentActionGroup",
			"PUT",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/",
		},
		{"CreateAgentAlias", "PUT", "/agents/PLACEHOLDER/agentaliases/"},
		{"CreateDataSource", "PUT", "/knowledgebases/PLACEHOLDER/datasources/"},
		{"CreateFlow", "POST", "/flows/"},
		{"CreateFlowAlias", "POST", "/flows/PLACEHOLDER/aliases"},
		{"CreateFlowVersion", "POST", "/flows/PLACEHOLDER/versions"},
		{"CreateKnowledgeBase", "PUT", "/knowledgebases/"},
		{"CreatePrompt", "POST", "/prompts/"},
		{"CreatePromptVersion", "POST", "/prompts/PLACEHOLDER/versions"},
		{"DeleteAgent", "DELETE", "/agents/PLACEHOLDER/"},
		{
			"DeleteAgentActionGroup",
			"DELETE",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/PLACEHOLDER/",
		},
		{"DeleteAgentAlias", "DELETE", "/agents/PLACEHOLDER/agentaliases/PLACEHOLDER/"},
		{"DeleteAgentVersion", "DELETE", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/"},
		{"DeleteDataSource", "DELETE", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER"},
		{"DeleteFlow", "DELETE", "/flows/PLACEHOLDER/"},
		{"DeleteFlowAlias", "DELETE", "/flows/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"DeleteFlowVersion", "DELETE", "/flows/PLACEHOLDER/versions/PLACEHOLDER/"},
		{"DeleteKnowledgeBase", "DELETE", "/knowledgebases/PLACEHOLDER"},
		{
			"DeleteKnowledgeBaseDocuments",
			"POST",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/documents/deleteDocuments",
		},
		{"DeletePrompt", "DELETE", "/prompts/PLACEHOLDER/"},
		{"DeleteResourcePolicy", "DELETE", "/resourcepolicy/PLACEHOLDER"},
		{
			"DisassociateAgentCollaborator",
			"DELETE",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/PLACEHOLDER/",
		},
		{
			"DisassociateAgentKnowledgeBase",
			"DELETE",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/PLACEHOLDER/",
		},
		{"GetAgent", "GET", "/agents/PLACEHOLDER/"},
		{
			"GetAgentActionGroup",
			"GET",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/PLACEHOLDER/",
		},
		{"GetAgentAlias", "GET", "/agents/PLACEHOLDER/agentaliases/PLACEHOLDER/"},
		{
			"GetAgentCollaborator",
			"GET",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/PLACEHOLDER/",
		},
		{
			"GetAgentKnowledgeBase",
			"GET",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/PLACEHOLDER/",
		},
		{"GetAgentVersion", "GET", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/"},
		{"GetDataSource", "GET", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER"},
		{"GetFlow", "GET", "/flows/PLACEHOLDER/"},
		{"GetFlowAlias", "GET", "/flows/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"GetFlowVersion", "GET", "/flows/PLACEHOLDER/versions/PLACEHOLDER/"},
		{
			"GetIngestionJob",
			"GET",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/PLACEHOLDER",
		},
		{"GetKnowledgeBase", "GET", "/knowledgebases/PLACEHOLDER"},
		{
			"GetKnowledgeBaseDocuments",
			"POST",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/documents/getDocuments",
		},
		{"GetPrompt", "GET", "/prompts/PLACEHOLDER/"},
		{"GetResourcePolicy", "GET", "/resourcepolicy/PLACEHOLDER"},
		{
			"IngestKnowledgeBaseDocuments",
			"PUT",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/documents",
		},
		{
			"ListAgentActionGroups",
			"POST",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/",
		},
		{"ListAgentAliases", "POST", "/agents/PLACEHOLDER/agentaliases/"},
		{
			"ListAgentCollaborators",
			"POST",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/",
		},
		{
			"ListAgentKnowledgeBases",
			"POST",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/",
		},
		{"ListAgentVersions", "POST", "/agents/PLACEHOLDER/agentversions/"},
		{"ListAgents", "POST", "/agents/"},
		{"ListDataSources", "POST", "/knowledgebases/PLACEHOLDER/datasources/"},
		{"ListFlowAliases", "GET", "/flows/PLACEHOLDER/aliases"},
		{"ListFlowVersions", "GET", "/flows/PLACEHOLDER/versions"},
		{"ListFlows", "GET", "/flows/"},
		{
			"ListIngestionJobs",
			"POST",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/",
		},
		{
			"ListKnowledgeBaseDocuments",
			"POST",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/documents",
		},
		{"ListKnowledgeBases", "POST", "/knowledgebases/"},
		{"ListPrompts", "GET", "/prompts/"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"PrepareAgent", "POST", "/agents/PLACEHOLDER/"},
		{"PrepareFlow", "POST", "/flows/PLACEHOLDER/"},
		{"PutResourcePolicy", "PUT", "/resourcepolicy/PLACEHOLDER"},
		{
			"StartIngestionJob",
			"PUT",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/",
		},
		{
			"StopIngestionJob",
			"POST",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/PLACEHOLDER/stop",
		},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAgent", "PUT", "/agents/PLACEHOLDER/"},
		{
			"UpdateAgentActionGroup",
			"PUT",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/PLACEHOLDER/",
		},
		{"UpdateAgentAlias", "PUT", "/agents/PLACEHOLDER/agentaliases/PLACEHOLDER/"},
		{
			"UpdateAgentCollaborator",
			"PUT",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/PLACEHOLDER/",
		},
		{
			"UpdateAgentKnowledgeBase",
			"PUT",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/PLACEHOLDER/",
		},
		{"UpdateDataSource", "PUT", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER"},
		{"UpdateFlow", "PUT", "/flows/PLACEHOLDER/"},
		{"UpdateFlowAlias", "PUT", "/flows/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"UpdateKnowledgeBase", "PUT", "/knowledgebases/PLACEHOLDER"},
		{"UpdatePrompt", "PUT", "/prompts/PLACEHOLDER/"},
		{"ValidateFlowDefinition", "POST", "/flows/validate-definition"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real BedrockAgent op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts classifyPath resolves it to the right op, all 75 ops against
// bedrockagent's real op count. It then drives the same request through the
// real Handler() and asserts the response does not carry the
// "UnknownOperationException" __type that every one of this service's 18
// dispatch-miss default cases (dispatch, dispatchAgentID,
// dispatchAgentVersionSuffix, dispatchActionGroups, dispatchCollaborators,
// dispatchAgentKBs, dispatchAgentAliases, dispatchKBID, dispatchDSID,
// dispatchIngestionJobs, dispatchKBDocuments, dispatchFlowID,
// dispatchFlowVersions, dispatchFlowAliases, dispatchPromptID,
// dispatchPromptVersions, and handler_resource_policy.go's own default, all
// in handler.go) emits under a variety of message tails ("unknown agent op",
// "unknown flow op", etc.) -- grepped across every non-test .go file in this
// package, "UnknownOperationException" appears only on these 18 miss
// branches, never on a domain error (which use ResourceNotFoundException/
// ConflictException/ValidationException/InternalServerException via
// handleErr), so it is a safe single sentinel for all of them.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h, _ := setupHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(
				t,
				rec.Body.String(),
				"UnknownOperationException",
				"method=%s path=%s op=%s: dispatched to an unmatched-route default",
				tc.method,
				tc.path,
				tc.op,
			)
		})
	}
}
