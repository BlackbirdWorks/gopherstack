package bedrock_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// sdkAgentRouteCases is the authoritative method+path for every real
// BedrockAgents operation -- AgentsHandler, the in-package sub-API this
// directory hosts to emulate the SEPARATE bedrock-agent.amazonaws.com wire
// shapes (distinct from services/bedrockagent, and registered as its own
// Registerable -- see AgentsHandler's doc comment in
// handler_agents_dispatch.go). Extracted from the pinned bedrockagent@v1.58.4
// client's serializers.go: each entry's "request.Method" and the string
// passed to httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for any
// {Param} URI label. Five bedrock-agent-shaped names AgentsHandler
// deliberately does NOT advertise (DeletePromptVersion, GetPromptVersion,
// ListPromptVersions, CreateAgentVersion, UpdateKnowledgeBaseDocuments) are
// correctly absent here too -- none of them exist as a HandleSerialize func
// in the real bedrockagent SDK either, confirming GetSupportedOperations's
// comment that they are not real wire operations.
//
// Regenerate by grepping bedrockagent's serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkAgentRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateAgentCollaborator", "PUT", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/"},
		{"AssociateAgentKnowledgeBase", "PUT", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/"},
		{"CreateAgent", "PUT", "/agents/"},
		{"CreateAgentActionGroup", "PUT", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/"},
		{"CreateAgentAlias", "PUT", "/agents/PLACEHOLDER/agentaliases/"},
		{"CreateDataSource", "PUT", "/knowledgebases/PLACEHOLDER/datasources/"},
		{"CreateFlow", "POST", "/flows/"},
		{"CreateFlowAlias", "POST", "/flows/PLACEHOLDER/aliases"},
		{"CreateFlowVersion", "POST", "/flows/PLACEHOLDER/versions"},
		{"CreateKnowledgeBase", "PUT", "/knowledgebases/"},
		{"CreatePrompt", "POST", "/prompts/"},
		{"CreatePromptVersion", "POST", "/prompts/PLACEHOLDER/versions"},
		{"DeleteAgent", "DELETE", "/agents/PLACEHOLDER/"},
		{"DeleteAgentActionGroup", "DELETE", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/PLACEHOLDER/"},
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
		{"GetAgentActionGroup", "GET", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/PLACEHOLDER/"},
		{"GetAgentAlias", "GET", "/agents/PLACEHOLDER/agentaliases/PLACEHOLDER/"},
		{
			"GetAgentCollaborator",
			"GET",
			"/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/PLACEHOLDER/",
		},
		{"GetAgentKnowledgeBase", "GET", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/PLACEHOLDER/"},
		{"GetAgentVersion", "GET", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/"},
		{"GetDataSource", "GET", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER"},
		{"GetFlow", "GET", "/flows/PLACEHOLDER/"},
		{"GetFlowAlias", "GET", "/flows/PLACEHOLDER/aliases/PLACEHOLDER"},
		{"GetFlowVersion", "GET", "/flows/PLACEHOLDER/versions/PLACEHOLDER/"},
		{"GetIngestionJob", "GET", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/PLACEHOLDER"},
		{"GetKnowledgeBase", "GET", "/knowledgebases/PLACEHOLDER"},
		{
			"GetKnowledgeBaseDocuments",
			"POST",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/documents/getDocuments",
		},
		{"GetPrompt", "GET", "/prompts/PLACEHOLDER/"},
		{"GetResourcePolicy", "GET", "/resourcepolicy/PLACEHOLDER"},
		{"IngestKnowledgeBaseDocuments", "PUT", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/documents"},
		{"ListAgentActionGroups", "POST", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/"},
		{"ListAgentAliases", "POST", "/agents/PLACEHOLDER/agentaliases/"},
		{"ListAgentCollaborators", "POST", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/agentcollaborators/"},
		{"ListAgentKnowledgeBases", "POST", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/knowledgebases/"},
		{"ListAgentVersions", "POST", "/agents/PLACEHOLDER/agentversions/"},
		{"ListAgents", "POST", "/agents/"},
		{"ListDataSources", "POST", "/knowledgebases/PLACEHOLDER/datasources/"},
		{"ListFlowAliases", "GET", "/flows/PLACEHOLDER/aliases"},
		{"ListFlowVersions", "GET", "/flows/PLACEHOLDER/versions"},
		{"ListFlows", "GET", "/flows/"},
		{"ListIngestionJobs", "POST", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/"},
		{"ListKnowledgeBaseDocuments", "POST", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/documents"},
		{"ListKnowledgeBases", "POST", "/knowledgebases/"},
		{"ListPrompts", "GET", "/prompts/"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"PrepareAgent", "POST", "/agents/PLACEHOLDER/"},
		{"PrepareFlow", "POST", "/flows/PLACEHOLDER/"},
		{"PutResourcePolicy", "PUT", "/resourcepolicy/PLACEHOLDER"},
		{"StartIngestionJob", "PUT", "/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/"},
		{
			"StopIngestionJob",
			"POST",
			"/knowledgebases/PLACEHOLDER/datasources/PLACEHOLDER/ingestionjobs/PLACEHOLDER/stop",
		},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateAgent", "PUT", "/agents/PLACEHOLDER/"},
		{"UpdateAgentActionGroup", "PUT", "/agents/PLACEHOLDER/agentversions/PLACEHOLDER/actiongroups/PLACEHOLDER/"},
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

// TestAgentsHandler_ExtractOperation_SDKRouteTable drives every real
// BedrockAgents op's authoritative method+path (see sdkAgentRouteCases)
// through AgentsHandler.ExtractOperation and asserts the route table
// resolves it to the right op, then drives the same request through the real
// AgentsHandler.Handler() and asserts it did not fall through to any of the
// "UnknownOperationException" dispatch-miss sentinels scattered across this
// package's dispatchXxxRoutes fallthroughs (handler_agents_dispatch.go and
// siblings) -- guarding against an op name that resolves correctly but has
// no matching case anywhere in the dispatch tree (gopherstack-ey26 class).
func TestAgentsHandler_ExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := bedrock.NewAgentsHandler(bedrock.NewInMemoryBackend("000000000000", "us-east-1"))

	for _, tc := range sdkAgentRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"method=%s path=%s op=%s: dispatched to the unmatched-route handler", tc.method, tc.path, tc.op)
		})
	}
}
