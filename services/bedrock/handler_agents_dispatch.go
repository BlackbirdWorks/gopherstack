package bedrock

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/labstack/echo/v5"
)

// AgentsHandler handles Bedrock Agents API requests.
// The Bedrock Agents API lives at bedrock-agent.amazonaws.com (separate from
// the core bedrock API), so it is registered as its own Registerable.
type AgentsHandler struct {
	Backend *InMemoryBackend
}

// NewAgentsHandler creates a new Bedrock Agents handler.
func NewAgentsHandler(backend *InMemoryBackend) *AgentsHandler {
	return &AgentsHandler{Backend: backend}
}

// Name returns the service name.
func (h *AgentsHandler) Name() string { return "BedrockAgents" }

// GetSupportedOperations returns supported operations.
func (h *AgentsHandler) GetSupportedOperations() []string {
	return []string{
		// Existing agent ops
		"CreateAgent",
		"DeleteAgent",
		"GetAgent",
		"ListAgents",
		"UpdateAgent",
		"PrepareAgent",
		"CreateAgentActionGroup",
		"DeleteAgentActionGroup",
		"GetAgentActionGroup",
		"ListAgentActionGroups",
		"UpdateAgentActionGroup",
		"CreateAgentAlias",
		"DeleteAgentAlias",
		"GetAgentAlias",
		"ListAgentAliases",
		"UpdateAgentAlias",
		"AssociateAgentKnowledgeBase",
		"UpdateAgentKnowledgeBase",
		"DisassociateAgentKnowledgeBase",
		"GetAgentKnowledgeBase",
		"ListAgentKnowledgeBases",
		"CreateKnowledgeBase",
		"DeleteKnowledgeBase",
		"GetKnowledgeBase",
		"ListKnowledgeBases",
		"UpdateKnowledgeBase",
		"CreateDataSource",
		"DeleteDataSource",
		"GetDataSource",
		"ListDataSources",
		"UpdateDataSource",
		"StartIngestionJob",
		"GetIngestionJob",
		"ListIngestionJobs",
		// Flows
		"CreateFlow",
		"DeleteFlow",
		"GetFlow",
		"ListFlows",
		"UpdateFlow",
		"PrepareFlow",
		"CreateFlowAlias",
		"DeleteFlowAlias",
		"GetFlowAlias",
		"ListFlowAliases",
		"UpdateFlowAlias",
		"CreateFlowVersion",
		"DeleteFlowVersion",
		"GetFlowVersion",
		"ListFlowVersions",
		"ValidateFlowDefinition",
		// Prompts
		"CreatePrompt",
		"DeletePrompt",
		"GetPrompt",
		"ListPrompts",
		"UpdatePrompt",
		"CreatePromptVersion",
		// DeletePromptVersion, GetPromptVersion, and ListPromptVersions are deliberately
		// NOT advertised: they are not real bedrock-agent operations. The real client
		// gets/deletes a specific prompt version via GetPrompt/DeletePrompt's
		// promptVersion query parameter on the base /prompts/{id}/ path, and lists
		// versions via ListPrompts' promptIdentifier parameter -- there is no distinct
		// wire operation. The /prompts/{id}/versions[/{ver}] sub-paths handled by
		// dispatchPromptVersionRoutes (handler_prompt_versions.go) remain wired as an
		// internal convenience route (used by this package's own tests) but are
		// unreachable by any real bedrock-agent SDK client, which would never construct
		// that path.
		// Agent versions
		// CreateAgentVersion is deliberately NOT advertised: it is not a real
		// bedrock-agent operation. Real AWS creates a new agent version only as a side
		// effect of PrepareAgent (see dispatchCanonicalAgentVersionRoutes' POST
		// .../agentversions/DRAFT branch, which IS the real, advertised operation). The
		// non-canonical POST /agents/{id}/versions route (dispatchAgentVersionRoutes,
		// handler_agents.go) that dispatches to it remains wired for this package's own
		// tests but is unreachable by a real client, which sends PrepareAgent instead.
		"GetAgentVersion",
		"ListAgentVersions",
		"DeleteAgentVersion",
		// Agent collaborators
		"AssociateAgentCollaborator",
		"DisassociateAgentCollaborator",
		"GetAgentCollaborator",
		"ListAgentCollaborators",
		"UpdateAgentCollaborator",
		// Knowledge base documents
		"IngestKnowledgeBaseDocuments",
		"GetKnowledgeBaseDocuments",
		"DeleteKnowledgeBaseDocuments",
		"ListKnowledgeBaseDocuments",
		// UpdateKnowledgeBaseDocuments is deliberately NOT advertised: it is not a real
		// bedrock-agent operation. Real AWS's IngestKnowledgeBaseDocuments (PUT,
		// already advertised above) both adds and updates documents -- there is no
		// separate update call. dispatchDocumentOps (handler_knowledge_base_documents.go)
		// now routes PUT on the base .../documents path to real Ingest and POST (GET
		// too, as harmless leniency) to real List; the fabricated PUT-means-Update
		// handler (handleUpdateKBDocuments) and its backend method were deleted --
		// see PARITY.md for the fix history (previously SEVERE: List and Delete were
		// both unreachable via their real wire shape, silently hitting Ingest instead).
		// Ingestion job management
		"StopIngestionJob",
		// Resource tags (agent-domain)
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		// Agent memory: GetAgentMemory/DeleteAgentMemory ARE real AWS operations, but on
		// bedrock-agent-runtime (a separate, data-plane SDK client this repo does not
		// vendor as its own service) rather than bedrock-agent (the control-plane
		// client sdk_completeness_test.go checks against here). They are correctly
		// implemented and wire-shape-routed (see dispatchMemoryRoutes,
		// /agents/{id}/agentversions/{v}/memories/...); the reverse sdkcheck flags them
		// as phantom only because it can't see the runtime client, not because they are
		// fabricated.
		"GetAgentMemory",
		"DeleteAgentMemory",
		// parity-4: knowledge-base resource policies, added by the
		// aws-sdk-go-v2/service/bedrockagent bump (see PARITY.md). A
		// distinct operation family from core bedrock's own
		// PutResourcePolicy/GetResourcePolicy/DeleteResourcePolicy -- see
		// resource_policy.go's package doc comment.
		opPutResourcePolicy,
		opGetResourcePolicy,
		opDeleteResourcePolicy,
	}
}

// ChaosServiceName returns the chaos service name.
func (h *AgentsHandler) ChaosServiceName() string { return "bedrock-agent" }

// ChaosOperations returns all supported operations.
func (h *AgentsHandler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the supported regions.
func (h *AgentsHandler) ChaosRegions() []string { return []string{h.Backend.region} }

// Reset clears all agent/kb state.
//
// registry.ResetAll empties every store.Table registered on this backend
// instance -- including the non-agent-domain (guardrails/models/...) tables
// registerAllTables also registers -- but AgentsHandler's own backend
// instance (see AgentsProvider.Init in provider.go) never receives core
// Bedrock mutations (RouteMatcher only matches /agents/ and /knowledgebases/
// paths), so those tables are always already empty here; resetting them is a
// no-op, not a behavior change.
func (h *AgentsHandler) Reset() {
	h.Backend.mu.Lock("AgentsHandler.Reset")
	defer h.Backend.mu.Unlock()

	h.Backend.registry.ResetAll()
	h.Backend.agentsByName = make(map[string]string)
	h.Backend.kbByName = make(map[string]string)
	h.Backend.flowsByName = make(map[string]string)
	h.Backend.flowVersionCounters = make(map[string]int)
	h.Backend.promptsByName = make(map[string]string)
	h.Backend.promptVersionCounters = make(map[string]int)
	h.Backend.agentVersionCounters = make(map[string]int)
	h.Backend.agentTags = make(map[string]map[string]string)
	h.Backend.agentMemory = make(map[string][]any)
	// Reset ID counters for deterministic IDs after reset.
	h.Backend.agentCounter = 0
	h.Backend.actionGroupCounter = 0
	h.Backend.agentAliasCounter = 0
	h.Backend.kbCounter = 0
	h.Backend.dataSourceCounter = 0
	h.Backend.flowCounter = 0
	h.Backend.flowAliasCounter = 0
	h.Backend.promptCounter = 0
	h.Backend.agentCollabCounter = 0
	// resourcePolicies (knowledge-base flavor, parity-4) is registered on
	// b.registry so registry.ResetAll above already clears its entries; only
	// its revision counter needs a manual reset here, matching every other
	// counter in this method.
	h.Backend.resourcePolicyRevisionCounter = 0
}

// Route path constants.
const (
	agentsPath        = "/agents"
	knowledgeBasePath = "/knowledgebases"

	// Response key constants.
	respAgent              = "agent"
	respAgentAlias         = "agentAlias"
	respAgentActionGroup   = "agentActionGroup"
	respAgentKnowledgeBase = "agentKnowledgeBase"
	respKnowledgeBase      = "knowledgeBase"
	respDataSource         = "dataSource"
	respIngestionJob       = "ingestionJob"

	// Status constants.
	statusDeleting = "DELETING"

	// Path split limit for two-part path parsing.
	splitInTwo = 2

	// JSON response key constants.
	keyAgentID         = "agentId"
	keyKnowledgeBaseID = "knowledgeBaseId"

	// Op name constants shared between BedrockAgents and Bedrock handlers.
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
	// ResourcePolicy op names: shared between the core bedrock and
	// bedrock-agent flavors (see resource_policy.go's package doc comment)
	// purely so the op-name string literal isn't duplicated 4x across files.
	opPutResourcePolicy    = "PutResourcePolicy"
	opGetResourcePolicy    = "GetResourcePolicy"
	opDeleteResourcePolicy = "DeleteResourcePolicy"

	// Agent sub-path suffixes.
	suffixAgentAliases = "/aliases"
	opAgentStatusKey   = "agentStatus"
)

// RouteMatcher returns a function matching Bedrock Agents requests.
func (h *AgentsHandler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/agents/") ||
			path == agentsPath ||
			strings.HasPrefix(path, "/knowledgebases/") ||
			path == knowledgeBasePath ||
			strings.HasPrefix(path, agentResourcePolicyPathPrefix) ||
			routeMatcherBatch3(path)
	}
}

// MatchPriority returns the routing priority.
func (h *AgentsHandler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the operation name from the request.
func (h *AgentsHandler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	if op := extractAgentLevelOperation(path, method); op != "" {
		return op
	}

	if op := extractAgentAliasOperation(path, method); op != "" {
		return op
	}

	if op := extractAgentKBAssociationOperation(path, method); op != "" {
		return op
	}

	if op := extractKBResourceOperation(path, method); op != "" {
		return op
	}

	if op := extractAgentResourcePolicyOperation(path, method); op != "" {
		return op
	}

	return "Unknown"
}

// extractAgentLevelOperation matches operations on /agents and its direct
// action-group sub-resource. Returns "" when the path does not match.
func extractAgentLevelOperation(path, method string) string {
	switch {
	case path == agentsPath && (method == http.MethodPost || method == http.MethodPut):
		return "CreateAgent"
	case path == agentsPath && method == http.MethodGet:
		return "ListAgents"
	case strings.HasSuffix(path, "/prepare") && method == http.MethodPost:
		return "PrepareAgent"
	case strings.HasSuffix(path, "/action-groups") && method == http.MethodPost:
		return "CreateAgentActionGroup"
	case strings.HasSuffix(path, "/action-groups") && method == http.MethodGet:
		return "ListAgentActionGroups"
	}

	return ""
}

// extractAgentAliasOperation matches operations on the agent alias sub-resource.
// Returns "" when the path does not match.
func extractAgentAliasOperation(path, method string) string {
	switch {
	case strings.HasSuffix(path, suffixAgentAliases) && method == http.MethodPost:
		return "CreateAgentAlias"
	case strings.HasSuffix(path, suffixAgentAliases) && method == http.MethodGet:
		return "ListAgentAliases"
	}

	return ""
}

// extractAgentKBAssociationOperation matches operations on the agent-version
// knowledge-base association sub-resource. Returns "" when the path does not match.
func extractAgentKBAssociationOperation(path, method string) string {
	switch {
	case strings.Contains(path, "/agentversions/") &&
		strings.Contains(path, "/knowledgebases") && method == http.MethodGet:
		return "ListAgentKnowledgeBases"
	case strings.Contains(path, "/agentversions/") &&
		strings.HasSuffix(path, "/knowledgebases") && method == http.MethodPut:
		return "AssociateAgentKnowledgeBase"
	}

	return ""
}

// extractKBResourceOperation matches operations on /knowledgebases and its
// data-source/ingestion-job sub-resources. Returns "" when the path does not match.
func extractKBResourceOperation(path, method string) string {
	switch {
	case path == knowledgeBasePath && (method == http.MethodPost || method == http.MethodPut):
		return "CreateKnowledgeBase"
	case path == knowledgeBasePath && method == http.MethodGet:
		return "ListKnowledgeBases"
	case strings.HasSuffix(path, "/datasources") && method == http.MethodPost:
		return "CreateDataSource"
	case strings.HasSuffix(path, "/datasources") && method == http.MethodGet:
		return "ListDataSources"
	case strings.HasSuffix(path, "/ingestionjobs") && method == http.MethodPost:
		return "StartIngestionJob"
	case strings.HasSuffix(path, "/ingestionjobs") && method == http.MethodGet:
		return "ListIngestionJobs"
	}

	return ""
}

// ExtractResource extracts a resource identifier from the request.
func (h *AgentsHandler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function.
func (h *AgentsHandler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		method := r.Method
		log := logger.Load(r.Context())

		var body []byte
		switch method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
			var err error

			body, err = httputils.ReadBody(r)
			if err != nil {
				log.ErrorContext(r.Context(), "bedrock-agent: failed to read body", "error", err)

				return c.JSON(
					http.StatusInternalServerError,
					agentErrResp("InternalFailure", "internal server error"),
				)
			}
		}

		return h.dispatch(c, path, method, body)
	}
}

// dispatch routes requests to the appropriate handler.
func (h *AgentsHandler) dispatch(c *echo.Context, path, method string, body []byte) error {
	path = strings.TrimSuffix(path, "/")

	if handled, err := h.dispatchAgentRoutes(c, path, method, body); handled {
		return err
	}

	if handled, err := h.dispatchKBRoutes(c, path, method, body); handled {
		return err
	}

	if handled, err := h.dispatchResourcePolicyRoutes(c, path, method, body); handled {
		return err
	}

	// Flow, Prompt, and Tag routes (batch-3)
	switch {
	case strings.HasPrefix(path, flowsPath):
		return h.dispatchFlowRoutes(c, path, method, body)
	case strings.HasPrefix(path, promptsPath):
		return h.dispatchPromptRoutes(c, path, method, body)
	case strings.HasPrefix(path, "/tags/"):
		return h.dispatchTagRoutes(c, path, method, body)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown operation: "+path),
	)
}

// dispatchAgentRoutes handles /agents and /agents/{agentId}/... routes.
// Returns (true, err) when the path was matched; (false, nil) when it was not.
func (h *AgentsHandler) dispatchAgentRoutes(
	c *echo.Context, path, method string, body []byte,
) (bool, error) {
	switch {
	case path == agentsPath && (method == http.MethodPost || method == http.MethodPut):
		return true, h.handleCreateAgent(c, body)
	case path == agentsPath && method == http.MethodGet:
		return true, h.handleListAgents(c)
	}

	rest, cutOK := strings.CutPrefix(path, "/agents/")
	if !cutOK {
		return false, nil
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	agentID := parts[0]
	suffix := ""

	if len(parts) > splitInTwo-1 {
		suffix = "/" + parts[1]
	}

	return true, h.dispatchAgentIDRoutes(c, agentID, suffix, method, body)
}

// dispatchAgentIDRoutes handles routes for a specific agent ID.
func (h *AgentsHandler) dispatchAgentIDRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetAgent(c, agentID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateAgent(c, agentID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteAgent(c, agentID)
	case suffix == "/prepare" && method == http.MethodPost:
		return h.handlePrepareAgent(c, agentID)
	}

	if handled, err := h.dispatchAgentVersionSubRoutes(c, agentID, suffix, method, body); handled {
		return err
	}

	switch {
	case strings.HasPrefix(suffix, "/action-groups"):
		return h.dispatchActionGroupRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentaliases"):
		return h.dispatchAliasRoutes(
			c,
			agentID,
			strings.Replace(suffix, "/agentaliases", suffixAgentAliases, 1),
			method,
			body,
		)
	case strings.HasPrefix(suffix, suffixAgentAliases):
		return h.dispatchAliasRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentversions"):
		return h.dispatchCanonicalAgentVersionRoutes(c, agentID, suffix, method)
	case strings.HasPrefix(suffix, "/versions"):
		return h.dispatchAgentVersionRoutes(c, agentID, suffix, method)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown agent operation"),
	)
}

// dispatchAgentVersionSubRoutes handles the /agentversions/{version}/... sub-resource
// routes (knowledge bases, collaborators, memories, action groups) nested under a
// specific agent. Returns (true, err) when the path was matched; (false, nil) otherwise.
func (h *AgentsHandler) dispatchAgentVersionSubRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) (bool, error) {
	switch {
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/knowledgebases"):
		return true, h.dispatchAgentKBRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/agentcollaborators"):
		return true, h.dispatchAgentCollabRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/memories"):
		return true, h.dispatchMemoryRoutes(c, agentID, suffix, method)
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/actiongroups"):
		return true, h.dispatchCanonicalActionGroupRoutes(c, agentID, suffix, method, body)
	}

	return false, nil
}

// dispatchKBRoutes handles /knowledgebases and /knowledgebases/{kbId}/... routes.
// Returns (true, err) when the path was matched; (false, nil) when it was not.
func (h *AgentsHandler) dispatchKBRoutes(
	c *echo.Context, path, method string, body []byte,
) (bool, error) {
	switch {
	case path == knowledgeBasePath && (method == http.MethodPost || method == http.MethodPut):
		return true, h.handleCreateKnowledgeBase(c, body)
	case path == knowledgeBasePath && method == http.MethodGet:
		return true, h.handleListKnowledgeBases(c)
	}

	rest, cutOK := strings.CutPrefix(path, "/knowledgebases/")
	if !cutOK {
		return false, nil
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	kbID := parts[0]
	suffix := ""

	if len(parts) > splitInTwo-1 {
		suffix = "/" + parts[1]
	}

	switch {
	case suffix == "" && method == http.MethodGet:
		return true, h.handleGetKnowledgeBase(c, kbID)
	case suffix == "" && method == http.MethodPut:
		return true, h.handleUpdateKnowledgeBase(c, kbID, body)
	case suffix == "" && method == http.MethodDelete:
		return true, h.handleDeleteKnowledgeBase(c, kbID)
	case strings.HasPrefix(suffix, "/datasources"):
		return true, h.dispatchDataSourceRoutes(c, kbID, suffix, method, body)
	}

	return true, c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown kb operation"),
	)
}

func agentErrResp(code, msg string) map[string]any {
	return map[string]any{
		"message": msg,
		"__type":  code,
	}
}

const (
	flowsPath   = "/flows"
	promptsPath = "/prompts"

	respFlow          = "flow"
	respFlowAlias     = "flowAlias"
	respFlowVersion   = "flowVersion"
	respPrompt        = "prompt"
	respPromptVersion = "promptVersion"
	respAgentVersion  = "agentVersion"
	respCollaborator  = "agentCollaborator"

	keyFlowID         = "flowId"
	keyFlowAliasID    = "flowAliasId"
	keyPromptID       = "promptId"
	keyCollaboratorID = "collaboratorId"
	keyVersion        = "version"

	suffixAliases  = "/aliases"
	suffixVersions = "/versions"

	// arnFieldCount is the number of colon-separated fields in a well-formed
	// AWS ARN: "arn:partition:service:region:account:resource".
	arnFieldCount = 6
)

// isAgentResourceKind reports whether kind is an ARN resource-type prefix
// AgentsHandler owns, taken from every arn.Build call reachable through this
// handler's dispatch tree (agents.go, agent_aliases.go, knowledge_bases.go,
// flows.go, flow_aliases.go, prompts.go). Real bedrock-agent resource ARNs
// all use the "bedrock" service segment, which core Bedrock's own Handler
// also uses for guardrails/custom models/etc, so claiming by resource kind
// (not service token) is required to avoid swallowing /tags/ routes that
// belong to that other handler.
func isAgentResourceKind(kind string) bool {
	switch kind {
	case "agent", "agent-alias", "knowledge-base", "flow", "prompt":
		return true
	default:
		return false
	}
}

// routeMatcherBatch3 returns true if the path matches any batch-3 routes.
// Called from the updated RouteMatcher.
func routeMatcherBatch3(path string) bool {
	if strings.HasPrefix(path, "/flows") || path == flowsPath {
		return true
	}
	if strings.HasPrefix(path, "/prompts") || path == promptsPath {
		return true
	}
	if rest, ok := strings.CutPrefix(path, "/tags/"); ok {
		return isBedrockAgentArn(rest)
	}

	return false
}

// isBedrockAgentArn reports whether arn names a resource kind AgentsHandler
// owns. FlowAlias ARNs (flow/{id}/alias/{aliasId}) are covered by the "flow"
// kind since they nest under the flow resource rather than using their own
// top-level prefix.
func isBedrockAgentArn(arn string) bool {
	parts := strings.SplitN(arn, ":", arnFieldCount)
	if len(parts) != arnFieldCount || parts[0] != "arn" || parts[2] != "bedrock" {
		return false
	}

	kind, _, _ := strings.Cut(parts[arnFieldCount-1], "/")

	return isAgentResourceKind(kind)
}
