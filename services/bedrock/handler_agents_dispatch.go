package bedrock

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
		opPrepareAgent,
		opCreateAgentActionGroup,
		opDeleteAgentActionGroup,
		opGetAgentActionGroup,
		opListAgentActionGroups,
		opUpdateAgentActionGroup,
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
		opGetAgentVersion,
		opListAgentVersions,
		opDeleteAgentVersion,
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
	// Op name constants shared between GetSupportedOperations and the
	// extract*Op/dispatch*Routes functions that mirror it, so the op-name
	// string literal isn't duplicated 3-4x across this file.
	opPrepareAgent           = "PrepareAgent"
	opCreateAgentActionGroup = "CreateAgentActionGroup"
	opDeleteAgentActionGroup = "DeleteAgentActionGroup"
	opGetAgentActionGroup    = "GetAgentActionGroup"
	opListAgentActionGroups  = "ListAgentActionGroups"
	opUpdateAgentActionGroup = "UpdateAgentActionGroup"
	opGetAgentVersion        = "GetAgentVersion"
	opListAgentVersions      = "ListAgentVersions"
	opDeleteAgentVersion     = "DeleteAgentVersion"

	// Sub-path suffixes shared across several op families, so the literal
	// isn't duplicated across this file and its sibling route files.
	suffixDataSources   = "/datasources"
	suffixIngestionJobs = "/ingestionjobs"
	suffixDocuments     = "/documents"
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

// ExtractOperation extracts the operation name from the request. Mirrors
// dispatch()'s own path handling (including its unconditional
// strings.TrimSuffix(path, "/")) and dispatch tree exactly, so this
// observability-only classifier never drifts from the real dispatch
// contract in Handler(). Previously this recognized only 10 of the 75 real
// bedrock-agent operations (found by gopherstack-n1mb's route table): every
// Get/Update/Delete/List op on a specific resource, plus the entire Flow,
// Prompt, Tag, memory, collaborator, data-source, ingestion-job, and
// document families, resolved to "Unknown" here even though Handler()
// already dispatched every one of them correctly.
func (h *AgentsHandler) ExtractOperation(c *echo.Context) string {
	path := strings.TrimSuffix(c.Request().URL.Path, "/")
	method := c.Request().Method

	if op, ok := extractAgentRoutesOp(path, method); ok {
		return op
	}

	if op, ok := extractKBRoutesOp(path, method); ok {
		return op
	}

	if op := extractAgentResourcePolicyOperation(path, method); op != "" {
		return op
	}

	if op, ok := extractFlowPromptTagOp(path, method); ok {
		return op
	}

	return "Unknown"
}

// extractFlowPromptTagOp mirrors dispatch()'s Flow/Prompt/Tag switch (batch-3).
func extractFlowPromptTagOp(path, method string) (string, bool) {
	switch {
	case strings.HasPrefix(path, flowsPath):
		return extractFlowOp(path, method)
	case strings.HasPrefix(path, promptsPath):
		return extractPromptOp(path, method)
	case strings.HasPrefix(path, "/tags/"):
		return extractTagOp(path, method)
	}

	return "", false
}

// extractAgentRoutesOp mirrors dispatchAgentRoutes.
func extractAgentRoutesOp(path, method string) (string, bool) {
	switch {
	case path == agentsPath && method == http.MethodPut:
		return "CreateAgent", true
	case path == agentsPath && (method == http.MethodPost || method == http.MethodGet):
		return "ListAgents", true
	}

	rest, ok := strings.CutPrefix(path, "/agents/")
	if !ok {
		return "", false
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	suffix := ""

	if len(parts) > splitInTwo-1 {
		suffix = "/" + parts[1]
	}

	return extractAgentIDRoutesOp(suffix, method)
}

// extractAgentIDRoutesOp mirrors dispatchAgentIDRoutes.
func extractAgentIDRoutesOp(suffix, method string) (string, bool) {
	if op, ok := extractAgentIDBareOp(suffix, method); ok {
		return op, true
	}

	if op, ok := extractAgentVersionSubRoutesOp(suffix, method); ok {
		return op, true
	}

	return extractAgentIDSubResourceOp(suffix, method)
}

// extractAgentIDBareOp mirrors dispatchAgentIDRoutes' first switch (the
// bare "/agents/{id}" path and its "/prepare" alias).
func extractAgentIDBareOp(suffix, method string) (string, bool) {
	switch {
	case suffix == "" && method == http.MethodGet:
		return "GetAgent", true
	case suffix == "" && method == http.MethodPut:
		return "UpdateAgent", true
	case suffix == "" && method == http.MethodDelete:
		return "DeleteAgent", true
	// PrepareAgent's real wire shape POSTs to this same bare path (see the
	// matching fix in dispatchAgentIDRoutes above); the "/prepare" suffix
	// case below is the non-canonical internal-test-only route.
	case suffix == "" && method == http.MethodPost:
		return opPrepareAgent, true
	case suffix == "/prepare" && method == http.MethodPost:
		return opPrepareAgent, true
	}

	return "", false
}

// extractAgentIDSubResourceOp mirrors dispatchAgentIDRoutes' second switch
// (action-groups, aliases, and both agent-version path shapes).
func extractAgentIDSubResourceOp(suffix, method string) (string, bool) {
	switch {
	case strings.HasPrefix(suffix, "/action-groups"):
		return extractActionGroupOp(suffix, method)
	case strings.HasPrefix(suffix, "/agentaliases"):
		return extractAliasOp(strings.Replace(suffix, "/agentaliases", suffixAgentAliases, 1), method)
	case strings.HasPrefix(suffix, suffixAgentAliases):
		return extractAliasOp(suffix, method)
	case strings.HasPrefix(suffix, "/agentversions"):
		return extractCanonicalAgentVersionOp(suffix, method)
	case strings.HasPrefix(suffix, suffixVersions):
		return extractAgentVersionOp(suffix, method)
	}

	return "", false
}

// extractAgentVersionSubRoutesOp mirrors dispatchAgentVersionSubRoutes.
func extractAgentVersionSubRoutesOp(suffix, method string) (string, bool) {
	switch {
	case strings.HasPrefix(suffix, "/agentversions/") && strings.Contains(suffix, "/knowledgebases"):
		return extractAgentKBAssocOp(suffix, method)
	case strings.HasPrefix(suffix, "/agentversions/") && strings.Contains(suffix, "/agentcollaborators"):
		return extractAgentCollabOp(suffix, method)
	case strings.HasPrefix(suffix, "/agentversions/") && strings.Contains(suffix, "/memories"):
		return extractMemoryOp(method)
	case strings.HasPrefix(suffix, "/agentversions/") && strings.Contains(suffix, "/actiongroups"):
		return extractCanonicalActionGroupOp(suffix, method)
	}

	return "", false
}

// extractAgentKBAssocOp mirrors dispatchAgentKBRoutes.
func extractAgentKBAssocOp(suffix, method string) (string, bool) {
	if strings.HasSuffix(suffix, "/knowledgebases") && method == http.MethodPut {
		return "AssociateAgentKnowledgeBase", true
	}

	if strings.HasSuffix(suffix, "/knowledgebases") && (method == http.MethodPost || method == http.MethodGet) {
		return "ListAgentKnowledgeBases", true
	}

	parts := strings.Split(suffix, "/knowledgebases/")
	if len(parts) == splitInTwo {
		switch method {
		case http.MethodGet:
			return "GetAgentKnowledgeBase", true
		case http.MethodPut:
			return "UpdateAgentKnowledgeBase", true
		case http.MethodDelete:
			return "DisassociateAgentKnowledgeBase", true
		}
	}

	return "", false
}

// extractAgentCollabOp mirrors dispatchAgentCollabRoutes.
func extractAgentCollabOp(suffix, method string) (string, bool) {
	collabSuffix := collabSuffixFrom(suffix)

	if collabSuffix == "/agentcollaborators" {
		switch method {
		case http.MethodPut:
			return "AssociateAgentCollaborator", true
		case http.MethodPost, http.MethodGet:
			return "ListAgentCollaborators", true
		}
	}

	if _, ok := strings.CutPrefix(collabSuffix, "/agentcollaborators/"); ok {
		switch method {
		case http.MethodGet:
			return "GetAgentCollaborator", true
		case http.MethodPut:
			return "UpdateAgentCollaborator", true
		case http.MethodDelete:
			return "DisassociateAgentCollaborator", true
		}
	}

	return "", false
}

// extractMemoryOp mirrors dispatchMemoryRoutes (method alone disambiguates;
// the sessionId query/path parsing there has no effect on the op name).
func extractMemoryOp(method string) (string, bool) {
	switch method {
	case http.MethodGet:
		return "GetAgentMemory", true
	case http.MethodDelete:
		return "DeleteAgentMemory", true
	}

	return "", false
}

// extractCanonicalActionGroupOp mirrors dispatchCanonicalActionGroupRoutes --
// the real bedrock-agent wire shape (.../agentversions/{v}/actiongroups/...).
func extractCanonicalActionGroupOp(suffix, method string) (string, bool) {
	_, rest, ok := strings.Cut(suffix, "/actiongroups")
	if !ok {
		return "", false
	}

	switch {
	case rest == "" && method == http.MethodPut:
		return opCreateAgentActionGroup, true
	case rest == "" && (method == http.MethodPost || method == http.MethodGet):
		return opListAgentActionGroups, true
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return opGetAgentActionGroup, true
	case strings.HasPrefix(rest, "/") && method == http.MethodPut:
		return opUpdateAgentActionGroup, true
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return opDeleteAgentActionGroup, true
	}

	return "", false
}

// extractActionGroupOp mirrors dispatchActionGroupRoutes -- the
// non-canonical "/action-groups" (hyphenated) internal-test-only route; no
// real bedrock-agent client ever sends this shape (see
// extractCanonicalActionGroupOp for the real one).
func extractActionGroupOp(suffix, method string) (string, bool) {
	if suffix == "/action-groups" {
		switch method {
		case http.MethodPost:
			return opCreateAgentActionGroup, true
		case http.MethodGet:
			return opListAgentActionGroups, true
		}
	}

	if strings.HasPrefix(suffix, "/action-groups/") {
		switch method {
		case http.MethodGet:
			return opGetAgentActionGroup, true
		case http.MethodPut:
			return opUpdateAgentActionGroup, true
		case http.MethodDelete:
			return opDeleteAgentActionGroup, true
		}
	}

	return "", false
}

// extractAliasOp mirrors dispatchAliasRoutes.
func extractAliasOp(suffix, method string) (string, bool) {
	if suffix == suffixAgentAliases && method == http.MethodPut {
		return "CreateAgentAlias", true
	}

	if suffix == suffixAgentAliases && (method == http.MethodPost || method == http.MethodGet) {
		return "ListAgentAliases", true
	}

	if _, ok := strings.CutPrefix(suffix, suffixAgentAliases+"/"); ok {
		switch method {
		case http.MethodGet:
			return "GetAgentAlias", true
		case http.MethodPut:
			return "UpdateAgentAlias", true
		case http.MethodDelete:
			return "DeleteAgentAlias", true
		}
	}

	return "", false
}

// extractCanonicalAgentVersionOp mirrors dispatchCanonicalAgentVersionRoutes.
func extractCanonicalAgentVersionOp(suffix, method string) (string, bool) {
	if suffix == "/agentversions" && (method == http.MethodPost || method == http.MethodGet) {
		return opListAgentVersions, true
	}

	version, ok := strings.CutPrefix(suffix, "/agentversions/")
	if !ok {
		return "", false
	}

	if version == agentStatusDraft && method == http.MethodPost {
		return opPrepareAgent, true
	}

	if method == http.MethodGet {
		return opGetAgentVersion, true
	}

	if method == http.MethodDelete {
		return opDeleteAgentVersion, true
	}

	return "", false
}

// extractAgentVersionOp mirrors the non-canonical dispatchAgentVersionRoutes
// (suffixVersions, not "/agentversions") -- internal-test-only, unreachable by
// a real client, which sends PrepareAgent to create a version instead (see
// dispatchAgentVersionRoutes's doc comment).
func extractAgentVersionOp(suffix, method string) (string, bool) {
	if suffix == suffixVersions && method == http.MethodGet {
		return opListAgentVersions, true
	}

	if suffix == suffixVersions && method == http.MethodPost {
		return "CreateAgentVersion", true
	}

	if _, ok := strings.CutPrefix(suffix, suffixVersions+"/"); ok {
		switch method {
		case http.MethodGet:
			return opGetAgentVersion, true
		case http.MethodDelete:
			return opDeleteAgentVersion, true
		}
	}

	return "", false
}

// extractKBRoutesOp mirrors dispatchKBRoutes.
func extractKBRoutesOp(path, method string) (string, bool) {
	switch {
	case path == knowledgeBasePath && method == http.MethodPut:
		return "CreateKnowledgeBase", true
	case path == knowledgeBasePath && (method == http.MethodPost || method == http.MethodGet):
		return "ListKnowledgeBases", true
	}

	rest, ok := strings.CutPrefix(path, "/knowledgebases/")
	if !ok {
		return "", false
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	suffix := ""

	if len(parts) > splitInTwo-1 {
		suffix = "/" + parts[1]
	}

	switch {
	case suffix == "" && method == http.MethodGet:
		return "GetKnowledgeBase", true
	case suffix == "" && method == http.MethodPut:
		return "UpdateKnowledgeBase", true
	case suffix == "" && method == http.MethodDelete:
		return "DeleteKnowledgeBase", true
	case strings.HasPrefix(suffix, suffixDataSources):
		return extractDataSourceOp(suffix, method)
	}

	return "", false
}

// extractDataSourceOp mirrors dispatchDataSourceRoutes.
func extractDataSourceOp(suffix, method string) (string, bool) {
	if suffix == suffixDataSources && method == http.MethodPut {
		return "CreateDataSource", true
	}

	if suffix == suffixDataSources && (method == http.MethodPost || method == http.MethodGet) {
		return "ListDataSources", true
	}

	rest, ok := strings.CutPrefix(suffix, "/datasources/")
	if !ok {
		return "", false
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	dsSuffix := ""

	if len(parts) > splitInTwo-1 {
		dsSuffix = "/" + parts[1]
	}

	return extractDataSourceIDOp(dsSuffix, method)
}

// extractDataSourceIDOp mirrors dispatchDataSourceIDRoutes.
func extractDataSourceIDOp(dsSuffix, method string) (string, bool) {
	switch {
	case dsSuffix == "" && method == http.MethodGet:
		return "GetDataSource", true
	case dsSuffix == "" && method == http.MethodPut:
		return "UpdateDataSource", true
	case dsSuffix == "" && method == http.MethodDelete:
		return "DeleteDataSource", true
	case strings.HasPrefix(dsSuffix, suffixIngestionJobs):
		return extractIngestionOp(dsSuffix, method)
	case strings.HasPrefix(dsSuffix, suffixDocuments):
		return extractDocumentOp(dsSuffix, method)
	}

	return "", false
}

// extractIngestionOp mirrors dispatchDataSourceIngestionRoutes.
func extractIngestionOp(dsSuffix, method string) (string, bool) {
	switch {
	case dsSuffix == suffixIngestionJobs && method == http.MethodPut:
		return "StartIngestionJob", true
	case dsSuffix == suffixIngestionJobs && (method == http.MethodPost || method == http.MethodGet):
		return "ListIngestionJobs", true
	case strings.HasPrefix(dsSuffix, "/ingestionjobs/"):
		return extractIngestionJobIDOp(dsSuffix, method)
	}

	return "", false
}

// extractIngestionJobIDOp mirrors dispatchIngestionJobRoutes.
func extractIngestionJobIDOp(dsSuffix, method string) (string, bool) {
	jobPath := strings.TrimPrefix(dsSuffix, "/ingestionjobs/")

	if idx := strings.Index(jobPath, "/"); idx >= 0 && jobPath[idx:] == "/stop" && method == http.MethodPost {
		return "StopIngestionJob", true
	}

	if method == http.MethodGet {
		return "GetIngestionJob", true
	}

	return "", false
}

// extractDocumentOp mirrors dispatchDataSourceDocumentRoutes/dispatchDocumentOps.
func extractDocumentOp(dsSuffix, method string) (string, bool) {
	switch {
	case dsSuffix == "/documents/getDocuments" && method == http.MethodPost:
		return "GetKnowledgeBaseDocuments", true
	case dsSuffix == "/documents/deleteDocuments" && method == http.MethodPost:
		return "DeleteKnowledgeBaseDocuments", true
	case dsSuffix == suffixDocuments && method == http.MethodPut:
		return "IngestKnowledgeBaseDocuments", true
	case dsSuffix == suffixDocuments && (method == http.MethodPost || method == http.MethodGet):
		return "ListKnowledgeBaseDocuments", true
	}

	return "", false
}

// extractFlowOp mirrors dispatchFlowRoutes.
func extractFlowOp(path, method string) (string, bool) {
	if path == flowsPath {
		switch method {
		case http.MethodPost, http.MethodPut:
			return "CreateFlow", true
		case http.MethodGet:
			return "ListFlows", true
		}
	}

	if (path == flowsPath+"/validateFlowDefinition" || path == flowsPath+"/validate-definition") &&
		method == http.MethodPost {
		return "ValidateFlowDefinition", true
	}

	rest, ok := strings.CutPrefix(path, "/flows/")
	if !ok {
		return "", false
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	suffix := ""

	if len(parts) == splitInTwo {
		suffix = "/" + parts[1]
	}

	return extractFlowIDOp(suffix, method)
}

// extractFlowIDOp mirrors dispatchFlowIDRoutes.
func extractFlowIDOp(suffix, method string) (string, bool) {
	switch {
	case suffix == "" && method == http.MethodGet:
		return "GetFlow", true
	case suffix == "" && method == http.MethodPut:
		return "UpdateFlow", true
	case suffix == "" && method == http.MethodDelete:
		return "DeleteFlow", true
	case suffix == "" && method == http.MethodPost:
		return "PrepareFlow", true
	case strings.HasPrefix(suffix, suffixAliases):
		return extractFlowAliasOp(suffix, method)
	case strings.HasPrefix(suffix, suffixVersions):
		return extractFlowVersionOp(suffix, method)
	}

	return "", false
}

// extractFlowAliasOp mirrors dispatchFlowAliasRoutes.
func extractFlowAliasOp(suffix, method string) (string, bool) {
	if suffix == suffixAliases {
		switch method {
		case http.MethodPost, http.MethodPut:
			return "CreateFlowAlias", true
		case http.MethodGet:
			return "ListFlowAliases", true
		}
	}

	if _, ok := strings.CutPrefix(suffix, suffixAliases+"/"); ok {
		switch method {
		case http.MethodGet:
			return "GetFlowAlias", true
		case http.MethodPut:
			return "UpdateFlowAlias", true
		case http.MethodDelete:
			return "DeleteFlowAlias", true
		}
	}

	return "", false
}

// extractFlowVersionOp mirrors dispatchFlowVersionRoutes.
func extractFlowVersionOp(suffix, method string) (string, bool) {
	if suffix == suffixVersions {
		switch method {
		case http.MethodPost, http.MethodPut:
			return "CreateFlowVersion", true
		case http.MethodGet:
			return "ListFlowVersions", true
		}
	}

	if _, ok := strings.CutPrefix(suffix, suffixVersions+"/"); ok {
		switch method {
		case http.MethodGet:
			return "GetFlowVersion", true
		case http.MethodDelete:
			return "DeleteFlowVersion", true
		}
	}

	return "", false
}

// extractPromptOp mirrors dispatchPromptRoutes.
func extractPromptOp(path, method string) (string, bool) {
	if path == promptsPath {
		switch method {
		case http.MethodPost, http.MethodPut:
			return "CreatePrompt", true
		case http.MethodGet:
			return "ListPrompts", true
		}
	}

	rest, ok := strings.CutPrefix(path, "/prompts/")
	if !ok {
		return "", false
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	suffix := ""

	if len(parts) == splitInTwo {
		suffix = "/" + parts[1]
	}

	return extractPromptIDOp(suffix, method)
}

// extractPromptIDOp mirrors dispatchPromptIDRoutes.
func extractPromptIDOp(suffix, method string) (string, bool) {
	switch {
	case suffix == "" && method == http.MethodGet:
		return "GetPrompt", true
	case suffix == "" && method == http.MethodPut:
		return "UpdatePrompt", true
	case suffix == "" && method == http.MethodDelete:
		return "DeletePrompt", true
	case strings.HasPrefix(suffix, suffixVersions):
		return extractPromptVersionOp(suffix, method)
	}

	return "", false
}

// extractPromptVersionOp mirrors dispatchPromptVersionRoutes's create-only
// case. GetPromptVersion/DeletePromptVersion/ListPromptVersions are
// deliberately left unclassified: per GetSupportedOperations's comment they
// are internal convenience routes only, not real bedrock-agent wire
// operations, so no real client request should ever resolve to them.
func extractPromptVersionOp(suffix, method string) (string, bool) {
	if suffix == suffixVersions && method == http.MethodPost {
		return "CreatePromptVersion", true
	}

	return "", false
}

// extractTagOp mirrors dispatchTagRoutes.
func extractTagOp(path, method string) (string, bool) {
	resourceArn, ok := strings.CutPrefix(path, "/tags/")
	if !ok || resourceArn == "" {
		return "", false
	}

	switch method {
	case http.MethodGet:
		return opListTagsForResource, true
	case http.MethodPost:
		return opTagResource, true
	case http.MethodDelete:
		return opUntagResource, true
	}

	return "", false
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
					agentErrResp("InternalServerException", "internal server error"),
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
	// ListAgents is real bedrock-agent@v1.58.4 serializers.go:4449: POST
	// /agents/, the SAME path+method family CreateAgent's PUT uses --
	// method alone disambiguates them, like PrepareAgent/PrepareFlow
	// elsewhere in this package. GET is accepted too as harmless extra
	// leniency for this package's own tests (no real client sends it).
	switch {
	case path == agentsPath && method == http.MethodPut:
		return true, h.handleCreateAgent(c, body)
	case path == agentsPath && (method == http.MethodPost || method == http.MethodGet):
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
	if handled, err := h.dispatchAgentIDBareRoutes(c, agentID, suffix, method, body); handled {
		return err
	}

	if handled, err := h.dispatchAgentVersionSubRoutes(c, agentID, suffix, method, body); handled {
		return err
	}

	if handled, err := h.dispatchAgentIDSubResourceRoutes(c, agentID, suffix, method, body); handled {
		return err
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown agent operation"),
	)
}

// dispatchAgentIDBareRoutes handles the bare "/agents/{id}" path and its
// "/prepare" alias. Returns (true, err) when the path was matched; (false,
// nil) when it was not.
func (h *AgentsHandler) dispatchAgentIDBareRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) (bool, error) {
	switch {
	case suffix == "" && method == http.MethodGet:
		return true, h.handleGetAgent(c, agentID)
	case suffix == "" && method == http.MethodPut:
		return true, h.handleUpdateAgent(c, agentID, body)
	case suffix == "" && method == http.MethodDelete:
		return true, h.handleDeleteAgent(c, agentID)
	// PrepareAgent POSTs to the same "/agents/{agentId}/" path as
	// Get/Update/Delete -- bedrockagent@v1.58.4 serializers.go:5419 has no
	// "/prepare" suffix; method alone disambiguates it, exactly like
	// PrepareFlow (see handler_flows.go's dispatchFlowIDRoutes). Found
	// unreachable by gopherstack-n1mb's route table: a real client's
	// PrepareAgent request fell through to "unknown agent operation"
	// because only the non-canonical "/prepare" suffix below was wired.
	case suffix == "" && method == http.MethodPost:
		return true, h.handlePrepareAgent(c, agentID)
	// Non-canonical "/prepare" suffix kept wired for this package's own
	// tests (handler_agents_test.go); unreachable by a real client, which
	// sends the suffix=="" case above instead.
	case suffix == "/prepare" && method == http.MethodPost:
		return true, h.handlePrepareAgent(c, agentID)
	}

	return false, nil
}

// dispatchAgentIDSubResourceRoutes handles action-groups, aliases, and both
// agent-version path shapes. Returns (true, err) when the path was matched;
// (false, nil) when it was not.
func (h *AgentsHandler) dispatchAgentIDSubResourceRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) (bool, error) {
	switch {
	case strings.HasPrefix(suffix, "/action-groups"):
		return true, h.dispatchActionGroupRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentaliases"):
		return true, h.dispatchAliasRoutes(
			c,
			agentID,
			strings.Replace(suffix, "/agentaliases", suffixAgentAliases, 1),
			method,
			body,
		)
	case strings.HasPrefix(suffix, suffixAgentAliases):
		return true, h.dispatchAliasRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentversions"):
		return true, h.dispatchCanonicalAgentVersionRoutes(c, agentID, suffix, method)
	case strings.HasPrefix(suffix, suffixVersions):
		return true, h.dispatchAgentVersionRoutes(c, agentID, suffix, method)
	}

	return false, nil
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
	// ListKnowledgeBases is real bedrock-agent@v1.58.4 serializers.go:5191:
	// POST /knowledgebases/, the SAME path+method family CreateKnowledgeBase's
	// PUT uses -- method alone disambiguates them. GET is accepted too as
	// harmless extra leniency for this package's own tests.
	switch {
	case path == knowledgeBasePath && method == http.MethodPut:
		return true, h.handleCreateKnowledgeBase(c, body)
	case path == knowledgeBasePath && (method == http.MethodPost || method == http.MethodGet):
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
	case strings.HasPrefix(suffix, suffixDataSources):
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

	respPromptVersion = "promptVersion"
	respAgentVersion  = "agentVersion"
	respCollaborator  = "agentCollaborator"

	// keyFlowID names the parent flow reference ("flowId") on FlowAlias
	// responses; a resource's own id is always the flat "id" key (keyID).
	keyFlowID         = "flowId"
	keyID             = "id"
	keyCollaboratorID = "collaboratorId"
	keyVersion        = "version"
	keyDefinitionHash = "definitionHash"

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
