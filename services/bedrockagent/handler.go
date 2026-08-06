package bedrockagent

import (
	"context"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ---------------------------------------------------------------------------
// Operation name constants
// ---------------------------------------------------------------------------

const (
	opCreateAgent                    = "CreateAgent"
	opGetAgent                       = "GetAgent"
	opUpdateAgent                    = "UpdateAgent"
	opDeleteAgent                    = "DeleteAgent"
	opListAgents                     = "ListAgents"
	opPrepareAgent                   = "PrepareAgent"
	opGetAgentVersion                = "GetAgentVersion"
	opDeleteAgentVersion             = "DeleteAgentVersion"
	opListAgentVersions              = "ListAgentVersions"
	opCreateAgentActionGroup         = "CreateAgentActionGroup"
	opGetAgentActionGroup            = "GetAgentActionGroup"
	opUpdateAgentActionGroup         = "UpdateAgentActionGroup"
	opDeleteAgentActionGroup         = "DeleteAgentActionGroup"
	opListAgentActionGroups          = "ListAgentActionGroups"
	opCreateAgentAlias               = "CreateAgentAlias"
	opGetAgentAlias                  = "GetAgentAlias"
	opUpdateAgentAlias               = "UpdateAgentAlias"
	opDeleteAgentAlias               = "DeleteAgentAlias"
	opListAgentAliases               = "ListAgentAliases"
	opAssociateAgentCollaborator     = "AssociateAgentCollaborator"
	opGetAgentCollaborator           = "GetAgentCollaborator"
	opUpdateAgentCollaborator        = "UpdateAgentCollaborator"
	opDisassociateAgentCollaborator  = "DisassociateAgentCollaborator"
	opListAgentCollaborators         = "ListAgentCollaborators"
	opCreateKnowledgeBase            = "CreateKnowledgeBase"
	opGetKnowledgeBase               = "GetKnowledgeBase"
	opUpdateKnowledgeBase            = "UpdateKnowledgeBase"
	opDeleteKnowledgeBase            = "DeleteKnowledgeBase"
	opListKnowledgeBases             = "ListKnowledgeBases"
	opAssociateAgentKnowledgeBase    = "AssociateAgentKnowledgeBase"
	opGetAgentKnowledgeBase          = "GetAgentKnowledgeBase"
	opUpdateAgentKnowledgeBase       = "UpdateAgentKnowledgeBase"
	opDisassociateAgentKnowledgeBase = "DisassociateAgentKnowledgeBase"
	opListAgentKnowledgeBases        = "ListAgentKnowledgeBases"
	opCreateDataSource               = "CreateDataSource"
	opGetDataSource                  = "GetDataSource"
	opUpdateDataSource               = "UpdateDataSource"
	opDeleteDataSource               = "DeleteDataSource"
	opListDataSources                = "ListDataSources"
	opStartIngestionJob              = "StartIngestionJob"
	opGetIngestionJob                = "GetIngestionJob"
	opStopIngestionJob               = "StopIngestionJob"
	opListIngestionJobs              = "ListIngestionJobs"
	opCreateFlow                     = "CreateFlow"
	opGetFlow                        = "GetFlow"
	opUpdateFlow                     = "UpdateFlow"
	opDeleteFlow                     = "DeleteFlow"
	opListFlows                      = "ListFlows"
	opPrepareFlow                    = "PrepareFlow"
	opValidateFlowDefinition         = "ValidateFlowDefinition"
	opCreateFlowVersion              = "CreateFlowVersion"
	opGetFlowVersion                 = "GetFlowVersion"
	opDeleteFlowVersion              = "DeleteFlowVersion"
	opListFlowVersions               = "ListFlowVersions"
	opCreateFlowAlias                = "CreateFlowAlias"
	opGetFlowAlias                   = "GetFlowAlias"
	opUpdateFlowAlias                = "UpdateFlowAlias"
	opDeleteFlowAlias                = "DeleteFlowAlias"
	opListFlowAliases                = "ListFlowAliases"
	opCreatePrompt                   = "CreatePrompt"
	opGetPrompt                      = "GetPrompt"
	opUpdatePrompt                   = "UpdatePrompt"
	opDeletePrompt                   = "DeletePrompt"
	opListPrompts                    = "ListPrompts"
	opCreatePromptVersion            = "CreatePromptVersion"
	// opGetPromptVersion/opDeletePromptVersion are used only for internal request
	// classification (classifyPromptVersionPath, handler_prompts.go) -- neither is a
	// real bedrock-agent operation name and neither is advertised via
	// GetSupportedOperations(); see the comment there.
	opGetPromptVersion             = "GetPromptVersion"
	opDeletePromptVersion          = "DeletePromptVersion"
	opIngestKnowledgeBaseDocuments = "IngestKnowledgeBaseDocuments"
	opGetKnowledgeBaseDocuments    = "GetKnowledgeBaseDocuments"
	opDeleteKnowledgeBaseDocuments = "DeleteKnowledgeBaseDocuments"
	opListKnowledgeBaseDocuments   = "ListKnowledgeBaseDocuments"
	opListTagsForResource          = "ListTagsForResource"
	opTagResource                  = "TagResource"
	opUntagResource                = "UntagResource"
	opPutResourcePolicy            = "PutResourcePolicy"
	opGetResourcePolicy            = "GetResourcePolicy"
	opDeleteResourcePolicy         = "DeleteResourcePolicy"
)

// ---------------------------------------------------------------------------
// Path constants
// ---------------------------------------------------------------------------

const (
	agentsBase  = "/agents"
	kbBase      = "/knowledgebases"
	flowsBase   = "/flows"
	promptsBase = "/prompts"
	tagsBase    = "/tags/"
	baService   = "bedrock-agent"
	// baSigV4Service is the real aws-sdk-go-v2 SigV4 signing name for this
	// service ("bedrock", not "bedrock-agent" -- confirmed via bedrockagent's
	// own endpoints.go). RouteMatcher must check both: baService for
	// ChaosServiceName-style callers, baSigV4Service for genuine requests.
	baSigV4Service = "bedrock"
	baPriority     = 87
	splitTwo       = 2
	splitThree     = 3
	splitFour      = 4
	maxPageDefault = 100
)

// ---------------------------------------------------------------------------
// Goconst string constants
// ---------------------------------------------------------------------------

const (
	keyAgent             = "agent"
	keyAgentID           = "agentId"
	keyAgentStatus       = "agentStatus"
	keyAgentVersion      = "agentVersion"
	keyAgentActionGroup  = "agentActionGroup"
	keyAgentAlias        = "agentAlias"
	keyAgentCollaborator = "agentCollaborator"
	keyKnowledgeBase     = "knowledgeBase"
	keyAgentKB           = "agentKnowledgeBase"
	keyDataSource        = "dataSource"
	keyIngestionJob      = "ingestionJob"
	keyDocumentDetails   = "documentDetails"
	keyNextToken         = "nextToken"
	keyStatus            = "status"
	statusDeleting       = "DELETING"
	opUnknown            = "Unknown"
)

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

// Handler is the HTTP handler for the Bedrock Agent REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Bedrock Agent handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears handler state (delegates to backend).
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

// Name returns the service name.
func (h *Handler) Name() string { return "BedrockAgent" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateAgent, opGetAgent, opUpdateAgent, opDeleteAgent, opListAgents, opPrepareAgent,
		// No CreateAgentVersion: it is not a real bedrockagent SDK operation
		// (see the doc comment on InMemoryBackend.CreateAgentVersion).
		opGetAgentVersion, opDeleteAgentVersion, opListAgentVersions,
		opCreateAgentActionGroup, opGetAgentActionGroup, opUpdateAgentActionGroup,
		opDeleteAgentActionGroup, opListAgentActionGroups,
		opCreateAgentAlias, opGetAgentAlias, opUpdateAgentAlias, opDeleteAgentAlias, opListAgentAliases,
		opAssociateAgentCollaborator, opGetAgentCollaborator, opUpdateAgentCollaborator,
		opDisassociateAgentCollaborator, opListAgentCollaborators,
		opCreateKnowledgeBase, opGetKnowledgeBase, opUpdateKnowledgeBase,
		opDeleteKnowledgeBase, opListKnowledgeBases,
		opAssociateAgentKnowledgeBase, opGetAgentKnowledgeBase, opUpdateAgentKnowledgeBase,
		opDisassociateAgentKnowledgeBase, opListAgentKnowledgeBases,
		opCreateDataSource, opGetDataSource, opUpdateDataSource, opDeleteDataSource, opListDataSources,
		opStartIngestionJob, opGetIngestionJob, opStopIngestionJob, opListIngestionJobs,
		opCreateFlow, opGetFlow, opUpdateFlow, opDeleteFlow, opListFlows, opPrepareFlow,
		opValidateFlowDefinition,
		opCreateFlowVersion, opGetFlowVersion, opDeleteFlowVersion, opListFlowVersions,
		opCreateFlowAlias, opGetFlowAlias, opUpdateFlowAlias, opDeleteFlowAlias, opListFlowAliases,
		opCreatePrompt, opGetPrompt, opUpdatePrompt, opDeletePrompt, opListPrompts,
		opCreatePromptVersion,
		// GetPromptVersion and DeletePromptVersion are deliberately NOT advertised: they
		// are not real bedrock-agent operations. The real client gets/deletes a specific
		// prompt version via GetPrompt/DeletePrompt's promptVersion query parameter on
		// the base /prompts/{id}/ path (which this package does not yet implement — see
		// PARITY.md gaps), not a distinct wire operation. dispatchPromptVersions'
		// GET/DELETE /prompts/{id}/versions/{ver} routes remain wired as an internal
		// convenience used by this package's own tests but are unreachable by any real
		// bedrock-agent SDK client.
		opIngestKnowledgeBaseDocuments, opGetKnowledgeBaseDocuments,
		opDeleteKnowledgeBaseDocuments, opListKnowledgeBaseDocuments,
		opListTagsForResource, opTagResource, opUntagResource,
		opPutResourcePolicy, opGetResourcePolicy, opDeleteResourcePolicy,
	}
}

// ChaosServiceName returns the chaos service name.
func (h *Handler) ChaosServiceName() string { return baService }

// ChaosOperations returns all operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the supported regions.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function matching Bedrock Agent requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		svc := httputils.ExtractServiceFromRequest(c.Request())
		if svc == baService {
			return true
		}

		// baSigV4Service ("bedrock") is the real signing name shared by
		// bedrock, bedrockruntime, AND bedrockagent -- ambiguous within the
		// family, so it falls through to the path check below just like an
		// empty/unknown scope (bedrockagent's higher MatchPriority already
		// resolves the /agents,/flows,/prompts overlap with plain bedrock).
		// Any OTHER known, non-empty scope names a genuinely different
		// service and must not be swallowed by this prefix fallback --
		// /tags/, /agents, etc. are shared prefixes other services also
		// serve (e.g. networkmanager, grafana).
		if svc != "" && svc != baSigV4Service {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, agentsBase) ||
			strings.HasPrefix(path, kbBase) ||
			strings.HasPrefix(path, flowsBase) ||
			strings.HasPrefix(path, promptsBase) ||
			strings.HasPrefix(path, tagsBase) ||
			strings.HasPrefix(path, resourcePolicyBase)
	}
}

// MatchPriority returns routing priority.
func (h *Handler) MatchPriority() int { return baPriority }

// ExtractOperation determines the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return classifyPath(c.Request().Method, c.Request().URL.Path)
}

// ExtractResource extracts an agent or flow ID from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	for _, prefix := range []string{"/agents/", "/flows/", "/knowledgebases/", "/prompts/"} {
		if rest, ok := strings.CutPrefix(path, prefix); ok {
			parts := strings.SplitN(rest, "/", splitTwo)

			return parts[0]
		}
	}

	return ""
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)
		ctx := context.WithValue(c.Request().Context(), regionKey{}, region)
		log := logger.Load(ctx)
		path := strings.TrimSuffix(c.Request().URL.Path, "/")
		method := c.Request().Method
		query := c.Request().URL.Query()

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "bedrockagent: failed to read body", "error", err)

			return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", "internal server error"))
		}

		return h.dispatch(ctx, c, path, method, query, body)
	}
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatch(
	ctx context.Context, c *echo.Context, path, method string, query url.Values, body []byte,
) error {
	switch {
	case strings.HasPrefix(path, agentsBase):
		return h.dispatchAgents(ctx, c, path, method, body)
	case strings.HasPrefix(path, kbBase):
		return h.dispatchKB(ctx, c, path, method, body)
	case strings.HasPrefix(path, flowsBase):
		return h.dispatchFlows(ctx, c, path, method, body)
	case strings.HasPrefix(path, promptsBase):
		return h.dispatchPrompts(ctx, c, path, method, body)
	case strings.HasPrefix(path, tagsBase):
		return h.dispatchTags(ctx, c, path, method, query, body)
	case strings.HasPrefix(path, resourcePolicyBase):
		return h.dispatchResourcePolicy(ctx, c, path, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown: "+path))
}

// ---------------------------------------------------------------------------
// Agent dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchAgents(
	ctx context.Context, c *echo.Context, path, method string, body []byte,
) error {
	if path == agentsBase {
		return h.dispatchAgentRoot(ctx, c, method, body)
	}

	rest, _ := strings.CutPrefix(path, agentsBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	agentID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchAgentID(ctx, c, agentID, suffix, method, body)
}

func (h *Handler) dispatchAgentRoot(
	ctx context.Context, c *echo.Context, method string, body []byte,
) error {
	switch method {
	case http.MethodPut:
		return h.handleCreateAgent(ctx, c, body)
	case http.MethodPost, http.MethodGet:
		return h.handleListAgents(ctx, c)
	}

	return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", method))
}

func (h *Handler) dispatchAgentID(
	ctx context.Context, c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetAgent(ctx, c, agentID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateAgent(ctx, c, agentID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteAgent(ctx, c, agentID)
	case suffix == "" && method == http.MethodPost:
		return h.handlePrepareAgent(ctx, c, agentID)
	case suffix == "/prepare" && method == http.MethodPost:
		return h.handlePrepareAgent(ctx, c, agentID)
	case strings.HasPrefix(suffix, "/agentversions"):
		return h.dispatchAgentVersions(ctx, c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentaliases"):
		return h.dispatchAgentAliases(ctx, c, agentID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown agent op"))
}

func (h *Handler) dispatchAgentVersions(
	ctx context.Context, c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/agentversions")

	if rest == "" {
		// Real AWS has no CreateAgentVersion wire op: ListAgentVersions is a
		// POST to this exact collection path (see the wire-shape note on
		// InMemoryBackend.CreateAgentVersion in backend.go). GET is also
		// accepted here as harmless extra leniency.
		switch method {
		case http.MethodPost, http.MethodGet:
			return h.handleListAgentVersions(ctx, c, agentID)
		}

		return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", method))
	}

	parts := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", splitTwo)
	agentVersion := parts[0]
	vSuffix := ""

	if len(parts) == splitTwo {
		vSuffix = "/" + parts[1]
	}

	return h.dispatchAgentVersionSuffix(ctx, c, agentID, agentVersion, vSuffix, method, body)
}

func (h *Handler) dispatchAgentVersionSuffix(
	ctx context.Context, c *echo.Context, agentID, agentVersion, vSuffix, method string, body []byte,
) error {
	switch {
	case vSuffix == "" && method == http.MethodGet:
		return h.handleGetAgentVersion(ctx, c, agentID, agentVersion)
	case vSuffix == "" && method == http.MethodDelete:
		return h.handleDeleteAgentVersion(ctx, c, agentID, agentVersion)
	case strings.HasPrefix(vSuffix, "/actiongroups"):
		return h.dispatchActionGroups(ctx, c, agentID, agentVersion, vSuffix, method, body)
	case strings.HasPrefix(vSuffix, "/agentcollaborators"):
		return h.dispatchCollaborators(ctx, c, agentID, agentVersion, vSuffix, method, body)
	case strings.HasPrefix(vSuffix, "/knowledgebases"):
		return h.dispatchAgentKBs(ctx, c, agentID, agentVersion, vSuffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown version op"))
}

func (h *Handler) dispatchActionGroups(
	ctx context.Context, c *echo.Context, agentID, agentVersion, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/actiongroups")

	// CreateAgentActionGroup (PUT) and ListAgentActionGroups (POST) share
	// this exact collection path on the real wire, distinguished only by
	// method -- POST is NOT a Create synonym here. GET is accepted too as
	// harmless extra leniency (no real client sends it).
	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleCreateAgentActionGroup(ctx, c, agentID, agentVersion, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListAgentActionGroups(ctx, c, agentID, agentVersion)
		}
	}

	agID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetAgentActionGroup(ctx, c, agentID, agentVersion, agID)
	case http.MethodPut:
		return h.handleUpdateAgentActionGroup(ctx, c, agentID, agentVersion, agID, body)
	case http.MethodDelete:
		return h.handleDeleteAgentActionGroup(ctx, c, agentID, agentVersion, agID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown action group op"))
}

func (h *Handler) dispatchCollaborators(
	ctx context.Context, c *echo.Context, agentID, agentVersion, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/agentcollaborators")

	// AssociateAgentCollaborator (PUT) and ListAgentCollaborators (POST)
	// share this exact collection path on the real wire; without the POST
	// case here a real SDK client's ListAgentCollaborators call 404s.
	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleAssociateCollaborator(ctx, c, agentID, agentVersion, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListCollaborators(ctx, c, agentID, agentVersion)
		}
	}

	collaboratorID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetCollaborator(ctx, c, agentID, agentVersion, collaboratorID)
	case http.MethodPut:
		return h.handleUpdateCollaborator(ctx, c, agentID, agentVersion, collaboratorID, body)
	case http.MethodDelete:
		return h.handleDisassociateCollaborator(ctx, c, agentID, agentVersion, collaboratorID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown collab op"))
}

func (h *Handler) dispatchAgentKBs(
	ctx context.Context, c *echo.Context, agentID, agentVersion, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/knowledgebases")

	// AssociateAgentKnowledgeBase (PUT) and ListAgentKnowledgeBases (POST)
	// share this exact collection path on the real wire; without the POST
	// case here a real SDK client's ListAgentKnowledgeBases call 404s.
	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleAssociateAgentKB(ctx, c, agentID, agentVersion, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListAgentKBs(ctx, c, agentID, agentVersion)
		}
	}

	kbID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetAgentKB(ctx, c, agentID, agentVersion, kbID)
	case http.MethodPut:
		return h.handleUpdateAgentKB(ctx, c, agentID, agentVersion, kbID, body)
	case http.MethodDelete:
		return h.handleDisassociateAgentKB(ctx, c, agentID, agentVersion, kbID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown agent-kb op"))
}

func (h *Handler) dispatchAgentAliases(
	ctx context.Context, c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/agentaliases")

	// CreateAgentAlias (PUT) and ListAgentAliases (POST) share this exact
	// collection path on the real wire, distinguished only by method -- POST
	// is NOT a Create synonym here. GET is accepted too as harmless extra
	// leniency (no real client sends it).
	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleCreateAgentAlias(ctx, c, agentID, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListAgentAliases(ctx, c, agentID)
		}
	}

	aliasID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetAgentAlias(ctx, c, agentID, aliasID)
	case http.MethodPut:
		return h.handleUpdateAgentAlias(ctx, c, agentID, aliasID, body)
	case http.MethodDelete:
		return h.handleDeleteAgentAlias(ctx, c, agentID, aliasID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown alias op"))
}

// ---------------------------------------------------------------------------
// Knowledge base dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchKB(
	ctx context.Context, c *echo.Context, path, method string, body []byte,
) error {
	if path == kbBase {
		switch method {
		case http.MethodPut:
			return h.handleCreateKB(ctx, c, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListKBs(ctx, c)
		}
	}

	rest, _ := strings.CutPrefix(path, kbBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	kbID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchKBID(ctx, c, kbID, suffix, method, body)
}

func (h *Handler) dispatchKBID(
	ctx context.Context, c *echo.Context, kbID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetKB(ctx, c, kbID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateKB(ctx, c, kbID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteKB(ctx, c, kbID)
	case strings.HasPrefix(suffix, "/datasources"):
		return h.dispatchDataSources(ctx, c, kbID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown kb op"))
}

func (h *Handler) dispatchDataSources(
	ctx context.Context, c *echo.Context, kbID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/datasources")

	// CreateDataSource (PUT) and ListDataSources (POST) share this exact
	// collection path on the real wire, distinguished only by method -- POST
	// is NOT a Create synonym here. GET is accepted too as harmless extra
	// leniency (no real client sends it).
	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleCreateDS(ctx, c, kbID, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListDS(ctx, c, kbID)
		}
	}

	parts := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", splitTwo)
	dsID := parts[0]
	dsSuffix := ""

	if len(parts) == splitTwo {
		dsSuffix = "/" + parts[1]
	}

	return h.dispatchDSID(ctx, c, kbID, dsID, dsSuffix, method, body)
}

func (h *Handler) dispatchDSID(
	ctx context.Context, c *echo.Context, kbID, dsID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetDS(ctx, c, kbID, dsID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateDS(ctx, c, kbID, dsID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteDS(ctx, c, kbID, dsID)
	case strings.HasPrefix(suffix, "/ingestionjobs"):
		return h.dispatchIngestionJobs(ctx, c, kbID, dsID, suffix, method, body)
	case strings.HasPrefix(suffix, "/documents"):
		return h.dispatchKBDocuments(ctx, c, kbID, dsID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown ds op"))
}

func (h *Handler) dispatchIngestionJobs(
	ctx context.Context, c *echo.Context, kbID, dsID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/ingestionjobs")

	// StartIngestionJob (PUT) and ListIngestionJobs (POST) share this exact
	// collection path on the real wire, distinguished only by method -- POST
	// is NOT a Start synonym here. GET is accepted too as harmless extra
	// leniency (no real client sends it).
	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleStartIngestionJob(ctx, c, kbID, dsID, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListIngestionJobs(ctx, c, kbID, dsID)
		}
	}

	parts := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", splitTwo)
	jobID := parts[0]

	if len(parts) == splitTwo && parts[1] == "stop" {
		return h.handleStopIngestionJob(ctx, c, kbID, dsID, jobID)
	}

	if method == http.MethodGet {
		return h.handleGetIngestionJob(ctx, c, kbID, dsID, jobID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown ingestion op"))
}

func (h *Handler) dispatchKBDocuments(
	ctx context.Context, c *echo.Context, kbID, dsID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/documents")

	// IngestKnowledgeBaseDocuments (PUT) and ListKnowledgeBaseDocuments (POST)
	// share this exact collection path on the real wire, distinguished only by
	// method -- POST is NOT an Ingest synonym here. GET is accepted too as
	// harmless extra leniency (no real client sends it).
	switch {
	case rest == "" && method == http.MethodPut:
		return h.handleIngestKBDocs(ctx, c, kbID, dsID, body)
	case rest == "" && (method == http.MethodPost || method == http.MethodGet):
		return h.handleListKBDocs(ctx, c, kbID, dsID)
	case rest == "/deleteDocuments":
		return h.handleDeleteKBDocs(ctx, c, kbID, dsID, body)
	case rest == "/getDocuments":
		return h.handleGetKBDocs(ctx, c, kbID, dsID, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown kb docs op"))
}

// ---------------------------------------------------------------------------
// Flow dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchFlows(
	ctx context.Context, c *echo.Context, path, method string, body []byte,
) error {
	if path == flowsBase {
		switch method {
		case http.MethodPost:
			return h.handleCreateFlow(ctx, c, body)
		case http.MethodGet:
			return h.handleListFlows(ctx, c)
		}
	}

	if path == flowsBase+"/validate-definition" {
		return h.handleValidateFlowDef(ctx, c, body)
	}

	rest, _ := strings.CutPrefix(path, flowsBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	flowID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchFlowID(ctx, c, flowID, suffix, method, body)
}

func (h *Handler) dispatchFlowID(
	ctx context.Context, c *echo.Context, flowID, suffix, method string, body []byte,
) error {
	if suffix == "" {
		switch method {
		case http.MethodGet:
			return h.handleGetFlow(ctx, c, flowID)
		case http.MethodPut:
			return h.handleUpdateFlow(ctx, c, flowID, body)
		case http.MethodDelete:
			return h.handleDeleteFlow(ctx, c, flowID)
		}
	}

	if suffix == "/prepare" && method == http.MethodPost {
		return h.handlePrepareFlow(ctx, c, flowID)
	}

	if strings.HasPrefix(suffix, "/versions") {
		return h.dispatchFlowVersions(ctx, c, flowID, suffix, method, body)
	}

	if strings.HasPrefix(suffix, "/aliases") {
		return h.dispatchFlowAliases(ctx, c, flowID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown flow op"))
}

func (h *Handler) dispatchFlowVersions(
	ctx context.Context, c *echo.Context, flowID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/versions")

	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateFlowVersion(ctx, c, flowID, body)
		case http.MethodGet:
			return h.handleListFlowVersions(ctx, c, flowID)
		}
	}

	flowVersion := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetFlowVersion(ctx, c, flowID, flowVersion)
	case http.MethodDelete:
		return h.handleDeleteFlowVersion(ctx, c, flowID, flowVersion)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown flow version op"))
}

func (h *Handler) dispatchFlowAliases(
	ctx context.Context, c *echo.Context, flowID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/aliases")

	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateFlowAlias(ctx, c, flowID, body)
		case http.MethodGet:
			return h.handleListFlowAliases(ctx, c, flowID)
		}
	}

	aliasID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetFlowAlias(ctx, c, flowID, aliasID)
	case http.MethodPut:
		return h.handleUpdateFlowAlias(ctx, c, flowID, aliasID, body)
	case http.MethodDelete:
		return h.handleDeleteFlowAlias(ctx, c, flowID, aliasID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown flow alias op"))
}

// ---------------------------------------------------------------------------
// Prompt dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchPrompts(
	ctx context.Context, c *echo.Context, path, method string, body []byte,
) error {
	if path == promptsBase {
		switch method {
		case http.MethodPost:
			return h.handleCreatePrompt(ctx, c, body)
		case http.MethodGet:
			return h.handleListPrompts(ctx, c)
		}
	}

	rest, _ := strings.CutPrefix(path, promptsBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	promptID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchPromptID(ctx, c, promptID, suffix, method, body)
}

func (h *Handler) dispatchPromptID(
	ctx context.Context, c *echo.Context, promptID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetPrompt(ctx, c, promptID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdatePrompt(ctx, c, promptID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeletePrompt(ctx, c, promptID)
	case strings.HasPrefix(suffix, "/versions"):
		return h.dispatchPromptVersions(ctx, c, promptID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown prompt op"))
}

func (h *Handler) dispatchPromptVersions(
	ctx context.Context, c *echo.Context, promptID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/versions")

	if rest == "" && method == http.MethodPost {
		return h.handleCreatePromptVersion(ctx, c, promptID, body)
	}

	versionID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetPromptVersion(ctx, c, promptID, versionID)
	case http.MethodDelete:
		return h.handleDeletePromptVersion(ctx, c, promptID, versionID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown prompt version op"))
}

// ---------------------------------------------------------------------------
// Tag dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchTags(
	ctx context.Context, c *echo.Context, path, method string, query url.Values, body []byte,
) error {
	resourceARN, _ := strings.CutPrefix(path, tagsBase)

	switch method {
	case http.MethodGet:
		return h.handleListTags(ctx, c, resourceARN)
	case http.MethodPost:
		return h.handleTagResource(ctx, c, resourceARN, body)
	case http.MethodDelete:
		return h.handleUntagResource(ctx, c, resourceARN, query)
	}

	return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", method))
}
