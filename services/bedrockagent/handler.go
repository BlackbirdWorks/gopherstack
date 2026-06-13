package bedrockagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ---------------------------------------------------------------------------
// Operation name constants
// ---------------------------------------------------------------------------

const (
	opCreateAgent                  = "CreateAgent"
	opGetAgent                     = "GetAgent"
	opUpdateAgent                  = "UpdateAgent"
	opDeleteAgent                  = "DeleteAgent"
	opListAgents                   = "ListAgents"
	opPrepareAgent                 = "PrepareAgent"
	opCreateAgentVersion           = "CreateAgentVersion"
	opGetAgentVersion              = "GetAgentVersion"
	opDeleteAgentVersion           = "DeleteAgentVersion"
	opListAgentVersions            = "ListAgentVersions"
	opCreateAgentActionGroup       = "CreateAgentActionGroup"
	opGetAgentActionGroup          = "GetAgentActionGroup"
	opUpdateAgentActionGroup       = "UpdateAgentActionGroup"
	opDeleteAgentActionGroup       = "DeleteAgentActionGroup"
	opListAgentActionGroups        = "ListAgentActionGroups"
	opCreateAgentAlias             = "CreateAgentAlias"
	opGetAgentAlias                = "GetAgentAlias"
	opUpdateAgentAlias             = "UpdateAgentAlias"
	opDeleteAgentAlias             = "DeleteAgentAlias"
	opListAgentAliases             = "ListAgentAliases"
	opAssociateAgentCollaborator   = "AssociateAgentCollaborator"
	opGetAgentCollaborator         = "GetAgentCollaborator"
	opUpdateAgentCollaborator      = "UpdateAgentCollaborator"
	opDisassociateAgentCollaborator = "DisassociateAgentCollaborator"
	opListAgentCollaborators       = "ListAgentCollaborators"
	opCreateKnowledgeBase          = "CreateKnowledgeBase"
	opGetKnowledgeBase             = "GetKnowledgeBase"
	opUpdateKnowledgeBase          = "UpdateKnowledgeBase"
	opDeleteKnowledgeBase          = "DeleteKnowledgeBase"
	opListKnowledgeBases           = "ListKnowledgeBases"
	opAssociateAgentKnowledgeBase  = "AssociateAgentKnowledgeBase"
	opGetAgentKnowledgeBase        = "GetAgentKnowledgeBase"
	opUpdateAgentKnowledgeBase     = "UpdateAgentKnowledgeBase"
	opDisassociateAgentKnowledgeBase = "DisassociateAgentKnowledgeBase"
	opListAgentKnowledgeBases      = "ListAgentKnowledgeBases"
	opCreateDataSource             = "CreateDataSource"
	opGetDataSource                = "GetDataSource"
	opUpdateDataSource             = "UpdateDataSource"
	opDeleteDataSource             = "DeleteDataSource"
	opListDataSources              = "ListDataSources"
	opStartIngestionJob            = "StartIngestionJob"
	opGetIngestionJob              = "GetIngestionJob"
	opStopIngestionJob             = "StopIngestionJob"
	opListIngestionJobs            = "ListIngestionJobs"
	opCreateFlow                   = "CreateFlow"
	opGetFlow                      = "GetFlow"
	opUpdateFlow                   = "UpdateFlow"
	opDeleteFlow                   = "DeleteFlow"
	opListFlows                    = "ListFlows"
	opPrepareFlow                  = "PrepareFlow"
	opValidateFlowDefinition       = "ValidateFlowDefinition"
	opCreateFlowVersion            = "CreateFlowVersion"
	opGetFlowVersion               = "GetFlowVersion"
	opDeleteFlowVersion            = "DeleteFlowVersion"
	opListFlowVersions             = "ListFlowVersions"
	opCreateFlowAlias              = "CreateFlowAlias"
	opGetFlowAlias                 = "GetFlowAlias"
	opUpdateFlowAlias              = "UpdateFlowAlias"
	opDeleteFlowAlias              = "DeleteFlowAlias"
	opListFlowAliases              = "ListFlowAliases"
	opCreatePrompt                 = "CreatePrompt"
	opGetPrompt                    = "GetPrompt"
	opUpdatePrompt                 = "UpdatePrompt"
	opDeletePrompt                 = "DeletePrompt"
	opListPrompts                  = "ListPrompts"
	opCreatePromptVersion          = "CreatePromptVersion"
	opGetPromptVersion             = "GetPromptVersion"
	opDeletePromptVersion          = "DeletePromptVersion"
	opIngestKnowledgeBaseDocuments = "IngestKnowledgeBaseDocuments"
	opGetKnowledgeBaseDocuments    = "GetKnowledgeBaseDocuments"
	opDeleteKnowledgeBaseDocuments = "DeleteKnowledgeBaseDocuments"
	opListKnowledgeBaseDocuments   = "ListKnowledgeBaseDocuments"
	opListTagsForResource          = "ListTagsForResource"
	opTagResource                  = "TagResource"
	opUntagResource                = "UntagResource"
)

// ---------------------------------------------------------------------------
// Path constants
// ---------------------------------------------------------------------------

const (
	agentsBase    = "/agents"
	kbBase        = "/knowledgebases"
	flowsBase     = "/flows"
	promptsBase   = "/prompts"
	tagsBase      = "/tags/"
	baService     = "bedrock-agent"
	baPriority    = 87
	splitTwo      = 2
	splitThree    = 3
	splitFour     = 4
	maxPageDefault = 100
)

var errUnknown = errors.New("unknown operation")

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
		opCreateAgentVersion, opGetAgentVersion, opDeleteAgentVersion, opListAgentVersions,
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
		opCreatePromptVersion, opGetPromptVersion, opDeletePromptVersion,
		opIngestKnowledgeBaseDocuments, opGetKnowledgeBaseDocuments,
		opDeleteKnowledgeBaseDocuments, opListKnowledgeBaseDocuments,
		opListTagsForResource, opTagResource, opUntagResource,
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

		path := c.Request().URL.Path

		return strings.HasPrefix(path, agentsBase) ||
			strings.HasPrefix(path, kbBase) ||
			strings.HasPrefix(path, flowsBase) ||
			strings.HasPrefix(path, promptsBase) ||
			strings.HasPrefix(path, tagsBase)
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

		return h.dispatch(c, ctx, path, method, query, body)
	}
}

// ---------------------------------------------------------------------------
// Dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatch(
	c *echo.Context, ctx context.Context, path, method string, query url.Values, body []byte,
) error {
	switch {
	case strings.HasPrefix(path, agentsBase):
		return h.dispatchAgents(c, ctx, path, method, body)
	case strings.HasPrefix(path, kbBase):
		return h.dispatchKB(c, ctx, path, method, body)
	case strings.HasPrefix(path, flowsBase):
		return h.dispatchFlows(c, ctx, path, method, body)
	case strings.HasPrefix(path, promptsBase):
		return h.dispatchPrompts(c, ctx, path, method, body)
	case strings.HasPrefix(path, tagsBase):
		return h.dispatchTags(c, ctx, path, method, query, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown: "+path))
}

// ---------------------------------------------------------------------------
// Agent dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchAgents(
	c *echo.Context, ctx context.Context, path, method string, body []byte,
) error {
	if path == agentsBase {
		return h.dispatchAgentRoot(c, ctx, method, body)
	}

	rest, _ := strings.CutPrefix(path, agentsBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	agentID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchAgentID(c, ctx, agentID, suffix, method, body)
}

func (h *Handler) dispatchAgentRoot(
	c *echo.Context, ctx context.Context, method string, body []byte,
) error {
	switch method {
	case http.MethodPut, http.MethodPost:
		return h.handleCreateAgent(c, ctx, body)
	case http.MethodGet:
		return h.handleListAgents(c, ctx)
	}

	return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", method))
}

func (h *Handler) dispatchAgentID(
	c *echo.Context, ctx context.Context, agentID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetAgent(c, ctx, agentID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateAgent(c, ctx, agentID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteAgent(c, ctx, agentID)
	case suffix == "/prepare" && method == http.MethodPost:
		return h.handlePrepareAgent(c, ctx, agentID)
	case strings.HasPrefix(suffix, "/agentversions"):
		return h.dispatchAgentVersions(c, ctx, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentaliases"):
		return h.dispatchAgentAliases(c, ctx, agentID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown agent op"))
}

func (h *Handler) dispatchAgentVersions(
	c *echo.Context, ctx context.Context, agentID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/agentversions")

	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateAgentVersion(c, ctx, agentID, body)
		case http.MethodGet:
			return h.handleListAgentVersions(c, ctx, agentID)
		}

		return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", method))
	}

	parts := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", splitTwo)
	agentVersion := parts[0]
	vSuffix := ""

	if len(parts) == splitTwo {
		vSuffix = "/" + parts[1]
	}

	return h.dispatchAgentVersionSuffix(c, ctx, agentID, agentVersion, vSuffix, method, body)
}

func (h *Handler) dispatchAgentVersionSuffix(
	c *echo.Context, ctx context.Context, agentID, agentVersion, vSuffix, method string, body []byte,
) error {
	switch {
	case vSuffix == "" && method == http.MethodGet:
		return h.handleGetAgentVersion(c, ctx, agentID, agentVersion)
	case vSuffix == "" && method == http.MethodDelete:
		return h.handleDeleteAgentVersion(c, ctx, agentID, agentVersion)
	case strings.HasPrefix(vSuffix, "/actiongroups"):
		return h.dispatchActionGroups(c, ctx, agentID, agentVersion, vSuffix, method, body)
	case strings.HasPrefix(vSuffix, "/agentcollaborators"):
		return h.dispatchCollaborators(c, ctx, agentID, agentVersion, vSuffix, method, body)
	case strings.HasPrefix(vSuffix, "/knowledgebases"):
		return h.dispatchAgentKBs(c, ctx, agentID, agentVersion, vSuffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown version op"))
}

func (h *Handler) dispatchActionGroups(
	c *echo.Context, ctx context.Context, agentID, agentVersion, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/actiongroups")

	if rest == "" {
		switch method {
		case http.MethodPut, http.MethodPost:
			return h.handleCreateAgentActionGroup(c, ctx, agentID, body)
		case http.MethodGet:
			return h.handleListAgentActionGroups(c, ctx, agentID, agentVersion)
		}
	}

	agID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetAgentActionGroup(c, ctx, agentID, agentVersion, agID)
	case http.MethodPut:
		return h.handleUpdateAgentActionGroup(c, ctx, agentID, agentVersion, agID, body)
	case http.MethodDelete:
		return h.handleDeleteAgentActionGroup(c, ctx, agentID, agentVersion, agID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown action group op"))
}

func (h *Handler) dispatchCollaborators(
	c *echo.Context, ctx context.Context, agentID, agentVersion, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/agentcollaborators")

	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleAssociateCollaborator(c, ctx, agentID, agentVersion, body)
		case http.MethodGet:
			return h.handleListCollaborators(c, ctx, agentID, agentVersion)
		}
	}

	collaboratorID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetCollaborator(c, ctx, agentID, agentVersion, collaboratorID)
	case http.MethodPut:
		return h.handleUpdateCollaborator(c, ctx, agentID, agentVersion, collaboratorID, body)
	case http.MethodDelete:
		return h.handleDisassociateCollaborator(c, ctx, agentID, agentVersion, collaboratorID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown collab op"))
}

func (h *Handler) dispatchAgentKBs(
	c *echo.Context, ctx context.Context, agentID, agentVersion, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/knowledgebases")

	if rest == "" {
		switch method {
		case http.MethodPut:
			return h.handleAssociateAgentKB(c, ctx, agentID, agentVersion, body)
		case http.MethodGet:
			return h.handleListAgentKBs(c, ctx, agentID, agentVersion)
		}
	}

	kbID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetAgentKB(c, ctx, agentID, agentVersion, kbID)
	case http.MethodPut:
		return h.handleUpdateAgentKB(c, ctx, agentID, agentVersion, kbID, body)
	case http.MethodDelete:
		return h.handleDisassociateAgentKB(c, ctx, agentID, agentVersion, kbID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown agent-kb op"))
}

func (h *Handler) dispatchAgentAliases(
	c *echo.Context, ctx context.Context, agentID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/agentaliases")

	if rest == "" {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreateAgentAlias(c, ctx, agentID, body)
		case http.MethodGet:
			return h.handleListAgentAliases(c, ctx, agentID)
		}
	}

	aliasID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetAgentAlias(c, ctx, agentID, aliasID)
	case http.MethodPut:
		return h.handleUpdateAgentAlias(c, ctx, agentID, aliasID, body)
	case http.MethodDelete:
		return h.handleDeleteAgentAlias(c, ctx, agentID, aliasID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown alias op"))
}

// ---------------------------------------------------------------------------
// Knowledge base dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchKB(
	c *echo.Context, ctx context.Context, path, method string, body []byte,
) error {
	if path == kbBase {
		switch method {
		case http.MethodPut, http.MethodPost:
			return h.handleCreateKB(c, ctx, body)
		case http.MethodGet:
			return h.handleListKBs(c, ctx)
		}
	}

	rest, _ := strings.CutPrefix(path, kbBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	kbID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchKBID(c, ctx, kbID, suffix, method, body)
}

func (h *Handler) dispatchKBID(
	c *echo.Context, ctx context.Context, kbID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetKB(c, ctx, kbID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateKB(c, ctx, kbID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteKB(c, ctx, kbID)
	case strings.HasPrefix(suffix, "/datasources"):
		return h.dispatchDataSources(c, ctx, kbID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown kb op"))
}

func (h *Handler) dispatchDataSources(
	c *echo.Context, ctx context.Context, kbID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/datasources")

	if rest == "" {
		switch method {
		case http.MethodPut, http.MethodPost:
			return h.handleCreateDS(c, ctx, kbID, body)
		case http.MethodGet:
			return h.handleListDS(c, ctx, kbID)
		}
	}

	parts := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", splitTwo)
	dsID := parts[0]
	dsSuffix := ""

	if len(parts) == splitTwo {
		dsSuffix = "/" + parts[1]
	}

	return h.dispatchDSID(c, ctx, kbID, dsID, dsSuffix, method, body)
}

func (h *Handler) dispatchDSID(
	c *echo.Context, ctx context.Context, kbID, dsID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetDS(c, ctx, kbID, dsID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateDS(c, ctx, kbID, dsID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteDS(c, ctx, kbID, dsID)
	case strings.HasPrefix(suffix, "/ingestionjobs"):
		return h.dispatchIngestionJobs(c, ctx, kbID, dsID, suffix, method, body)
	case strings.HasPrefix(suffix, "/documents"):
		return h.dispatchKBDocuments(c, ctx, kbID, dsID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown ds op"))
}

func (h *Handler) dispatchIngestionJobs(
	c *echo.Context, ctx context.Context, kbID, dsID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/ingestionjobs")

	if rest == "" {
		switch method {
		case http.MethodPut, http.MethodPost:
			return h.handleStartIngestionJob(c, ctx, kbID, dsID, body)
		case http.MethodGet:
			return h.handleListIngestionJobs(c, ctx, kbID, dsID)
		}
	}

	parts := strings.SplitN(strings.TrimPrefix(rest, "/"), "/", splitTwo)
	jobID := parts[0]

	if len(parts) == splitTwo && parts[1] == "stop" {
		return h.handleStopIngestionJob(c, ctx, kbID, dsID, jobID)
	}

	if method == http.MethodGet {
		return h.handleGetIngestionJob(c, ctx, kbID, dsID, jobID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown ingestion op"))
}

func (h *Handler) dispatchKBDocuments(
	c *echo.Context, ctx context.Context, kbID, dsID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/documents")

	switch {
	case rest == "" && method == http.MethodPost:
		return h.handleIngestKBDocs(c, ctx, kbID, dsID, body)
	case rest == "" && method == http.MethodGet:
		return h.handleListKBDocs(c, ctx, kbID, dsID)
	case rest == "/deleteDocuments":
		return h.handleDeleteKBDocs(c, ctx, kbID, dsID, body)
	case rest == "/getDocuments":
		return h.handleGetKBDocs(c, ctx, kbID, dsID, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown kb docs op"))
}

// ---------------------------------------------------------------------------
// Flow dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchFlows(
	c *echo.Context, ctx context.Context, path, method string, body []byte,
) error {
	if path == flowsBase {
		switch method {
		case http.MethodPost:
			return h.handleCreateFlow(c, ctx, body)
		case http.MethodGet:
			return h.handleListFlows(c, ctx)
		}
	}

	if path == flowsBase+"/validate-definition" {
		return h.handleValidateFlowDef(c, ctx, body)
	}

	rest, _ := strings.CutPrefix(path, flowsBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	flowID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchFlowID(c, ctx, flowID, suffix, method, body)
}

func (h *Handler) dispatchFlowID(
	c *echo.Context, ctx context.Context, flowID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetFlow(c, ctx, flowID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateFlow(c, ctx, flowID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteFlow(c, ctx, flowID)
	case suffix == "/prepare" && method == http.MethodPost:
		return h.handlePrepareFlow(c, ctx, flowID)
	case strings.HasPrefix(suffix, "/versions"):
		return h.dispatchFlowVersions(c, ctx, flowID, suffix, method, body)
	case strings.HasPrefix(suffix, "/aliases"):
		return h.dispatchFlowAliases(c, ctx, flowID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown flow op"))
}

func (h *Handler) dispatchFlowVersions(
	c *echo.Context, ctx context.Context, flowID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/versions")

	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateFlowVersion(c, ctx, flowID, body)
		case http.MethodGet:
			return h.handleListFlowVersions(c, ctx, flowID)
		}
	}

	flowVersion := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetFlowVersion(c, ctx, flowID, flowVersion)
	case http.MethodDelete:
		return h.handleDeleteFlowVersion(c, ctx, flowID, flowVersion)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown flow version op"))
}

func (h *Handler) dispatchFlowAliases(
	c *echo.Context, ctx context.Context, flowID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/aliases")

	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateFlowAlias(c, ctx, flowID, body)
		case http.MethodGet:
			return h.handleListFlowAliases(c, ctx, flowID)
		}
	}

	aliasID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetFlowAlias(c, ctx, flowID, aliasID)
	case http.MethodPut:
		return h.handleUpdateFlowAlias(c, ctx, flowID, aliasID, body)
	case http.MethodDelete:
		return h.handleDeleteFlowAlias(c, ctx, flowID, aliasID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown flow alias op"))
}

// ---------------------------------------------------------------------------
// Prompt dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchPrompts(
	c *echo.Context, ctx context.Context, path, method string, body []byte,
) error {
	if path == promptsBase {
		switch method {
		case http.MethodPost:
			return h.handleCreatePrompt(c, ctx, body)
		case http.MethodGet:
			return h.handleListPrompts(c, ctx)
		}
	}

	rest, _ := strings.CutPrefix(path, promptsBase+"/")
	parts := strings.SplitN(rest, "/", splitTwo)
	promptID := parts[0]
	suffix := ""

	if len(parts) == splitTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchPromptID(c, ctx, promptID, suffix, method, body)
}

func (h *Handler) dispatchPromptID(
	c *echo.Context, ctx context.Context, promptID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetPrompt(c, ctx, promptID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdatePrompt(c, ctx, promptID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeletePrompt(c, ctx, promptID)
	case strings.HasPrefix(suffix, "/versions"):
		return h.dispatchPromptVersions(c, ctx, promptID, suffix, method, body)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown prompt op"))
}

func (h *Handler) dispatchPromptVersions(
	c *echo.Context, ctx context.Context, promptID, suffix, method string, body []byte,
) error {
	rest, _ := strings.CutPrefix(suffix, "/versions")

	if rest == "" && method == http.MethodPost {
		return h.handleCreatePromptVersion(c, ctx, promptID, body)
	}

	versionID := strings.TrimPrefix(rest, "/")

	switch method {
	case http.MethodGet:
		return h.handleGetPromptVersion(c, ctx, promptID, versionID)
	case http.MethodDelete:
		return h.handleDeletePromptVersion(c, ctx, promptID, versionID)
	}

	return c.JSON(http.StatusNotFound, errResp("UnknownOperationException", "unknown prompt version op"))
}

// ---------------------------------------------------------------------------
// Tag dispatch
// ---------------------------------------------------------------------------

func (h *Handler) dispatchTags(
	c *echo.Context, ctx context.Context, path, method string, query url.Values, body []byte,
) error {
	resourceARN, _ := strings.CutPrefix(path, tagsBase)

	switch method {
	case http.MethodGet:
		return h.handleListTags(c, ctx, resourceARN)
	case http.MethodPost:
		return h.handleTagResource(c, ctx, resourceARN, body)
	case http.MethodDelete:
		return h.handleUntagResource(c, ctx, resourceARN, query)
	}

	return c.JSON(http.StatusMethodNotAllowed, errResp("MethodNotAllowedException", method))
}

// ---------------------------------------------------------------------------
// Agent handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAgent(c *echo.Context, ctx context.Context, body []byte) error {
	var req struct {
		Tags            map[string]string `json:"tags"`
		Guardrail       map[string]any    `json:"guardrailConfiguration"`
		Memory          map[string]any    `json:"memoryConfiguration"`
		AgentName       string            `json:"agentName"`
		Collaboration   string            `json:"agentCollaboration"`
		Description     string            `json:"description"`
		FoundationModel string            `json:"foundationModel"`
		Instruction     string            `json:"instruction"`
		RoleARN         string            `json:"agentResourceRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	agent, err := h.Backend.CreateAgent(ctx, AgentConfig{
		AgentName:       req.AgentName,
		Collaboration:   req.Collaboration,
		Description:     req.Description,
		FoundationModel: req.FoundationModel,
		Instruction:     req.Instruction,
		RoleARN:         req.RoleARN,
		Tags:            req.Tags,
		Guardrail:       req.Guardrail,
		Memory:          req.Memory,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agent": agent})
}

func (h *Handler) handleGetAgent(c *echo.Context, ctx context.Context, agentID string) error {
	agent, err := h.Backend.GetAgent(ctx, agentID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agent": agent})
}

func (h *Handler) handleUpdateAgent(
	c *echo.Context, ctx context.Context, agentID string, body []byte,
) error {
	var req struct {
		Tags            map[string]string `json:"tags"`
		Guardrail       map[string]any    `json:"guardrailConfiguration"`
		Memory          map[string]any    `json:"memoryConfiguration"`
		AgentName       string            `json:"agentName"`
		Collaboration   string            `json:"agentCollaboration"`
		Description     string            `json:"description"`
		FoundationModel string            `json:"foundationModel"`
		Instruction     string            `json:"instruction"`
		RoleARN         string            `json:"agentResourceRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	agent, err := h.Backend.UpdateAgent(ctx, agentID, AgentConfig{
		AgentName:       req.AgentName,
		Collaboration:   req.Collaboration,
		Description:     req.Description,
		FoundationModel: req.FoundationModel,
		Instruction:     req.Instruction,
		RoleARN:         req.RoleARN,
		Tags:            req.Tags,
		Guardrail:       req.Guardrail,
		Memory:          req.Memory,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agent": agent})
}

func (h *Handler) handleDeleteAgent(c *echo.Context, ctx context.Context, agentID string) error {
	if err := h.Backend.DeleteAgent(ctx, agentID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentId": agentID, "agentStatus": "DELETING"})
}

func (h *Handler) handleListAgents(c *echo.Context, ctx context.Context) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	agents, outToken, err := h.Backend.ListAgents(ctx, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentSummaries": agents, "nextToken": outToken})
}

func (h *Handler) handlePrepareAgent(c *echo.Context, ctx context.Context, agentID string) error {
	agent, err := h.Backend.PrepareAgent(ctx, agentID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{
		"agentId":      agent.AgentID,
		"agentStatus":  agent.AgentStatus,
		"agentVersion": agent.AgentVersion,
	})
}

// ---------------------------------------------------------------------------
// Agent version handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAgentVersion(
	c *echo.Context, ctx context.Context, agentID string, body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	_ = json.Unmarshal(body, &req)

	av, err := h.Backend.CreateAgentVersion(ctx, agentID, req.Description)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentVersion": av})
}

func (h *Handler) handleGetAgentVersion(
	c *echo.Context, ctx context.Context, agentID, version string,
) error {
	av, err := h.Backend.GetAgentVersion(ctx, agentID, version)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentVersion": av})
}

func (h *Handler) handleDeleteAgentVersion(
	c *echo.Context, ctx context.Context, agentID, version string,
) error {
	if err := h.Backend.DeleteAgentVersion(ctx, agentID, version); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentId": agentID, "agentVersion": version, "agentStatus": "DELETING"})
}

func (h *Handler) handleListAgentVersions(
	c *echo.Context, ctx context.Context, agentID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListAgentVersions(ctx, agentID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"agentVersionSummaries": summaries,
		"nextToken":             outToken,
	})
}

// ---------------------------------------------------------------------------
// Agent action group handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAgentActionGroup(
	c *echo.Context, ctx context.Context, agentID string, body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
		ActionGroupState    string         `json:"actionGroupState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	ag, err := h.Backend.CreateAgentActionGroup(ctx, agentID, ActionGroupConfig{
		ActionGroupName:     req.ActionGroupName,
		Description:         req.Description,
		ActionGroupState:    req.ActionGroupState,
		ActionGroupExecutor: req.ActionGroupExecutor,
		APISchema:           req.APISchema,
		FunctionSchema:      req.FunctionSchema,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentActionGroup": ag})
}

func (h *Handler) handleGetAgentActionGroup(
	c *echo.Context, ctx context.Context, agentID, agentVersion, agID string,
) error {
	ag, err := h.Backend.GetAgentActionGroup(ctx, agentID, agentVersion, agID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentActionGroup": ag})
}

func (h *Handler) handleUpdateAgentActionGroup(
	c *echo.Context, ctx context.Context, agentID, agentVersion, agID string, body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
		ActionGroupState    string         `json:"actionGroupState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	ag, err := h.Backend.UpdateAgentActionGroup(ctx, agentID, agentVersion, agID, ActionGroupConfig{
		ActionGroupName:     req.ActionGroupName,
		Description:         req.Description,
		ActionGroupState:    req.ActionGroupState,
		ActionGroupExecutor: req.ActionGroupExecutor,
		APISchema:           req.APISchema,
		FunctionSchema:      req.FunctionSchema,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentActionGroup": ag})
}

func (h *Handler) handleDeleteAgentActionGroup(
	c *echo.Context, ctx context.Context, agentID, agentVersion, agID string,
) error {
	if err := h.Backend.DeleteAgentActionGroup(ctx, agentID, agentVersion, agID); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListAgentActionGroups(
	c *echo.Context, ctx context.Context, agentID, agentVersion string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListAgentActionGroups(ctx, agentID, agentVersion, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"actionGroupSummaries": summaries,
		"nextToken":            outToken,
	})
}

// ---------------------------------------------------------------------------
// Agent alias handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateAgentAlias(
	c *echo.Context, ctx context.Context, agentID string, body []byte,
) error {
	var req struct {
		Tags                 map[string]string `json:"tags"`
		RoutingConfiguration []AliasRouting    `json:"routingConfiguration"`
		AgentAliasName       string            `json:"agentAliasName"`
		Description          string            `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	al, err := h.Backend.CreateAgentAlias(ctx, agentID, AliasConfig{
		AliasName:            req.AgentAliasName,
		Description:          req.Description,
		RoutingConfiguration: req.RoutingConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentAlias": al})
}

func (h *Handler) handleGetAgentAlias(
	c *echo.Context, ctx context.Context, agentID, aliasID string,
) error {
	al, err := h.Backend.GetAgentAlias(ctx, agentID, aliasID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentAlias": al})
}

func (h *Handler) handleUpdateAgentAlias(
	c *echo.Context, ctx context.Context, agentID, aliasID string, body []byte,
) error {
	var req struct {
		Tags                 map[string]string `json:"tags"`
		RoutingConfiguration []AliasRouting    `json:"routingConfiguration"`
		AgentAliasName       string            `json:"agentAliasName"`
		Description          string            `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	al, err := h.Backend.UpdateAgentAlias(ctx, agentID, aliasID, AliasConfig{
		AliasName:            req.AgentAliasName,
		Description:          req.Description,
		RoutingConfiguration: req.RoutingConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentAlias": al})
}

func (h *Handler) handleDeleteAgentAlias(
	c *echo.Context, ctx context.Context, agentID, aliasID string,
) error {
	if err := h.Backend.DeleteAgentAlias(ctx, agentID, aliasID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentId": agentID, "agentAliasId": aliasID, "agentAliasStatus": "DELETING"})
}

func (h *Handler) handleListAgentAliases(
	c *echo.Context, ctx context.Context, agentID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListAgentAliases(ctx, agentID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"agentAliasSummaries": summaries,
		"nextToken":           outToken,
	})
}

// ---------------------------------------------------------------------------
// Collaborator handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleAssociateCollaborator(
	c *echo.Context, ctx context.Context, agentID, agentVersion string, body []byte,
) error {
	var req struct {
		AgentDescriptor          map[string]any `json:"agentDescriptor"`
		CollaboratorName         string         `json:"collaboratorName"`
		CollaborationInstruction string         `json:"collaborationInstruction"`
		RelayConversationHistory string         `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	collab, err := h.Backend.AssociateAgentCollaborator(ctx, agentID, agentVersion, CollaboratorConfig{
		CollaboratorName:         req.CollaboratorName,
		CollaborationInstruction: req.CollaborationInstruction,
		RelayConversationHistory: req.RelayConversationHistory,
		AgentDescriptor:          req.AgentDescriptor,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentCollaborator": collab})
}

func (h *Handler) handleGetCollaborator(
	c *echo.Context, ctx context.Context, agentID, agentVersion, collaboratorID string,
) error {
	collab, err := h.Backend.GetAgentCollaborator(ctx, agentID, agentVersion, collaboratorID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentCollaborator": collab})
}

func (h *Handler) handleUpdateCollaborator(
	c *echo.Context, ctx context.Context, agentID, agentVersion, collaboratorID string, body []byte,
) error {
	var req struct {
		AgentDescriptor          map[string]any `json:"agentDescriptor"`
		CollaboratorName         string         `json:"collaboratorName"`
		CollaborationInstruction string         `json:"collaborationInstruction"`
		RelayConversationHistory string         `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	collab, err := h.Backend.UpdateAgentCollaborator(ctx, agentID, agentVersion, collaboratorID, CollaboratorConfig{
		CollaboratorName:         req.CollaboratorName,
		CollaborationInstruction: req.CollaborationInstruction,
		RelayConversationHistory: req.RelayConversationHistory,
		AgentDescriptor:          req.AgentDescriptor,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentCollaborator": collab})
}

func (h *Handler) handleDisassociateCollaborator(
	c *echo.Context, ctx context.Context, agentID, agentVersion, collaboratorID string,
) error {
	if err := h.Backend.DisassociateAgentCollaborator(ctx, agentID, agentVersion, collaboratorID); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListCollaborators(
	c *echo.Context, ctx context.Context, agentID, agentVersion string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	collabs, outToken, err := h.Backend.ListAgentCollaborators(ctx, agentID, agentVersion, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"agentCollaboratorSummaries": collabs,
		"nextToken":                  outToken,
	})
}

// ---------------------------------------------------------------------------
// Knowledge base handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateKB(c *echo.Context, ctx context.Context, body []byte) error {
	var req struct {
		Tags                 map[string]string `json:"tags"`
		KBConfiguration      map[string]any    `json:"knowledgeBaseConfiguration"`
		StorageConfiguration map[string]any    `json:"storageConfiguration"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		RoleARN              string            `json:"roleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	kb, err := h.Backend.CreateKnowledgeBase(ctx, KnowledgeBaseConfig{
		Name:                 req.Name,
		Description:          req.Description,
		RoleARN:              req.RoleARN,
		KBConfiguration:      req.KBConfiguration,
		StorageConfiguration: req.StorageConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"knowledgeBase": kb})
}

func (h *Handler) handleGetKB(c *echo.Context, ctx context.Context, kbID string) error {
	kb, err := h.Backend.GetKnowledgeBase(ctx, kbID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"knowledgeBase": kb})
}

func (h *Handler) handleUpdateKB(c *echo.Context, ctx context.Context, kbID string, body []byte) error {
	var req struct {
		Tags                 map[string]string `json:"tags"`
		KBConfiguration      map[string]any    `json:"knowledgeBaseConfiguration"`
		StorageConfiguration map[string]any    `json:"storageConfiguration"`
		Name                 string            `json:"name"`
		Description          string            `json:"description"`
		RoleARN              string            `json:"roleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	kb, err := h.Backend.UpdateKnowledgeBase(ctx, kbID, KnowledgeBaseConfig{
		Name:                 req.Name,
		Description:          req.Description,
		RoleARN:              req.RoleARN,
		KBConfiguration:      req.KBConfiguration,
		StorageConfiguration: req.StorageConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"knowledgeBase": kb})
}

func (h *Handler) handleDeleteKB(c *echo.Context, ctx context.Context, kbID string) error {
	if err := h.Backend.DeleteKnowledgeBase(ctx, kbID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"knowledgeBaseId": kbID, "status": "DELETING"})
}

func (h *Handler) handleListKBs(c *echo.Context, ctx context.Context) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListKnowledgeBases(ctx, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"knowledgeBaseSummaries": summaries,
		"nextToken":              outToken,
	})
}

// ---------------------------------------------------------------------------
// Agent–KB association handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleAssociateAgentKB(
	c *echo.Context, ctx context.Context, agentID, agentVersion string, body []byte,
) error {
	var req struct {
		KnowledgeBaseID    string `json:"knowledgeBaseId"`
		Description        string `json:"description"`
		KnowledgeBaseState string `json:"knowledgeBaseState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	assoc, err := h.Backend.AssociateAgentKnowledgeBase(
		ctx, agentID, agentVersion, req.KnowledgeBaseID, req.Description, req.KnowledgeBaseState,
	)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentKnowledgeBase": assoc})
}

func (h *Handler) handleGetAgentKB(
	c *echo.Context, ctx context.Context, agentID, agentVersion, kbID string,
) error {
	assoc, err := h.Backend.GetAgentKnowledgeBase(ctx, agentID, agentVersion, kbID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentKnowledgeBase": assoc})
}

func (h *Handler) handleUpdateAgentKB(
	c *echo.Context, ctx context.Context, agentID, agentVersion, kbID string, body []byte,
) error {
	var req struct {
		Description        string `json:"description"`
		KnowledgeBaseState string `json:"knowledgeBaseState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	assoc, err := h.Backend.UpdateAgentKnowledgeBase(
		ctx, agentID, agentVersion, kbID, req.Description, req.KnowledgeBaseState,
	)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"agentKnowledgeBase": assoc})
}

func (h *Handler) handleDisassociateAgentKB(
	c *echo.Context, ctx context.Context, agentID, agentVersion, kbID string,
) error {
	if err := h.Backend.DisassociateAgentKnowledgeBase(ctx, agentID, agentVersion, kbID); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListAgentKBs(
	c *echo.Context, ctx context.Context, agentID, agentVersion string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	assocs, outToken, err := h.Backend.ListAgentKnowledgeBases(ctx, agentID, agentVersion, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"agentKnowledgeBaseSummaries": assocs,
		"nextToken":                   outToken,
	})
}

// ---------------------------------------------------------------------------
// Data source handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDS(c *echo.Context, ctx context.Context, kbID string, body []byte) error {
	var req struct {
		DataSourceConfiguration map[string]any `json:"dataSourceConfiguration"`
		VectorIngestionConfig   map[string]any `json:"vectorIngestionConfiguration"`
		Name                    string         `json:"name"`
		Description             string         `json:"description"`
		DataDeletionPolicy      string         `json:"dataDeletionPolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	ds, err := h.Backend.CreateDataSource(ctx, kbID, DataSourceConfig{
		Name:                    req.Name,
		Description:             req.Description,
		DataDeletionPolicy:      req.DataDeletionPolicy,
		DataSourceConfiguration: req.DataSourceConfiguration,
		VectorIngestionConfig:   req.VectorIngestionConfig,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"dataSource": ds})
}

func (h *Handler) handleGetDS(c *echo.Context, ctx context.Context, kbID, dsID string) error {
	ds, err := h.Backend.GetDataSource(ctx, kbID, dsID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"dataSource": ds})
}

func (h *Handler) handleUpdateDS(
	c *echo.Context, ctx context.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DataSourceConfiguration map[string]any `json:"dataSourceConfiguration"`
		VectorIngestionConfig   map[string]any `json:"vectorIngestionConfiguration"`
		Name                    string         `json:"name"`
		Description             string         `json:"description"`
		DataDeletionPolicy      string         `json:"dataDeletionPolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	ds, err := h.Backend.UpdateDataSource(ctx, kbID, dsID, DataSourceConfig{
		Name:                    req.Name,
		Description:             req.Description,
		DataDeletionPolicy:      req.DataDeletionPolicy,
		DataSourceConfiguration: req.DataSourceConfiguration,
		VectorIngestionConfig:   req.VectorIngestionConfig,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"dataSource": ds})
}

func (h *Handler) handleDeleteDS(c *echo.Context, ctx context.Context, kbID, dsID string) error {
	if err := h.Backend.DeleteDataSource(ctx, kbID, dsID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"dataSourceId":    dsID,
		"knowledgeBaseId": kbID,
		"status":          "DELETING",
	})
}

func (h *Handler) handleListDS(c *echo.Context, ctx context.Context, kbID string) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListDataSources(ctx, kbID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"dataSourceSummaries": summaries,
		"nextToken":           outToken,
	})
}

// ---------------------------------------------------------------------------
// Ingestion job handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleStartIngestionJob(
	c *echo.Context, ctx context.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	_ = json.Unmarshal(body, &req)

	job, err := h.Backend.StartIngestionJob(ctx, kbID, dsID, req.Description)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{"ingestionJob": job})
}

func (h *Handler) handleGetIngestionJob(
	c *echo.Context, ctx context.Context, kbID, dsID, jobID string,
) error {
	job, err := h.Backend.GetIngestionJob(ctx, kbID, dsID, jobID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ingestionJob": job})
}

func (h *Handler) handleStopIngestionJob(
	c *echo.Context, ctx context.Context, kbID, dsID, jobID string,
) error {
	job, err := h.Backend.StopIngestionJob(ctx, kbID, dsID, jobID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"ingestionJob": job})
}

func (h *Handler) handleListIngestionJobs(
	c *echo.Context, ctx context.Context, kbID, dsID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	jobs, outToken, err := h.Backend.ListIngestionJobs(ctx, kbID, dsID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ingestionJobSummaries": jobs,
		"nextToken":             outToken,
	})
}

// ---------------------------------------------------------------------------
// Flow handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFlow(c *echo.Context, ctx context.Context, body []byte) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Definition  map[string]any    `json:"definition"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
		RoleARN     string            `json:"executionRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	f, err := h.Backend.CreateFlow(ctx, FlowConfig{
		Name:        req.Name,
		Description: req.Description,
		RoleARN:     req.RoleARN,
		Definition:  req.Definition,
		Tags:        req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusCreated, f)
}

func (h *Handler) handleGetFlow(c *echo.Context, ctx context.Context, flowID string) error {
	f, err := h.Backend.GetFlow(ctx, flowID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, f)
}

func (h *Handler) handleUpdateFlow(
	c *echo.Context, ctx context.Context, flowID string, body []byte,
) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Definition  map[string]any    `json:"definition"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
		RoleARN     string            `json:"executionRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	f, err := h.Backend.UpdateFlow(ctx, flowID, FlowConfig{
		Name:        req.Name,
		Description: req.Description,
		RoleARN:     req.RoleARN,
		Definition:  req.Definition,
		Tags:        req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, f)
}

func (h *Handler) handleDeleteFlow(c *echo.Context, ctx context.Context, flowID string) error {
	if err := h.Backend.DeleteFlow(ctx, flowID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": flowID, "status": "Deleting"})
}

func (h *Handler) handleListFlows(c *echo.Context, ctx context.Context) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListFlows(ctx, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"flowSummaries": summaries, "nextToken": outToken})
}

func (h *Handler) handlePrepareFlow(c *echo.Context, ctx context.Context, flowID string) error {
	f, err := h.Backend.PrepareFlow(ctx, flowID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, f)
}

func (h *Handler) handleValidateFlowDef(c *echo.Context, ctx context.Context, body []byte) error {
	var req struct {
		Definition map[string]any `json:"definition"`
	}

	_ = json.Unmarshal(body, &req)

	errs, err := h.Backend.ValidateFlowDefinition(ctx, req.Definition)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"validations": errs})
}

// ---------------------------------------------------------------------------
// Flow version handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFlowVersion(
	c *echo.Context, ctx context.Context, flowID string, body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	_ = json.Unmarshal(body, &req)

	fv, err := h.Backend.CreateFlowVersion(ctx, flowID, req.Description)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusCreated, fv)
}

func (h *Handler) handleGetFlowVersion(
	c *echo.Context, ctx context.Context, flowID, flowVersion string,
) error {
	fv, err := h.Backend.GetFlowVersion(ctx, flowID, flowVersion)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, fv)
}

func (h *Handler) handleDeleteFlowVersion(
	c *echo.Context, ctx context.Context, flowID, flowVersion string,
) error {
	if err := h.Backend.DeleteFlowVersion(ctx, flowID, flowVersion); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": flowID, "version": flowVersion, "status": "Deleting"})
}

func (h *Handler) handleListFlowVersions(
	c *echo.Context, ctx context.Context, flowID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListFlowVersions(ctx, flowID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"flowVersionSummaries": summaries, "nextToken": outToken})
}

// ---------------------------------------------------------------------------
// Flow alias handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateFlowAlias(
	c *echo.Context, ctx context.Context, flowID string, body []byte,
) error {
	var req struct {
		Tags                 map[string]string  `json:"tags"`
		RoutingConfiguration []FlowAliasRouting `json:"routingConfiguration"`
		Name                 string             `json:"name"`
		Description          string             `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	al, err := h.Backend.CreateFlowAlias(ctx, flowID, FlowAliasConfig{
		Name:                 req.Name,
		Description:          req.Description,
		RoutingConfiguration: req.RoutingConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusCreated, al)
}

func (h *Handler) handleGetFlowAlias(
	c *echo.Context, ctx context.Context, flowID, aliasID string,
) error {
	al, err := h.Backend.GetFlowAlias(ctx, flowID, aliasID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, al)
}

func (h *Handler) handleUpdateFlowAlias(
	c *echo.Context, ctx context.Context, flowID, aliasID string, body []byte,
) error {
	var req struct {
		Tags                 map[string]string  `json:"tags"`
		RoutingConfiguration []FlowAliasRouting `json:"routingConfiguration"`
		Name                 string             `json:"name"`
		Description          string             `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	al, err := h.Backend.UpdateFlowAlias(ctx, flowID, aliasID, FlowAliasConfig{
		Name:                 req.Name,
		Description:          req.Description,
		RoutingConfiguration: req.RoutingConfiguration,
		Tags:                 req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, al)
}

func (h *Handler) handleDeleteFlowAlias(
	c *echo.Context, ctx context.Context, flowID, aliasID string,
) error {
	if err := h.Backend.DeleteFlowAlias(ctx, flowID, aliasID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": aliasID, "flowId": flowID})
}

func (h *Handler) handleListFlowAliases(
	c *echo.Context, ctx context.Context, flowID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListFlowAliases(ctx, flowID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"flowAliasSummaries": summaries, "nextToken": outToken})
}

// ---------------------------------------------------------------------------
// Prompt handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePrompt(c *echo.Context, ctx context.Context, body []byte) error {
	var req struct {
		Tags           map[string]string `json:"tags"`
		Variants       []map[string]any  `json:"variants"`
		Name           string            `json:"name"`
		Description    string            `json:"description"`
		DefaultVariant string            `json:"defaultVariant"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	p, err := h.Backend.CreatePrompt(ctx, PromptConfig{
		Name:           req.Name,
		Description:    req.Description,
		DefaultVariant: req.DefaultVariant,
		Variants:       req.Variants,
		Tags:           req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusCreated, p)
}

func (h *Handler) handleGetPrompt(c *echo.Context, ctx context.Context, promptID string) error {
	p, err := h.Backend.GetPrompt(ctx, promptID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleUpdatePrompt(
	c *echo.Context, ctx context.Context, promptID string, body []byte,
) error {
	var req struct {
		Tags           map[string]string `json:"tags"`
		Variants       []map[string]any  `json:"variants"`
		Name           string            `json:"name"`
		Description    string            `json:"description"`
		DefaultVariant string            `json:"defaultVariant"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	p, err := h.Backend.UpdatePrompt(ctx, promptID, PromptConfig{
		Name:           req.Name,
		Description:    req.Description,
		DefaultVariant: req.DefaultVariant,
		Variants:       req.Variants,
		Tags:           req.Tags,
	})
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleDeletePrompt(c *echo.Context, ctx context.Context, promptID string) error {
	if err := h.Backend.DeletePrompt(ctx, promptID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": promptID})
}

func (h *Handler) handleListPrompts(c *echo.Context, ctx context.Context) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListPrompts(ctx, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"promptSummaries": summaries, "nextToken": outToken})
}

// ---------------------------------------------------------------------------
// Prompt version handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreatePromptVersion(
	c *echo.Context, ctx context.Context, promptID string, body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	_ = json.Unmarshal(body, &req)

	pv, err := h.Backend.CreatePromptVersion(ctx, promptID, req.Description)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusCreated, pv)
}

func (h *Handler) handleGetPromptVersion(
	c *echo.Context, ctx context.Context, promptID, version string,
) error {
	pv, err := h.Backend.GetPromptVersion(ctx, promptID, version)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, pv)
}

func (h *Handler) handleDeletePromptVersion(
	c *echo.Context, ctx context.Context, promptID, version string,
) error {
	if err := h.Backend.DeletePromptVersion(ctx, promptID, version); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"id": promptID, "version": version})
}

// ---------------------------------------------------------------------------
// KB document handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleIngestKBDocs(
	c *echo.Context, ctx context.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		Documents []struct {
			Metadata map[string]any `json:"metadata"`
			Content  map[string]any `json:"content"`
			DocID    string         `json:"documentId"`
		} `json:"documents"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	docs := make([]KBDocument, 0, len(req.Documents))

	for _, d := range req.Documents {
		docs = append(docs, KBDocument{
			DocID:    d.DocID,
			Metadata: d.Metadata,
			Content:  d.Content,
		})
	}

	details, err := h.Backend.IngestKnowledgeBaseDocuments(ctx, kbID, dsID, docs)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{"documentDetails": details})
}

func (h *Handler) handleGetKBDocs(
	c *echo.Context, ctx context.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIDs []string `json:"documentIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	details, err := h.Backend.GetKnowledgeBaseDocuments(ctx, kbID, dsID, req.DocumentIDs)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"documentDetails": details})
}

func (h *Handler) handleDeleteKBDocs(
	c *echo.Context, ctx context.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIDs []string `json:"documentIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	details, err := h.Backend.DeleteKnowledgeBaseDocuments(ctx, kbID, dsID, req.DocumentIDs)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusAccepted, map[string]any{"documentDetails": details})
}

func (h *Handler) handleListKBDocs(
	c *echo.Context, ctx context.Context, kbID, dsID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	details, outToken, err := h.Backend.ListKnowledgeBaseDocuments(ctx, kbID, dsID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"documentDetails": details,
		"nextToken":       outToken,
	})
}

// ---------------------------------------------------------------------------
// Tag handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListTags(
	c *echo.Context, ctx context.Context, resourceARN string,
) error {
	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

func (h *Handler) handleTagResource(
	c *echo.Context, ctx context.Context, resourceARN string, body []byte,
) error {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	if err := h.Backend.TagResource(ctx, resourceARN, req.Tags); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(
	c *echo.Context, ctx context.Context, resourceARN string, query url.Values,
) error {
	tagKeys := query["tagKeys"]

	if err := h.Backend.UntagResource(ctx, resourceARN, tagKeys); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func handleErr(c *echo.Context, err error) error {
	var syntaxErr *json.SyntaxError

	var code string
	var status int

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		status = http.StatusNotFound
		code = "ResourceNotFoundException"
	case errors.Is(err, awserr.ErrAlreadyExists):
		status = http.StatusConflict
		code = "ConflictException"
	case errors.Is(err, awserr.ErrInvalidParameter):
		status = http.StatusBadRequest
		code = "ValidationException"
	case errors.As(err, &syntaxErr):
		status = http.StatusBadRequest
		code = "ValidationException"
	default:
		status = http.StatusInternalServerError
		code = "InternalServerException"
	}

	c.Response().Header().Set("X-Amzn-Errortype", code)

	return c.JSON(status, map[string]any{"message": err.Error()})
}

func errResp(code, msg string) map[string]any {
	return map[string]any{"__type": code, "message": msg}
}

func pageParams(query url.Values) (int, string) {
	maxResults := maxPageDefault
	nextToken := query.Get("nextToken")

	if mr := query.Get("maxResults"); mr != "" {
		_, _ = fmt.Sscanf(mr, "%d", &maxResults)
	}

	return maxResults, nextToken
}

// classifyPath returns the operation name from method+path (used by ExtractOperation).
func classifyPath(method, path string) string {
	path = strings.TrimSuffix(path, "/")

	switch {
	case path == agentsBase && isWrite(method):
		return opCreateAgent
	case path == agentsBase:
		return opListAgents
	case path == kbBase && isWrite(method):
		return opCreateKnowledgeBase
	case path == kbBase:
		return opListKnowledgeBases
	case path == flowsBase && method == http.MethodPost:
		return opCreateFlow
	case path == flowsBase:
		return opListFlows
	case path == promptsBase && method == http.MethodPost:
		return opCreatePrompt
	case path == promptsBase:
		return opListPrompts
	}

	return classifySubPath(method, path)
}

func classifySubPath(method, path string) string {
	switch {
	case strings.HasPrefix(path, agentsBase+"/"):
		return classifyAgentPath(method, path)
	case strings.HasPrefix(path, kbBase+"/"):
		return classifyKBPath(method, path)
	case strings.HasPrefix(path, flowsBase+"/"):
		return classifyFlowPath(method, path)
	case strings.HasPrefix(path, promptsBase+"/"):
		return classifyPromptPath(method, path)
	case strings.HasPrefix(path, tagsBase):
		return classifyTagPath(method)
	}

	return "Unknown"
}

func classifyAgentPath(method, path string) string {
	rest, _ := strings.CutPrefix(path, agentsBase+"/")
	segs := strings.Split(rest, "/")

	switch {
	case len(segs) == 1 && method == http.MethodGet:
		return opGetAgent
	case len(segs) == 1 && method == http.MethodPut:
		return opUpdateAgent
	case len(segs) == 1 && method == http.MethodDelete:
		return opDeleteAgent
	case len(segs) == 2 && segs[1] == "prepare":
		return opPrepareAgent
	case containsSeg(segs, "agentversions") && containsSeg(segs, "actiongroups"):
		return classifyActionGroupPath(method, segs)
	case containsSeg(segs, "agentversions") && containsSeg(segs, "agentcollaborators"):
		return classifyCollabPath(method, segs)
	case containsSeg(segs, "agentversions") && containsSeg(segs, "knowledgebases"):
		return classifyAgentKBPath(method, segs)
	case containsSeg(segs, "agentversions"):
		return classifyAgentVersionPath(method, segs)
	case containsSeg(segs, "agentaliases"):
		return classifyAliasPath(method, segs)
	}

	return "Unknown"
}

func classifyActionGroupPath(method string, segs []string) string {
	idx := indexOf(segs, "actiongroups")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPut, http.MethodPost:
			return opCreateAgentActionGroup
		case http.MethodGet:
			return opListAgentActionGroups
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentActionGroup
	case http.MethodPut:
		return opUpdateAgentActionGroup
	case http.MethodDelete:
		return opDeleteAgentActionGroup
	}

	return "Unknown"
}

func classifyCollabPath(method string, segs []string) string {
	idx := indexOf(segs, "agentcollaborators")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPut:
			return opAssociateAgentCollaborator
		case http.MethodGet:
			return opListAgentCollaborators
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentCollaborator
	case http.MethodPut:
		return opUpdateAgentCollaborator
	case http.MethodDelete:
		return opDisassociateAgentCollaborator
	}

	return "Unknown"
}

func classifyAgentKBPath(method string, segs []string) string {
	idx := indexOf(segs, "knowledgebases")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPut:
			return opAssociateAgentKnowledgeBase
		case http.MethodGet:
			return opListAgentKnowledgeBases
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentKnowledgeBase
	case http.MethodPut:
		return opUpdateAgentKnowledgeBase
	case http.MethodDelete:
		return opDisassociateAgentKnowledgeBase
	}

	return "Unknown"
}

func classifyAgentVersionPath(method string, segs []string) string {
	idx := indexOf(segs, "agentversions")
	hasVersionID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasVersionID {
		switch method {
		case http.MethodPost:
			return opCreateAgentVersion
		case http.MethodGet:
			return opListAgentVersions
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentVersion
	case http.MethodDelete:
		return opDeleteAgentVersion
	}

	return "Unknown"
}

func classifyAliasPath(method string, segs []string) string {
	idx := indexOf(segs, "agentaliases")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPost, http.MethodPut:
			return opCreateAgentAlias
		case http.MethodGet:
			return opListAgentAliases
		}
	}

	switch method {
	case http.MethodGet:
		return opGetAgentAlias
	case http.MethodPut:
		return opUpdateAgentAlias
	case http.MethodDelete:
		return opDeleteAgentAlias
	}

	return "Unknown"
}

func classifyKBPath(method, path string) string {
	rest, _ := strings.CutPrefix(path, kbBase+"/")
	segs := strings.Split(rest, "/")

	switch {
	case len(segs) == 1 && method == http.MethodGet:
		return opGetKnowledgeBase
	case len(segs) == 1 && method == http.MethodPut:
		return opUpdateKnowledgeBase
	case len(segs) == 1 && method == http.MethodDelete:
		return opDeleteKnowledgeBase
	case containsSeg(segs, "datasources"):
		return classifyDSPath(method, segs)
	}

	return "Unknown"
}

func classifyDSPath(method string, segs []string) string {
	idx := indexOf(segs, "datasources")
	hasDSID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasDSID {
		switch method {
		case http.MethodPut, http.MethodPost:
			return opCreateDataSource
		case http.MethodGet:
			return opListDataSources
		}
	}

	dsSuffix := ""

	if len(segs) > idx+splitTwo {
		dsSuffix = segs[idx+splitTwo]
	}

	return classifyDSSuffix(method, segs[idx+1], dsSuffix, segs)
}

func classifyDSSuffix(method, _, suffix string, segs []string) string {
	switch suffix {
	case "ingestionjobs":
		return classifyJobPath(method, segs)
	case "documents":
		return classifyDocPath(method, segs)
	case "":
		switch method {
		case http.MethodGet:
			return opGetDataSource
		case http.MethodPut:
			return opUpdateDataSource
		case http.MethodDelete:
			return opDeleteDataSource
		}
	}

	return "Unknown"
}

func classifyJobPath(method string, segs []string) string {
	idx := indexOf(segs, "ingestionjobs")
	hasJobID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasJobID {
		switch method {
		case http.MethodPut, http.MethodPost:
			return opStartIngestionJob
		case http.MethodGet:
			return opListIngestionJobs
		}
	}

	if len(segs) > idx+splitTwo && segs[idx+splitTwo] == "stop" {
		return opStopIngestionJob
	}

	return opGetIngestionJob
}

func classifyDocPath(method string, segs []string) string {
	idx := indexOf(segs, "documents")

	if len(segs) > idx+1 {
		switch segs[idx+1] {
		case "deleteDocuments":
			return opDeleteKnowledgeBaseDocuments
		case "getDocuments":
			return opGetKnowledgeBaseDocuments
		}
	}

	switch method {
	case http.MethodPost:
		return opIngestKnowledgeBaseDocuments
	case http.MethodGet:
		return opListKnowledgeBaseDocuments
	}

	return "Unknown"
}

func classifyFlowPath(method, path string) string {
	rest, _ := strings.CutPrefix(path, flowsBase+"/")
	segs := strings.Split(rest, "/")

	switch {
	case len(segs) == 1 && method == http.MethodGet:
		return opGetFlow
	case len(segs) == 1 && method == http.MethodPut:
		return opUpdateFlow
	case len(segs) == 1 && method == http.MethodDelete:
		return opDeleteFlow
	case len(segs) == 2 && segs[1] == "prepare":
		return opPrepareFlow
	case containsSeg(segs, "versions"):
		return classifyFlowVersionPath(method, segs)
	case containsSeg(segs, "aliases"):
		return classifyFlowAliasPath(method, segs)
	}

	return "Unknown"
}

func classifyFlowVersionPath(method string, segs []string) string {
	idx := indexOf(segs, "versions")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPost:
			return opCreateFlowVersion
		case http.MethodGet:
			return opListFlowVersions
		}
	}

	switch method {
	case http.MethodGet:
		return opGetFlowVersion
	case http.MethodDelete:
		return opDeleteFlowVersion
	}

	return "Unknown"
}

func classifyFlowAliasPath(method string, segs []string) string {
	idx := indexOf(segs, "aliases")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID {
		switch method {
		case http.MethodPost:
			return opCreateFlowAlias
		case http.MethodGet:
			return opListFlowAliases
		}
	}

	switch method {
	case http.MethodGet:
		return opGetFlowAlias
	case http.MethodPut:
		return opUpdateFlowAlias
	case http.MethodDelete:
		return opDeleteFlowAlias
	}

	return "Unknown"
}

func classifyPromptPath(method, path string) string {
	rest, _ := strings.CutPrefix(path, promptsBase+"/")
	segs := strings.Split(rest, "/")

	switch {
	case len(segs) == 1 && method == http.MethodGet:
		return opGetPrompt
	case len(segs) == 1 && method == http.MethodPut:
		return opUpdatePrompt
	case len(segs) == 1 && method == http.MethodDelete:
		return opDeletePrompt
	case containsSeg(segs, "versions"):
		return classifyPromptVersionPath(method, segs)
	}

	return "Unknown"
}

func classifyPromptVersionPath(method string, segs []string) string {
	idx := indexOf(segs, "versions")
	hasID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasID && method == http.MethodPost {
		return opCreatePromptVersion
	}

	switch method {
	case http.MethodGet:
		return opGetPromptVersion
	case http.MethodDelete:
		return opDeletePromptVersion
	}

	return "Unknown"
}

func classifyTagPath(method string) string {
	switch method {
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	}

	return "Unknown"
}

func isWrite(method string) bool {
	return method == http.MethodPost || method == http.MethodPut
}

func containsSeg(segs []string, seg string) bool {
	for _, s := range segs {
		if s == seg {
			return true
		}
	}

	return false
}

func indexOf(segs []string, seg string) int {
	for i, s := range segs {
		if s == seg {
			return i
		}
	}

	return -1
}
