package bedrock

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ---------------------------------------------------------------------------
// Bedrock Agents handler
// ---------------------------------------------------------------------------

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
		"DeletePromptVersion",
		"GetPromptVersion",
		"ListPromptVersions",
		// Agent versions
		"CreateAgentVersion",
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
		"UpdateKnowledgeBaseDocuments",
		// Ingestion job management
		"StopIngestionJob",
		// Resource tags (agent-domain)
		opTagResource,
		opUntagResource,
		opListTagsForResource,
		// Agent memory
		"GetAgentMemory",
		"DeleteAgentMemory",
	}
}

// ChaosServiceName returns the chaos service name.
func (h *AgentsHandler) ChaosServiceName() string { return "bedrock-agent" }

// ChaosOperations returns all supported operations.
func (h *AgentsHandler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the supported regions.
func (h *AgentsHandler) ChaosRegions() []string { return []string{h.Backend.region} }

// Reset clears all agent/kb state.
func (h *AgentsHandler) Reset() {
	h.Backend.mu.Lock("AgentsHandler.Reset")
	defer h.Backend.mu.Unlock()

	h.Backend.agents = make(map[string]*Agent)
	h.Backend.agentsByName = make(map[string]string)
	h.Backend.agentActionGroups = make(map[string]*AgentActionGroup)
	h.Backend.agentAliases = make(map[string]*AgentAlias)
	h.Backend.agentKBAssociations = make(map[string]*AgentKnowledgeBaseAssociation)
	h.Backend.knowledgeBases = make(map[string]*KnowledgeBase)
	h.Backend.kbByName = make(map[string]string)
	h.Backend.dataSources = make(map[string]*DataSource)
	h.Backend.ingestionJobs = make(map[string]*IngestionJob)
	h.Backend.flows = make(map[string]*Flow)
	h.Backend.flowsByName = make(map[string]string)
	h.Backend.flowAliases = make(map[string]*FlowAlias)
	h.Backend.flowVersions = make(map[string]map[string]*FlowVersion)
	h.Backend.flowVersionCounters = make(map[string]int)
	h.Backend.prompts = make(map[string]*Prompt)
	h.Backend.promptsByName = make(map[string]string)
	h.Backend.promptVersions = make(map[string]map[string]*PromptVersion)
	h.Backend.promptVersionCounters = make(map[string]int)
	h.Backend.agentVersions = make(map[string]map[string]*AgentVersion)
	h.Backend.agentVersionCounters = make(map[string]int)
	h.Backend.agentCollaborators = make(map[string]map[string]*AgentCollaborator)
	h.Backend.kbDocuments = make(map[string]*KnowledgeBaseDocument)
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
			routeMatcherBatch3(path)
	}
}

// MatchPriority returns the routing priority.
func (h *AgentsHandler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the operation name from the request.
//
//nolint:cyclop,gocyclo // large dispatch switch is inherently complex for agents routing
func (h *AgentsHandler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

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
	case strings.HasSuffix(path, suffixAgentAliases) && method == http.MethodPost:
		return "CreateAgentAlias"
	case strings.HasSuffix(path, suffixAgentAliases) && method == http.MethodGet:
		return "ListAgentAliases"
	case strings.Contains(path, "/agentversions/") &&
		strings.Contains(path, "/knowledgebases") && method == http.MethodGet:
		return "ListAgentKnowledgeBases"
	case strings.Contains(path, "/agentversions/") &&
		strings.HasSuffix(path, "/knowledgebases") && method == http.MethodPut:
		return "AssociateAgentKnowledgeBase"
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

	return "Unknown"
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
//
//nolint:cyclop // agent sub-route dispatch is inherently branchy
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
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/knowledgebases"):
		return h.dispatchAgentKBRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/agentcollaborators"):
		return h.dispatchAgentCollabRoutes(c, agentID, suffix, method, body)
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/memories"):
		return h.dispatchMemoryRoutes(c, agentID, suffix, method)
	case strings.HasPrefix(suffix, "/agentversions/") &&
		strings.Contains(suffix, "/actiongroups"):
		return h.dispatchCanonicalActionGroupRoutes(c, agentID, suffix, method, body)
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

func (h *AgentsHandler) dispatchCanonicalActionGroupRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	_, rest, ok := strings.Cut(suffix, "/actiongroups")
	if !ok {
		return c.JSON(http.StatusNotFound, agentErrResp("UnknownOperationException", "unknown action group operation"))
	}

	switch {
	case rest == "" && method == http.MethodPut:
		return h.handleCreateAgentActionGroup(c, agentID, body)
	case rest == "" && method == http.MethodGet:
		return h.handleListAgentActionGroups(c, agentID)
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return h.handleGetAgentActionGroup(c, agentID, strings.TrimPrefix(rest, "/"))
	case strings.HasPrefix(rest, "/") && method == http.MethodPut:
		return h.handleUpdateAgentActionGroup(c, agentID, strings.TrimPrefix(rest, "/"), body)
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return h.handleDeleteAgentActionGroup(c, agentID, strings.TrimPrefix(rest, "/"))
	}

	return c.JSON(http.StatusNotFound, agentErrResp("UnknownOperationException", "unknown action group operation"))
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

// ---------------------------------------------------------------------------
// Agent handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreateAgent(c *echo.Context, body []byte) error {
	var req struct {
		Tags                   map[string]string `json:"tags"`
		GuardrailConfiguration map[string]any    `json:"guardrailConfiguration"`
		MemoryConfiguration    map[string]any    `json:"memoryConfiguration"`
		AgentName              string            `json:"agentName"`
		AgentCollaboration     string            `json:"agentCollaboration"`
		Description            string            `json:"description"`
		FoundationModel        string            `json:"foundationModel"`
		Instruction            string            `json:"instruction"`
		AgentResourceRole      string            `json:"agentResourceRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ag, err := h.Backend.CreateAgentWithConfiguration(AgentConfiguration{
		Tags:                   req.Tags,
		GuardrailConfiguration: req.GuardrailConfiguration,
		MemoryConfiguration:    req.MemoryConfiguration,
		AgentName:              req.AgentName,
		AgentCollaboration:     req.AgentCollaboration,
		Description:            req.Description,
		FoundationModel:        req.FoundationModel,
		Instruction:            req.Instruction,
		RoleArn:                req.AgentResourceRole,
	})
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respAgent: ag})
}

func (h *AgentsHandler) handleGetAgent(c *echo.Context, agentID string) error {
	ag, err := h.Backend.GetAgent(agentID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgent: ag})
}

func (h *AgentsHandler) handleListAgents(c *echo.Context) error {
	list, outToken := h.Backend.ListAgents(0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgent(c *echo.Context, agentID string, body []byte) error {
	var req struct {
		GuardrailConfiguration map[string]any `json:"guardrailConfiguration"`
		MemoryConfiguration    map[string]any `json:"memoryConfiguration"`
		AgentName              string         `json:"agentName"`
		AgentCollaboration     string         `json:"agentCollaboration"`
		Description            string         `json:"description"`
		FoundationModel        string         `json:"foundationModel"`
		Instruction            string         `json:"instruction"`
		AgentResourceRole      string         `json:"agentResourceRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ag, err := h.Backend.UpdateAgentWithConfiguration(agentID, AgentConfiguration{
		GuardrailConfiguration: req.GuardrailConfiguration,
		MemoryConfiguration:    req.MemoryConfiguration,
		AgentName:              req.AgentName,
		AgentCollaboration:     req.AgentCollaboration,
		Description:            req.Description,
		FoundationModel:        req.FoundationModel,
		Instruction:            req.Instruction,
		RoleArn:                req.AgentResourceRole,
	})
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgent: ag})
}

func (h *AgentsHandler) handleDeleteAgent(c *echo.Context, agentID string) error {
	if err := h.Backend.DeleteAgent(agentID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusAccepted,
		map[string]any{keyAgentID: agentID, opAgentStatusKey: statusDeleting},
	)
}

func (h *AgentsHandler) handlePrepareAgent(c *echo.Context, agentID string) error {
	ag, err := h.Backend.PrepareAgent(agentID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusAccepted,
		map[string]any{
			keyAgentID:       ag.AgentID,
			opAgentStatusKey: ag.AgentStatus,
			"agentVersion":   ag.AgentVersion,
		},
	)
}

// ---------------------------------------------------------------------------
// Action group routes dispatch
// ---------------------------------------------------------------------------

func (h *AgentsHandler) dispatchActionGroupRoutes(
	c *echo.Context,
	agentID, suffix, method string,
	body []byte,
) error {
	// suffix is like /action-groups or /action-groups/{agentVersion}/{id}
	if suffix == "/action-groups" {
		switch method {
		case http.MethodPost:
			return h.handleCreateAgentActionGroup(c, agentID, body)
		case http.MethodGet:
			return h.handleListAgentActionGroups(c, agentID)
		}
	}

	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodGet {
		// /action-groups/{agentVersion}/{actionGroupId}
		rest := strings.TrimPrefix(suffix, "/action-groups/")
		parts := strings.SplitN(rest, "/", splitInTwo)
		if len(parts) == splitInTwo {
			return h.handleGetAgentActionGroup(c, agentID, parts[1])
		}
	}

	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodPut {
		rest := strings.TrimPrefix(suffix, "/action-groups/")
		parts := strings.SplitN(rest, "/", splitInTwo)
		if len(parts) == splitInTwo {
			return h.handleUpdateAgentActionGroup(c, agentID, parts[1], body)
		}
	}

	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodDelete {
		rest := strings.TrimPrefix(suffix, "/action-groups/")
		parts := strings.SplitN(rest, "/", splitInTwo)
		if len(parts) == splitInTwo {
			return h.handleDeleteAgentActionGroup(c, agentID, parts[1])
		}
	}

	// List action groups: GET /action-groups/{agentVersion}
	if strings.HasPrefix(suffix, "/action-groups/") && method == http.MethodGet {
		return h.handleListAgentActionGroups(c, agentID)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown action group operation"),
	)
}

func (h *AgentsHandler) handleCreateAgentActionGroup(
	c *echo.Context,
	agentID string,
	body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ag, err := h.Backend.CreateAgentActionGroupWithSchemas(
		agentID,
		req.ActionGroupName,
		req.Description,
		req.ActionGroupExecutor,
		req.APISchema,
		req.FunctionSchema,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentActionGroup: ag})
}

func (h *AgentsHandler) handleGetAgentActionGroup(
	c *echo.Context,
	agentID, actionGroupID string,
) error {
	ag, err := h.Backend.GetAgentActionGroup(agentID, actionGroupID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentActionGroup: ag})
}

func (h *AgentsHandler) handleListAgentActionGroups(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentActionGroups(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"actionGroupSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgentActionGroup(
	c *echo.Context,
	agentID, actionGroupID string,
	body []byte,
) error {
	var req struct {
		ActionGroupExecutor map[string]any `json:"actionGroupExecutor"`
		APISchema           map[string]any `json:"apiSchema"`
		FunctionSchema      map[string]any `json:"functionSchema"`
		ActionGroupName     string         `json:"actionGroupName"`
		Description         string         `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ag, err := h.Backend.UpdateAgentActionGroupWithSchemas(
		agentID,
		actionGroupID,
		req.Description,
		req.ActionGroupExecutor,
		req.APISchema,
		req.FunctionSchema,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentActionGroup: ag})
}

func (h *AgentsHandler) handleDeleteAgentActionGroup(
	c *echo.Context,
	agentID, actionGroupID string,
) error {
	if err := h.Backend.DeleteAgentActionGroup(agentID, actionGroupID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// ---------------------------------------------------------------------------
// Alias routes
// ---------------------------------------------------------------------------

func (h *AgentsHandler) dispatchAliasRoutes(
	c *echo.Context,
	agentID, suffix, method string,
	body []byte,
) error {
	if suffix == suffixAgentAliases && (method == http.MethodPost || method == http.MethodPut) {
		return h.handleCreateAgentAlias(c, agentID, body)
	}

	if suffix == suffixAgentAliases && method == http.MethodGet {
		return h.handleListAgentAliases(c, agentID)
	}

	if aliasID, aliasOK := strings.CutPrefix(suffix, suffixAgentAliases+"/"); aliasOK {
		switch method {
		case http.MethodGet:
			return h.handleGetAgentAlias(c, agentID, aliasID)
		case http.MethodPut:
			return h.handleUpdateAgentAlias(c, agentID, aliasID, body)
		case http.MethodDelete:
			return h.handleDeleteAgentAlias(c, agentID, aliasID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown alias operation"),
	)
}

func (h *AgentsHandler) handleCreateAgentAlias(c *echo.Context, agentID string, body []byte) error {
	var req struct {
		AgentAliasName       string              `json:"agentAliasName"`
		RoutingConfiguration []AgentAliasRouting `json:"routingConfiguration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	alias, err := h.Backend.CreateAgentAlias(agentID, req.AgentAliasName, aliasVersion(req.RoutingConfiguration))
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respAgentAlias: alias})
}

func (h *AgentsHandler) handleGetAgentAlias(c *echo.Context, agentID, aliasID string) error {
	alias, err := h.Backend.GetAgentAlias(agentID, aliasID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentAlias: alias})
}

func (h *AgentsHandler) handleListAgentAliases(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentAliases(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentAliasSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgentAlias(
	c *echo.Context,
	agentID, aliasID string,
	body []byte,
) error {
	var req struct {
		AgentAliasName       string              `json:"agentAliasName"`
		RoutingConfiguration []AgentAliasRouting `json:"routingConfiguration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	alias, err := h.Backend.UpdateAgentAlias(
		agentID,
		aliasID,
		req.AgentAliasName,
		aliasVersion(req.RoutingConfiguration),
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentAlias: alias})
}

func aliasVersion(config []AgentAliasRouting) string {
	if len(config) == 0 {
		return ""
	}

	return config[0].AgentVersion
}

func (h *AgentsHandler) handleDeleteAgentAlias(c *echo.Context, agentID, aliasID string) error {
	if err := h.Backend.DeleteAgentAlias(agentID, aliasID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusAccepted,
		map[string]any{
			keyAgentID:         agentID,
			"agentAliasId":     aliasID,
			"agentAliasStatus": statusDeleting,
		},
	)
}

// ---------------------------------------------------------------------------
// Agent KB association routes
// ---------------------------------------------------------------------------

func (h *AgentsHandler) dispatchAgentKBRoutes(
	c *echo.Context,
	agentID, suffix, method string,
	body []byte,
) error {
	// suffix like /agentversions/DRAFT/knowledgebases or /agentversions/DRAFT/knowledgebases/{kbId}
	if strings.HasSuffix(suffix, "/knowledgebases") && method == http.MethodPut {
		return h.handleAssociateAgentKB(c, agentID, body)
	}

	if strings.HasSuffix(suffix, "/knowledgebases") && method == http.MethodGet {
		return h.handleListAgentKBs(c, agentID)
	}

	// /agentversions/{version}/knowledgebases/{kbId}
	parts := strings.Split(suffix, "/knowledgebases/")
	if len(parts) == splitInTwo {
		kbID := parts[1]

		switch method {
		case http.MethodGet:
			return h.handleGetAgentKB(c, agentID, kbID)
		case http.MethodPut:
			return h.handleUpdateAgentKB(c, agentID, kbID, body)
		case http.MethodDelete:
			return h.handleDisassociateAgentKB(c, agentID, kbID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown kb operation"),
	)
}

func (h *AgentsHandler) handleAssociateAgentKB(c *echo.Context, agentID string, body []byte) error {
	var req struct {
		KnowledgeBaseID string `json:"knowledgeBaseId"`
		Description     string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	assoc, err := h.Backend.AssociateAgentKnowledgeBase(
		agentID,
		req.KnowledgeBaseID,
		req.Description,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentKnowledgeBase: assoc})
}

func (h *AgentsHandler) handleGetAgentKB(c *echo.Context, agentID, kbID string) error {
	assoc, err := h.Backend.GetAgentKnowledgeBase(agentID, kbID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentKnowledgeBase: assoc})
}

func (h *AgentsHandler) handleUpdateAgentKB(
	c *echo.Context, agentID, kbID string, body []byte,
) error {
	var req struct {
		Description        string `json:"description"`
		KnowledgeBaseState string `json:"knowledgeBaseState"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", "invalid request body"))
	}

	assoc, err := h.Backend.UpdateAgentKnowledgeBase(agentID, kbID, req.Description, req.KnowledgeBaseState)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentKnowledgeBase: assoc})
}

func (h *AgentsHandler) handleListAgentKBs(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentKnowledgeBases(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentKnowledgeBaseSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleDisassociateAgentKB(c *echo.Context, agentID, kbID string) error {
	if err := h.Backend.DisassociateAgentKnowledgeBase(agentID, kbID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Knowledge base handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreateKnowledgeBase(c *echo.Context, body []byte) error {
	var req struct {
		KnowledgeBaseConfiguration map[string]any    `json:"knowledgeBaseConfiguration"`
		StorageConfiguration       map[string]any    `json:"storageConfiguration"`
		Tags                       map[string]string `json:"tags"`
		Name                       string            `json:"name"`
		Description                string            `json:"description"`
		RoleArn                    string            `json:"roleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	kb, err := h.Backend.CreateKnowledgeBase(
		req.Name,
		req.Description,
		req.RoleArn,
		req.KnowledgeBaseConfiguration,
		req.StorageConfiguration,
		req.Tags,
	)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respKnowledgeBase: kb})
}

func (h *AgentsHandler) handleGetKnowledgeBase(c *echo.Context, kbID string) error {
	kb, err := h.Backend.GetKnowledgeBase(kbID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respKnowledgeBase: kb})
}

func (h *AgentsHandler) handleListKnowledgeBases(c *echo.Context) error {
	list, outToken := h.Backend.ListKnowledgeBases(0, c.QueryParam("nextToken"))
	resp := map[string]any{"knowledgeBaseSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateKnowledgeBase(c *echo.Context, kbID string, body []byte) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
		RoleArn     string `json:"roleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	kb, err := h.Backend.UpdateKnowledgeBase(kbID, req.Name, req.Description, req.RoleArn)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respKnowledgeBase: kb})
}

func (h *AgentsHandler) handleDeleteKnowledgeBase(c *echo.Context, kbID string) error {
	if err := h.Backend.DeleteKnowledgeBase(kbID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusAccepted,
		map[string]any{keyKnowledgeBaseID: kbID, keyStatus: statusDeleting},
	)
}

// ---------------------------------------------------------------------------
// Data source routes
// ---------------------------------------------------------------------------

//
//nolint:cyclop // large dispatch table for data source operations is inherently complex
func (h *AgentsHandler) dispatchDataSourceRoutes(
	c *echo.Context,
	kbID, suffix, method string,
	body []byte,
) error {
	if suffix == "/datasources" && (method == http.MethodPost || method == http.MethodPut) {
		return h.handleCreateDataSource(c, kbID, body)
	}

	if suffix == "/datasources" && method == http.MethodGet {
		return h.handleListDataSources(c, kbID)
	}

	if rest, dsOK := strings.CutPrefix(suffix, "/datasources/"); dsOK {
		parts := strings.SplitN(rest, "/", splitInTwo)
		dsID := parts[0]
		dsSuffix := ""

		if len(parts) > splitInTwo-1 {
			dsSuffix = "/" + parts[1]
		}

		switch {
		case dsSuffix == "" && method == http.MethodGet:
			return h.handleGetDataSource(c, kbID, dsID)
		case dsSuffix == "" && method == http.MethodPut:
			return h.handleUpdateDataSource(c, kbID, dsID, body)
		case dsSuffix == "" && method == http.MethodDelete:
			return h.handleDeleteDataSource(c, kbID, dsID)
		case dsSuffix == "/ingestionjobs" && method == http.MethodPost:
			return h.handleStartIngestionJob(c, kbID, dsID, body)
		case dsSuffix == "/ingestionjobs" && method == http.MethodGet:
			return h.handleListIngestionJobs(c, kbID, dsID)
		case strings.HasPrefix(dsSuffix, "/ingestionjobs/"):
			return h.dispatchIngestionJobRoutes(c, kbID, dsID, dsSuffix, method)
		case dsSuffix == "/documents/getDocuments" && method == http.MethodPost:
			return h.handleGetKBDocuments(c, kbID, dsID, body)
		case strings.HasPrefix(dsSuffix, "/documents"):
			return h.dispatchDocumentOps(c, kbID, dsID, method, body)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown data source operation"),
	)
}

func (h *AgentsHandler) dispatchIngestionJobRoutes(
	c *echo.Context,
	kbID, dsID, dsSuffix, method string,
) error {
	jobID := strings.TrimPrefix(dsSuffix, "/ingestionjobs/")
	// strip any further sub-path
	if idx := strings.Index(jobID, "/"); idx >= 0 {
		subPath := jobID[idx:]
		jobID = jobID[:idx]

		if subPath == "/stop" && method == http.MethodPost {
			return h.handleStopIngestionJob(c, kbID, dsID, jobID)
		}
	}

	if method == http.MethodGet {
		return h.handleGetIngestionJob(c, kbID, dsID, jobID)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown ingestion job operation"),
	)
}

func (h *AgentsHandler) handleCreateDataSource(c *echo.Context, kbID string, body []byte) error {
	var req struct {
		DataSourceConfiguration map[string]any `json:"dataSourceConfiguration"`
		VectorIngestionConfig   map[string]any `json:"vectorIngestionConfiguration"`
		Name                    string         `json:"name"`
		Description             string         `json:"description"`
		DataDeletionPolicy      string         `json:"dataDeletionPolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ds, err := h.Backend.CreateDataSourceWithConfiguration(
		kbID,
		req.Name,
		req.Description,
		req.DataDeletionPolicy,
		req.DataSourceConfiguration,
		req.VectorIngestionConfig,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDataSource: ds})
}

func (h *AgentsHandler) handleGetDataSource(c *echo.Context, kbID, dsID string) error {
	ds, err := h.Backend.GetDataSource(kbID, dsID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDataSource: ds})
}

func (h *AgentsHandler) handleListDataSources(c *echo.Context, kbID string) error {
	list, outToken := h.Backend.ListDataSources(kbID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"dataSourceSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateDataSource(
	c *echo.Context,
	kbID, dsID string,
	body []byte,
) error {
	var req struct {
		DataSourceConfiguration map[string]any `json:"dataSourceConfiguration"`
		VectorIngestionConfig   map[string]any `json:"vectorIngestionConfiguration"`
		Name                    string         `json:"name"`
		Description             string         `json:"description"`
		DataDeletionPolicy      string         `json:"dataDeletionPolicy"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ds, err := h.Backend.UpdateDataSourceWithConfiguration(
		kbID,
		dsID,
		req.Name,
		req.Description,
		req.DataDeletionPolicy,
		req.DataSourceConfiguration,
		req.VectorIngestionConfig,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respDataSource: ds})
}

func (h *AgentsHandler) handleDeleteDataSource(c *echo.Context, kbID, dsID string) error {
	if err := h.Backend.DeleteDataSource(kbID, dsID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{
			"dataSourceId":     dsID,
			"knowledgeBaseId":  kbID,
			"dataSourceStatus": "DELETING",
		},
	)
}

func (h *AgentsHandler) handleStartIngestionJob(
	c *echo.Context,
	kbID, dsID string,
	body []byte,
) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	job, err := h.Backend.StartIngestionJob(kbID, dsID, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respIngestionJob: job})
}

func (h *AgentsHandler) handleGetIngestionJob(c *echo.Context, kbID, dsID, jobID string) error {
	job, err := h.Backend.GetIngestionJob(kbID, dsID, jobID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respIngestionJob: job})
}

func (h *AgentsHandler) handleListIngestionJobs(c *echo.Context, kbID, dsID string) error {
	list, outToken := h.Backend.ListIngestionJobs(kbID, dsID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"ingestionJobSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func agentErrResp(code, msg string) map[string]any {
	return map[string]any{
		"message": msg,
		"__type":  code,
	}
}
