package bedrock

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Route path constants for batch-3 resources
// ---------------------------------------------------------------------------

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
)

// ---------------------------------------------------------------------------
// RouteMatcher extension — now also handles /flows, /prompts, /tags
// ---------------------------------------------------------------------------

// routeMatcherBatch3 returns true if the path matches any batch-3 routes.
// Called from the updated RouteMatcher.
// Note: /tags/ paths are only claimed when the ARN belongs to a bedrock-agent resource
// (arn:aws:bedrock-agent:…). Other services (FIS, etc.) own their own /tags/ routes.
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

// isBedrockAgentArn reports whether arn is a bedrock-agent-owned ARN.
func isBedrockAgentArn(arn string) bool {
	return strings.Contains(arn, ":bedrock-agent:")
}

// ---------------------------------------------------------------------------
// Dispatch entrypoints (called from the main dispatch function)
// ---------------------------------------------------------------------------

// dispatchFlowRoutes handles /flows and /flows/{flowId}/...
func (h *AgentsHandler) dispatchFlowRoutes(
	c *echo.Context, path, method string, body []byte,
) error {
	// Exact /flows
	if path == flowsPath {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreateFlow(c, body)
		case http.MethodGet:
			return h.handleListFlows(c)
		}
	}

	// /flows/validateFlowDefinition (POST)
	if (path == flowsPath+"/validateFlowDefinition" || path == flowsPath+"/validate-definition") &&
		method == http.MethodPost {
		return h.handleValidateFlowDefinition(c)
	}

	rest, ok := strings.CutPrefix(path, "/flows/")
	if !ok {
		return c.JSON(
			http.StatusNotFound,
			agentErrResp("UnknownOperationException", "unknown flow operation: "+path),
		)
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	flowID := parts[0]
	suffix := ""

	if len(parts) == splitInTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchFlowIDRoutes(c, flowID, suffix, method, body)
}

func (h *AgentsHandler) dispatchFlowIDRoutes(
	c *echo.Context, flowID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetFlow(c, flowID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdateFlow(c, flowID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeleteFlow(c, flowID)
	case suffix == "/prepare" && method == http.MethodPost:
		return h.handlePrepareFlow(c, flowID)
	case strings.HasPrefix(suffix, suffixAliases):
		return h.dispatchFlowAliasRoutes(c, flowID, suffix, method, body)
	case strings.HasPrefix(suffix, suffixVersions):
		return h.dispatchFlowVersionRoutes(c, flowID, suffix, method)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown flow operation"),
	)
}

func (h *AgentsHandler) dispatchFlowAliasRoutes(
	c *echo.Context, flowID, suffix, method string, body []byte,
) error {
	if suffix == suffixAliases {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreateFlowAlias(c, flowID, body)
		case http.MethodGet:
			return h.handleListFlowAliases(c, flowID)
		}
	}

	if aliasID, ok := strings.CutPrefix(suffix, suffixAliases+"/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetFlowAlias(c, flowID, aliasID)
		case http.MethodPut:
			return h.handleUpdateFlowAlias(c, flowID, aliasID, body)
		case http.MethodDelete:
			return h.handleDeleteFlowAlias(c, flowID, aliasID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown flow alias operation"),
	)
}

func (h *AgentsHandler) dispatchFlowVersionRoutes(
	c *echo.Context, flowID, suffix, method string,
) error {
	if suffix == suffixVersions {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreateFlowVersion(c, flowID)
		case http.MethodGet:
			return h.handleListFlowVersions(c, flowID)
		}
	}

	if ver, ok := strings.CutPrefix(suffix, suffixVersions+"/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetFlowVersion(c, flowID, ver)
		case http.MethodDelete:
			return h.handleDeleteFlowVersion(c, flowID, ver)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown flow version operation"),
	)
}

// dispatchPromptRoutes handles /prompts and /prompts/{promptId}/...
func (h *AgentsHandler) dispatchPromptRoutes(
	c *echo.Context, path, method string, body []byte,
) error {
	if path == promptsPath {
		switch method {
		case http.MethodPost, http.MethodPut:
			return h.handleCreatePrompt(c, body)
		case http.MethodGet:
			return h.handleListPrompts(c)
		}
	}

	rest, ok := strings.CutPrefix(path, "/prompts/")
	if !ok {
		return c.JSON(
			http.StatusNotFound,
			agentErrResp("UnknownOperationException", "unknown prompt operation: "+path),
		)
	}

	parts := strings.SplitN(rest, "/", splitInTwo)
	promptID := parts[0]
	suffix := ""

	if len(parts) == splitInTwo {
		suffix = "/" + parts[1]
	}

	return h.dispatchPromptIDRoutes(c, promptID, suffix, method, body)
}

func (h *AgentsHandler) dispatchPromptIDRoutes(
	c *echo.Context, promptID, suffix, method string, body []byte,
) error {
	switch {
	case suffix == "" && method == http.MethodGet:
		return h.handleGetPrompt(c, promptID)
	case suffix == "" && method == http.MethodPut:
		return h.handleUpdatePrompt(c, promptID, body)
	case suffix == "" && method == http.MethodDelete:
		return h.handleDeletePrompt(c, promptID)
	case strings.HasPrefix(suffix, suffixVersions):
		return h.dispatchPromptVersionRoutes(c, promptID, suffix, method)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown prompt operation"),
	)
}

func (h *AgentsHandler) dispatchPromptVersionRoutes(
	c *echo.Context, promptID, suffix, method string,
) error {
	if suffix == suffixVersions {
		switch method {
		case http.MethodPost:
			return h.handleCreatePromptVersion(c, promptID)
		case http.MethodGet:
			return h.handleListPromptVersions(c, promptID)
		}
	}

	if ver, ok := strings.CutPrefix(suffix, suffixVersions+"/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetPromptVersion(c, promptID, ver)
		case http.MethodDelete:
			return h.handleDeletePromptVersion(c, promptID, ver)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown prompt version operation"),
	)
}

// dispatchAgentVersionRoutes handles /agents/{agentId}/versions/...
func (h *AgentsHandler) dispatchAgentVersionRoutes(
	c *echo.Context, agentID, suffix, method string,
) error {
	if suffix == "/versions" && method == http.MethodGet {
		return h.handleListAgentVersions(c, agentID)
	}

	if suffix == "/versions" && method == http.MethodPost {
		return h.handleCreateAgentVersion(c, agentID)
	}

	if ver, ok := strings.CutPrefix(suffix, suffixVersions+"/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetAgentVersion(c, agentID, ver)
		case http.MethodDelete:
			return h.handleDeleteAgentVersion(c, agentID, ver)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown agent version operation"),
	)
}

func (h *AgentsHandler) dispatchCanonicalAgentVersionRoutes(
	c *echo.Context, agentID, suffix, method string,
) error {
	if suffix == "/agentversions" && method == http.MethodGet {
		return h.handleListAgentVersions(c, agentID)
	}

	version, ok := strings.CutPrefix(suffix, "/agentversions/")
	if !ok {
		return c.JSON(http.StatusNotFound, agentErrResp("UnknownOperationException", "unknown agent version operation"))
	}

	if version == agentStatusDraft && method == http.MethodPost {
		return h.handlePrepareAgent(c, agentID)
	}
	if method == http.MethodGet {
		return h.handleGetAgentVersion(c, agentID, version)
	}
	if method == http.MethodDelete {
		return h.handleDeleteAgentVersion(c, agentID, version)
	}

	return c.JSON(http.StatusNotFound, agentErrResp("UnknownOperationException", "unknown agent version operation"))
}

// dispatchAgentCollabRoutes handles /agents/{agentId}/agentversions/{v}/agentcollaborators/...
func (h *AgentsHandler) dispatchAgentCollabRoutes(
	c *echo.Context, agentID, suffix, method string, body []byte,
) error {
	// suffix looks like: /agentversions/{ver}/agentcollaborators or
	//                    /agentversions/{ver}/agentcollaborators/{id}
	collabSuffix := collabSuffixFrom(suffix)

	if collabSuffix == "" {
		return c.JSON(
			http.StatusNotFound,
			agentErrResp("UnknownOperationException", "malformed collaborator path"),
		)
	}

	if collabSuffix == "/agentcollaborators" {
		switch method {
		case http.MethodPost:
			return h.handleAssociateAgentCollaborator(c, agentID, body)
		case http.MethodGet:
			return h.handleListAgentCollaborators(c, agentID)
		}
	}

	if collabID, ok := strings.CutPrefix(collabSuffix, "/agentcollaborators/"); ok {
		switch method {
		case http.MethodGet:
			return h.handleGetAgentCollaborator(c, agentID, collabID)
		case http.MethodPut:
			return h.handleUpdateAgentCollaborator(c, agentID, collabID, body)
		case http.MethodDelete:
			return h.handleDisassociateAgentCollaborator(c, agentID, collabID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown collaborator operation"),
	)
}

// collabSuffixFrom extracts the /agentcollaborators[/...] part from an agent suffix.
func collabSuffixFrom(suffix string) string {
	idx := strings.Index(suffix, "/agentcollaborators")
	if idx < 0 {
		return ""
	}

	return suffix[idx:]
}

// dispatchTagRoutes handles /tags/{resourceArn} REST paths.
func (h *AgentsHandler) dispatchTagRoutes(
	c *echo.Context, path, method string, body []byte,
) error {
	resourceArn, ok := strings.CutPrefix(path, "/tags/")
	if !ok || resourceArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "missing resourceArn in path"),
		)
	}

	switch method {
	case http.MethodGet:
		return h.handleListTagsForAgentResource(c, resourceArn)
	case http.MethodPost:
		return h.handleTagAgentResource(c, resourceArn, body)
	case http.MethodDelete:
		return h.handleUntagAgentResource(c, resourceArn, body)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown tag operation"),
	)
}

func (h *AgentsHandler) dispatchDocumentOps(
	c *echo.Context, kbID, dsID, method string, body []byte,
) error {
	switch method {
	case http.MethodGet:
		return h.handleListKBDocuments(c, kbID, dsID)
	case http.MethodPost:
		return h.handleIngestKBDocuments(c, kbID, dsID, body)
	case http.MethodPut:
		return h.handleUpdateKBDocuments(c, kbID, dsID, body)
	case http.MethodDelete:
		return h.handleDeleteKBDocuments(c, kbID, dsID, body)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown documents operation"),
	)
}

// dispatchMemoryRoutes handles /agents/{agentId}/agentversions/{v}/memories/...
func (h *AgentsHandler) dispatchMemoryRoutes(
	c *echo.Context, agentID, suffix, method string,
) error {
	// Extract session from query param or simple path
	sessionID := c.QueryParam("sessionId")

	if sessionID == "" {
		// Try parsing /memories/{sessionId}
		if rest, ok := strings.CutPrefix(suffix, "/memories/"); ok {
			sessionID = rest
		}
	}

	switch method {
	case http.MethodGet:
		return h.handleGetAgentMemory(c, agentID, sessionID)
	case http.MethodDelete:
		return h.handleDeleteAgentMemory(c, agentID, sessionID)
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown memory operation"),
	)
}

// ---------------------------------------------------------------------------
// Flow handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreateFlow(c *echo.Context, body []byte) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	f, err := h.Backend.CreateFlow(req.Name, req.Description, req.Tags)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respFlow: f})
}

func (h *AgentsHandler) handleGetFlow(c *echo.Context, flowID string) error {
	f, err := h.Backend.GetFlow(flowID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respFlow: f})
}

func (h *AgentsHandler) handleListFlows(c *echo.Context) error {
	list, outToken := h.Backend.ListFlows(0, c.QueryParam("nextToken"))
	resp := map[string]any{"flowSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateFlow(
	c *echo.Context, flowID string, body []byte,
) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	f, err := h.Backend.UpdateFlow(flowID, req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respFlow: f})
}

func (h *AgentsHandler) handleDeleteFlow(c *echo.Context, flowID string) error {
	if err := h.Backend.DeleteFlow(flowID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{keyFlowID: flowID, keyStatus: statusDeleting})
}

func (h *AgentsHandler) handlePrepareFlow(c *echo.Context, flowID string) error {
	f, err := h.Backend.PrepareFlow(flowID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{keyFlowID: f.FlowID, keyStatus: f.Status})
}

// ---------------------------------------------------------------------------
// Flow alias handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreateFlowAlias(
	c *echo.Context, flowID string, body []byte,
) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	fa, err := h.Backend.CreateFlowAlias(flowID, req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respFlowAlias: fa})
}

func (h *AgentsHandler) handleGetFlowAlias(
	c *echo.Context, flowID, aliasID string,
) error {
	fa, err := h.Backend.GetFlowAlias(flowID, aliasID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respFlowAlias: fa})
}

func (h *AgentsHandler) handleListFlowAliases(c *echo.Context, flowID string) error {
	list, outToken := h.Backend.ListFlowAliases(flowID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"flowAliasSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateFlowAlias(
	c *echo.Context, flowID, aliasID string, body []byte,
) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	fa, err := h.Backend.UpdateFlowAlias(flowID, aliasID, req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respFlowAlias: fa})
}

func (h *AgentsHandler) handleDeleteFlowAlias(
	c *echo.Context, flowID, aliasID string,
) error {
	if err := h.Backend.DeleteFlowAlias(flowID, aliasID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyFlowID: flowID, keyFlowAliasID: aliasID, keyStatus: statusDeleting},
	)
}

// ---------------------------------------------------------------------------
// Flow version handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreateFlowVersion(c *echo.Context, flowID string) error {
	fv, err := h.Backend.CreateFlowVersion(flowID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respFlowVersion: fv})
}

func (h *AgentsHandler) handleGetFlowVersion(
	c *echo.Context, flowID, version string,
) error {
	fv, err := h.Backend.GetFlowVersion(flowID, version)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respFlowVersion: fv})
}

func (h *AgentsHandler) handleListFlowVersions(c *echo.Context, flowID string) error {
	list, outToken := h.Backend.ListFlowVersions(flowID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"flowVersionSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleDeleteFlowVersion(
	c *echo.Context, flowID, version string,
) error {
	if err := h.Backend.DeleteFlowVersion(flowID, version); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyFlowID: flowID, keyVersion: version, keyStatus: statusDeleting},
	)
}

// ---------------------------------------------------------------------------
// Validate flow definition handler
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleValidateFlowDefinition(c *echo.Context) error {
	validations, err := h.Backend.ValidateFlowDefinition()
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", err.Error()),
		)
	}

	return c.JSON(http.StatusOK, map[string]any{"validations": validations})
}

// ---------------------------------------------------------------------------
// Prompt handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreatePrompt(c *echo.Context, body []byte) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	p, err := h.Backend.CreatePrompt(req.Name, req.Description, req.Tags)
	if err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			return c.JSON(http.StatusConflict, agentErrResp("ConflictException", err.Error()))
		}

		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respPrompt: p})
}

func (h *AgentsHandler) handleGetPrompt(c *echo.Context, promptID string) error {
	p, err := h.Backend.GetPrompt(promptID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respPrompt: p})
}

func (h *AgentsHandler) handleListPrompts(c *echo.Context) error {
	list, outToken := h.Backend.ListPrompts(0, c.QueryParam("nextToken"))
	resp := map[string]any{"promptSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdatePrompt(
	c *echo.Context, promptID string, body []byte,
) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	p, err := h.Backend.UpdatePrompt(promptID, req.Name, req.Description)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respPrompt: p})
}

func (h *AgentsHandler) handleDeletePrompt(c *echo.Context, promptID string) error {
	if err := h.Backend.DeletePrompt(promptID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{keyPromptID: promptID, keyStatus: statusDeleting})
}

// ---------------------------------------------------------------------------
// Prompt version handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreatePromptVersion(c *echo.Context, promptID string) error {
	pv, err := h.Backend.CreatePromptVersion(promptID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusCreated, map[string]any{respPromptVersion: pv})
}

func (h *AgentsHandler) handleGetPromptVersion(
	c *echo.Context, promptID, version string,
) error {
	pv, err := h.Backend.GetPromptVersion(promptID, version)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respPromptVersion: pv})
}

func (h *AgentsHandler) handleListPromptVersions(c *echo.Context, promptID string) error {
	list, outToken := h.Backend.ListPromptVersions(promptID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"promptVersionSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleDeletePromptVersion(
	c *echo.Context, promptID, version string,
) error {
	if err := h.Backend.DeletePromptVersion(promptID, version); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyPromptID: promptID, keyVersion: version, keyStatus: statusDeleting},
	)
}

// ---------------------------------------------------------------------------
// Agent version handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleCreateAgentVersion(c *echo.Context, agentID string) error {
	av, err := h.Backend.CreateAgentVersion(agentID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{respAgentVersion: av})
}

func (h *AgentsHandler) handleGetAgentVersion(
	c *echo.Context, agentID, version string,
) error {
	av, err := h.Backend.GetAgentVersion(agentID, version)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respAgentVersion: av})
}

func (h *AgentsHandler) handleListAgentVersions(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentVersions(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentVersionSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleDeleteAgentVersion(
	c *echo.Context, agentID, version string,
) error {
	if err := h.Backend.DeleteAgentVersion(agentID, version); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(
		http.StatusAccepted,
		map[string]any{keyAgentID: agentID, keyVersion: version, opAgentStatusKey: statusDeleting},
	)
}

// ---------------------------------------------------------------------------
// Agent collaborator handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleAssociateAgentCollaborator(
	c *echo.Context, agentID string, body []byte,
) error {
	var req struct {
		AgentVersion      string `json:"agentVersion"`
		CollaboratorArn   string `json:"collaboratorArn"`
		RelayConversation string `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ac, err := h.Backend.AssociateAgentCollaborator(
		agentID, req.AgentVersion, req.CollaboratorArn, req.RelayConversation,
	)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: ac})
}

func (h *AgentsHandler) handleGetAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string,
) error {
	ac, err := h.Backend.GetAgentCollaborator(agentID, collaboratorID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: ac})
}

func (h *AgentsHandler) handleListAgentCollaborators(c *echo.Context, agentID string) error {
	list, outToken := h.Backend.ListAgentCollaborators(agentID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"agentCollaboratorSummaries": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleUpdateAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string, body []byte,
) error {
	var req struct {
		RelayConversation string `json:"relayConversationHistory"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	ac, err := h.Backend.UpdateAgentCollaborator(agentID, collaboratorID, req.RelayConversation)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respCollaborator: ac})
}

func (h *AgentsHandler) handleDisassociateAgentCollaborator(
	c *echo.Context, agentID, collaboratorID string,
) error {
	if err := h.Backend.DisassociateAgentCollaborator(agentID, collaboratorID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Knowledge base document handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleIngestKBDocuments(
	c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIDs []string `json:"documentIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	docs, err := h.Backend.IngestKnowledgeBaseDocuments(kbID, dsID, req.DocumentIDs)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{"documents": docs})
}

func (h *AgentsHandler) handleListKBDocuments(c *echo.Context, kbID, dsID string) error {
	list, outToken := h.Backend.ListKnowledgeBaseDocuments(kbID, dsID, 0, c.QueryParam("nextToken"))
	resp := map[string]any{"documentDetails": list}

	if outToken != "" {
		resp["nextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *AgentsHandler) handleGetKBDocuments(
	c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIDs []string `json:"documentIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", "invalid request body"))
	}

	docs, err := h.Backend.GetKnowledgeBaseDocuments(kbID, dsID, req.DocumentIDs)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{"documentDetails": docs})
}

func (h *AgentsHandler) handleDeleteKBDocuments(
	c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIDs []string `json:"documentIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	if err := h.Backend.DeleteKnowledgeBaseDocuments(kbID, dsID, req.DocumentIDs); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *AgentsHandler) handleUpdateKBDocuments(
	c *echo.Context, kbID, dsID string, body []byte,
) error {
	var req struct {
		DocumentIDs []string `json:"documentIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	docs, err := h.Backend.UpdateKnowledgeBaseDocuments(kbID, dsID, req.DocumentIDs)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{"documents": docs})
}

// ---------------------------------------------------------------------------
// Stop ingestion job handler
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleStopIngestionJob(
	c *echo.Context, kbID, dsID, jobID string,
) error {
	job, err := h.Backend.StopIngestionJob(kbID, dsID, jobID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{respIngestionJob: job})
}

// ---------------------------------------------------------------------------
// Tag handlers (agent-domain)
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleListTagsForAgentResource(
	c *echo.Context, resourceArn string,
) error {
	tags := h.Backend.ListAgentResourceTags(resourceArn)

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

func (h *AgentsHandler) handleTagAgentResource(
	c *echo.Context, resourceArn string, body []byte,
) error {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	if err := h.Backend.TagAgentResource(resourceArn, req.Tags); err != nil {
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *AgentsHandler) handleUntagAgentResource(
	c *echo.Context, resourceArn string, body []byte,
) error {
	var req struct {
		TagKeys []string `json:"tagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			agentErrResp("ValidationException", "invalid request body"),
		)
	}

	if err := h.Backend.UntagAgentResource(resourceArn, req.TagKeys); err != nil {
		return c.JSON(http.StatusBadRequest, agentErrResp("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// ---------------------------------------------------------------------------
// Agent memory handlers
// ---------------------------------------------------------------------------

func (h *AgentsHandler) handleGetAgentMemory(
	c *echo.Context, agentID, sessionID string,
) error {
	entries := h.Backend.GetAgentMemory(agentID, sessionID)

	return c.JSON(http.StatusOK, map[string]any{"memoryContents": entries})
}

func (h *AgentsHandler) handleDeleteAgentMemory(
	c *echo.Context, agentID, sessionID string,
) error {
	if err := h.Backend.DeleteAgentMemory(agentID, sessionID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.NoContent(http.StatusNoContent)
}
