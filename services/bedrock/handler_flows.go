package bedrock

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

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
	// PrepareFlow POSTs to the same "/flows/{flowIdentifier}/" path as
	// Get/Update/Delete -- botocore bedrock-agent 2023-06-05 has no
	// "/prepare" suffix; method alone disambiguates it from CreateFlow
	// (which POSTs to "/flows/" with no id).
	case suffix == "" && method == http.MethodPost:
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

	return c.JSON(http.StatusCreated, f)
}

func (h *AgentsHandler) handleGetFlow(c *echo.Context, flowID string) error {
	f, err := h.Backend.GetFlow(flowID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, f)
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

	return c.JSON(http.StatusOK, f)
}

func (h *AgentsHandler) handleDeleteFlow(c *echo.Context, flowID string) error {
	if err := h.Backend.DeleteFlow(flowID); err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusOK, map[string]any{keyID: flowID})
}

func (h *AgentsHandler) handlePrepareFlow(c *echo.Context, flowID string) error {
	f, err := h.Backend.PrepareFlow(flowID)
	if err != nil {
		return c.JSON(http.StatusNotFound, agentErrResp("ResourceNotFoundException", err.Error()))
	}

	return c.JSON(http.StatusAccepted, map[string]any{keyID: f.FlowID, keyStatus: f.Status})
}

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
