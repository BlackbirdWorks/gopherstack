package bedrock

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchDocumentOps handles the .../documents collection path. The real
// bedrock-agent wire distinguishes IngestKnowledgeBaseDocuments and
// ListKnowledgeBaseDocuments by HTTP method alone (both share this exact
// path): Ingest is PUT, List is POST. GET is accepted too as harmless extra
// leniency (no real client sends it). GetKnowledgeBaseDocuments and
// DeleteKnowledgeBaseDocuments are real, but on the /getDocuments and
// /deleteDocuments sub-paths respectively -- both are carved out by the
// caller (dispatchDataSourceIDRoutes, handler_data_sources.go) before this
// function is ever reached, so dsSuffix here is always exactly "/documents".
func (h *AgentsHandler) dispatchDocumentOps(
	c *echo.Context, kbID, dsID, dsSuffix, method string, body []byte,
) error {
	if dsSuffix == suffixDocuments {
		switch method {
		case http.MethodPut:
			return h.handleIngestKBDocuments(c, kbID, dsID, body)
		case http.MethodPost, http.MethodGet:
			return h.handleListKBDocuments(c, kbID, dsID)
		}
	}

	return c.JSON(
		http.StatusNotFound,
		agentErrResp("UnknownOperationException", "unknown documents operation"),
	)
}

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
