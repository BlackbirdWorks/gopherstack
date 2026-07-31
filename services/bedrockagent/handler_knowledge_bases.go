package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Knowledge base handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateKB(ctx context.Context, c *echo.Context, body []byte) error {
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

	return c.JSON(http.StatusOK, map[string]any{keyKnowledgeBase: kb})
}

func (h *Handler) handleGetKB(ctx context.Context, c *echo.Context, kbID string) error {
	kb, err := h.Backend.GetKnowledgeBase(ctx, kbID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyKnowledgeBase: kb})
}

func (h *Handler) handleUpdateKB(ctx context.Context, c *echo.Context, kbID string, body []byte) error {
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

	return c.JSON(http.StatusOK, map[string]any{keyKnowledgeBase: kb})
}

func (h *Handler) handleDeleteKB(ctx context.Context, c *echo.Context, kbID string) error {
	if err := h.Backend.DeleteKnowledgeBase(ctx, kbID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"knowledgeBaseId": kbID, keyStatus: statusDeleting})
}

func (h *Handler) handleListKBs(ctx context.Context, c *echo.Context) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListKnowledgeBases(ctx, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"knowledgeBaseSummaries": summaries,
		keyNextToken:             outToken,
	})
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

	return opUnknown
}

// ---------------------------------------------------------------------------
// KB document handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleIngestKBDocs(
	ctx context.Context, c *echo.Context, kbID, dsID string, body []byte,
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

	return c.JSON(http.StatusAccepted, map[string]any{keyDocumentDetails: details})
}

func (h *Handler) handleGetKBDocs(
	ctx context.Context, c *echo.Context, kbID, dsID string, body []byte,
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

	return c.JSON(http.StatusOK, map[string]any{keyDocumentDetails: details})
}

func (h *Handler) handleDeleteKBDocs(
	ctx context.Context, c *echo.Context, kbID, dsID string, body []byte,
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

	return c.JSON(http.StatusAccepted, map[string]any{keyDocumentDetails: details})
}

func (h *Handler) handleListKBDocs(
	ctx context.Context, c *echo.Context, kbID, dsID string,
) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	details, outToken, err := h.Backend.ListKnowledgeBaseDocuments(ctx, kbID, dsID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDocumentDetails: details,
		keyNextToken:       outToken,
	})
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
	case http.MethodPut:
		return opIngestKnowledgeBaseDocuments
	case http.MethodPost, http.MethodGet:
		return opListKnowledgeBaseDocuments
	}

	return opUnknown
}
