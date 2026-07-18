package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Data source handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleCreateDS(ctx context.Context, c *echo.Context, kbID string, body []byte) error {
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

	return c.JSON(http.StatusOK, map[string]any{keyDataSource: ds})
}

func (h *Handler) handleGetDS(ctx context.Context, c *echo.Context, kbID, dsID string) error {
	ds, err := h.Backend.GetDataSource(ctx, kbID, dsID)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyDataSource: ds})
}

func (h *Handler) handleUpdateDS(
	ctx context.Context, c *echo.Context, kbID, dsID string, body []byte,
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

	return c.JSON(http.StatusOK, map[string]any{keyDataSource: ds})
}

func (h *Handler) handleDeleteDS(ctx context.Context, c *echo.Context, kbID, dsID string) error {
	if err := h.Backend.DeleteDataSource(ctx, kbID, dsID); err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"dataSourceId":    dsID,
		"knowledgeBaseId": kbID,
		keyStatus:         statusDeleting,
	})
}

func (h *Handler) handleListDS(ctx context.Context, c *echo.Context, kbID string) error {
	maxResults, nextToken := pageParams(c.Request().URL.Query())

	summaries, outToken, err := h.Backend.ListDataSources(ctx, kbID, maxResults, nextToken)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"dataSourceSummaries": summaries,
		keyNextToken:          outToken,
	})
}

func classifyDSPath(method string, segs []string) string {
	idx := indexOf(segs, "datasources")
	hasDSID := len(segs) > idx+1 && segs[idx+1] != ""

	if !hasDSID {
		switch method {
		case http.MethodPut:
			return opCreateDataSource
		case http.MethodPost, http.MethodGet:
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

	return opUnknown
}
