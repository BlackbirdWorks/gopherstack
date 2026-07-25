package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateWorkflow(c *echo.Context) error {
	var req struct {
		Tags          map[string]string `json:"tags"`
		Name          string            `json:"name"`
		Description   string            `json:"description"`
		Engine        string            `json:"engine"`
		DefinitionURI string            `json:"definitionUri"`
		DefinitionZip []byte            `json:"definitionZip"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	wf, err := h.Backend.CreateWorkflow(
		req.Name,
		req.Description,
		string(req.DefinitionZip),
		req.DefinitionURI,
		req.Engine,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	// Real CreateWorkflowOutput: arn/id/status/tags plus the optional uuid
	// field (gopherstack-fedo).
	return c.JSON(http.StatusCreated, map[string]any{
		"arn":    wf.Arn,
		"id":     wf.ID,
		"status": wf.Status,
		"uuid":   wf.UUID,
		keyTags:  wf.Tags,
	})
}

func (h *Handler) handleDeleteWorkflow(c *echo.Context, id string) error {
	if err := h.Backend.DeleteWorkflow(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetWorkflow(c *echo.Context, id string) error {
	wf, err := h.Backend.GetWorkflow(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, wf)
}

func (h *Handler) handleListWorkflows(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	q := c.Request().URL.Query()
	filter := &WorkflowFilter{Name: q.Get("name"), Type: q.Get("type")}
	workflows, next, err := h.Backend.ListWorkflows(filter, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"workflows": workflows, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflow(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateWorkflow(id, req.Name, req.Description); err != nil {
		return h.mapError(c, err)
	}

	wf, err := h.Backend.GetWorkflow(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, wf)
}

func (h *Handler) handleCreateWorkflowVersion(c *echo.Context, workflowID string) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		VersionName string            `json:"versionName"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	wv, err := h.Backend.CreateWorkflowVersion(
		workflowID,
		req.VersionName,
		req.Description,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, wv)
}

func (h *Handler) handleDeleteWorkflowVersion(
	c *echo.Context,
	workflowID, versionName string,
) error {
	if err := h.Backend.DeleteWorkflowVersion(workflowID, versionName); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetWorkflowVersion(c *echo.Context, workflowID, versionName string) error {
	wv, err := h.Backend.GetWorkflowVersion(workflowID, versionName)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, wv)
}

func (h *Handler) handleListWorkflowVersions(c *echo.Context, workflowID string) error {
	maxResults, nextToken := paginationQueryParams(c)
	filter := &WorkflowVersionFilter{Type: c.QueryParam("type")}
	versions, next, err := h.Backend.ListWorkflowVersions(workflowID, filter, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"workflowVersions": versions, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflowVersion(
	c *echo.Context,
	workflowID, versionName string,
) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateWorkflowVersion(workflowID, versionName, req.Description); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
