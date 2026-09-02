package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// workflowParameterInput mirrors types.WorkflowParameter's real JSON keys
// (confirmed via awsRestjson1_deserializeDocumentWorkflowParameter).
type workflowParameterInput struct {
	Description string `json:"description"`
	Optional    bool   `json:"optional"`
}

func toWorkflowParameterTemplate(in map[string]workflowParameterInput) map[string]WorkflowParameter {
	if in == nil {
		return nil
	}

	out := make(map[string]WorkflowParameter, len(in))
	for name, p := range in {
		out[name] = WorkflowParameter(p)
	}

	return out
}

func (h *Handler) handleCreateWorkflow(c *echo.Context) error {
	var req struct {
		Tags              map[string]string                 `json:"tags"`
		ParameterTemplate map[string]workflowParameterInput `json:"parameterTemplate"`
		StorageCapacity   *int                              `json:"storageCapacity"`
		Name              string                            `json:"name"`
		Description       string                            `json:"description"`
		Engine            string                            `json:"engine"`
		DefinitionURI     string                            `json:"definitionUri"`
		StorageType       string                            `json:"storageType"`
		DefinitionZip     []byte                            `json:"definitionZip"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	wf, err := h.Backend.CreateWorkflow(CreateWorkflowInput{
		Name:              req.Name,
		Description:       req.Description,
		DefinitionZip:     string(req.DefinitionZip),
		DefinitionURI:     req.DefinitionURI,
		Engine:            req.Engine,
		StorageType:       req.StorageType,
		StorageCapacity:   req.StorageCapacity,
		ParameterTemplate: toWorkflowParameterTemplate(req.ParameterTemplate),
		Tags:              req.Tags,
	})
	if err != nil {
		return h.mapError(c, err)
	}

	// Real CreateWorkflowOutput: arn/id/status/tags plus the optional uuid
	// field (gopherstack-fedo).
	return c.JSON(http.StatusCreated, map[string]any{
		keyArn:    wf.Arn,
		"id":      wf.ID,
		keyStatus: wf.Status,
		keyUUID:   wf.UUID,
		keyTags:   wf.Tags,
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

	return c.JSON(http.StatusOK, map[string]any{keyItems: workflows, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflow(c *echo.Context, id string) error {
	var req struct {
		StorageCapacity *int   `json:"storageCapacity"`
		Name            string `json:"name"`
		Description     string `json:"description"`
		StorageType     string `json:"storageType"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateWorkflow(
		id,
		req.Name,
		req.Description,
		req.StorageType,
		req.StorageCapacity,
	); err != nil {
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
		Tags              map[string]string                 `json:"tags"`
		ParameterTemplate map[string]workflowParameterInput `json:"parameterTemplate"`
		StorageCapacity   *int                              `json:"storageCapacity"`
		VersionName       string                            `json:"versionName"`
		Description       string                            `json:"description"`
		StorageType       string                            `json:"storageType"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	wv, err := h.Backend.CreateWorkflowVersion(CreateWorkflowVersionInput{
		WorkflowID:        workflowID,
		VersionName:       req.VersionName,
		Description:       req.Description,
		StorageType:       req.StorageType,
		StorageCapacity:   req.StorageCapacity,
		ParameterTemplate: toWorkflowParameterTemplate(req.ParameterTemplate),
		Tags:              req.Tags,
	})
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

	return c.JSON(http.StatusOK, map[string]any{keyItems: versions, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflowVersion(
	c *echo.Context,
	workflowID, versionName string,
) error {
	var req struct {
		StorageCapacity *int   `json:"storageCapacity"`
		Description     string `json:"description"`
		StorageType     string `json:"storageType"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	err := h.Backend.UpdateWorkflowVersion(
		workflowID, versionName, req.Description, req.StorageType, req.StorageCapacity,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
