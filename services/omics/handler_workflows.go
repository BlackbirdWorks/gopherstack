package omics

import (
	"net/http"
	"strings"

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
		ContainerRegistry map[string]any                    `json:"containerRegistryMap"`
		StorageCapacity   *int                              `json:"storageCapacity"`
		Name              string                            `json:"name"`
		Description       string                            `json:"description"`
		Engine            string                            `json:"engine"`
		DefinitionURI     string                            `json:"definitionUri"`
		StorageType       string                            `json:"storageType"`
		ReadmeMarkdown    string                            `json:"readmeMarkdown"`
		ReadmeURI         string                            `json:"readmeUri"`
		RegistryMapURI    string                            `json:"containerRegistryMapUri"`
		ReadmePath        string                            `json:"readmePath"`
		BucketOwnerID     string                            `json:"workflowBucketOwnerId"`
		RequestID         string                            `json:"requestId"`
		Accelerators      string                            `json:"accelerators"`
		Main              string                            `json:"main"`
		DefinitionZip     []byte                            `json:"definitionZip"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	ctx := c.Request().Context()

	readme, err := h.readmeFromURI(ctx, req.ReadmeMarkdown, req.ReadmeURI)
	if err != nil {
		return h.mapError(c, err)
	}

	registryMap, err := h.registryMapFromURI(ctx, req.ContainerRegistry, req.RegistryMapURI)
	if err != nil {
		return h.mapError(c, err)
	}

	in := CreateWorkflowInput{
		Accelerators:         req.Accelerators,
		Main:                 req.Main,
		ContainerRegistryMap: registryMap,
		Name:                 req.Name,
		Description:          req.Description,
		DefinitionZip:        string(req.DefinitionZip),
		DefinitionURI:        req.DefinitionURI,
		Engine:               req.Engine,
		StorageType:          req.StorageType,
		StorageCapacity:      req.StorageCapacity,
		ParameterTemplate:    toWorkflowParameterTemplate(req.ParameterTemplate),
		Tags:                 req.Tags,

		ReadmeMarkdown:        readme,
		ReadmePath:            req.ReadmePath,
		WorkflowBucketOwnerID: req.BucketOwnerID,
	}

	wf, err := idemCreate(
		h.idem, opCreateWorkflow, req.RequestID, idemFingerprint(req),
		func(w *Workflow) string { return w.ID }, h.Backend.GetWorkflow,
		func() (*Workflow, error) { return h.Backend.CreateWorkflow(in) },
	)
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
	if err := h.Backend.CheckWorkflowAccess(id, c.QueryParam("type"), c.QueryParam("workflowOwnerId")); err != nil {
		return h.mapError(c, err)
	}

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

	// Real ListWorkflowsOutput's element (WorkflowListItem) is narrower than
	// GetWorkflowOutput -- see WorkflowSummary's doc comment.
	summaries := make([]WorkflowSummary, 0, len(workflows))
	for _, wf := range workflows {
		summaries = append(summaries, newWorkflowSummary(wf))
	}

	return c.JSON(http.StatusOK, map[string]any{keyItems: summaries, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflow(c *echo.Context, id string) error {
	var req struct {
		StorageCapacity *int   `json:"storageCapacity"`
		Name            string `json:"name"`
		Description     string `json:"description"`
		StorageType     string `json:"storageType"`
		ReadmeMarkdown  string `json:"readmeMarkdown"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateWorkflow(
		id,
		req.Name,
		req.Description,
		req.StorageType,
		req.ReadmeMarkdown,
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
		ContainerRegistry map[string]any                    `json:"containerRegistryMap"`
		StorageCapacity   *int                              `json:"storageCapacity"`
		VersionName       string                            `json:"versionName"`
		Description       string                            `json:"description"`
		StorageType       string                            `json:"storageType"`
		DefinitionURI     string                            `json:"definitionUri"`
		ReadmeMarkdown    string                            `json:"readmeMarkdown"`
		ReadmeURI         string                            `json:"readmeUri"`
		RegistryMapURI    string                            `json:"containerRegistryMapUri"`
		ReadmePath        string                            `json:"readmePath"`
		BucketOwnerID     string                            `json:"workflowBucketOwnerId"`
		RequestID         string                            `json:"requestId"`
		Accelerators      string                            `json:"accelerators"`
		Main              string                            `json:"main"`
		Engine            string                            `json:"engine"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	ctx := c.Request().Context()

	readme, err := h.readmeFromURI(ctx, req.ReadmeMarkdown, req.ReadmeURI)
	if err != nil {
		return h.mapError(c, err)
	}

	registryMap, err := h.registryMapFromURI(ctx, req.ContainerRegistry, req.RegistryMapURI)
	if err != nil {
		return h.mapError(c, err)
	}

	in := CreateWorkflowVersionInput{
		Accelerators:         req.Accelerators,
		Main:                 req.Main,
		Engine:               req.Engine,
		ContainerRegistryMap: registryMap,
		WorkflowID:           workflowID,
		VersionName:          req.VersionName,
		Description:          req.Description,
		StorageType:          req.StorageType,
		StorageCapacity:      req.StorageCapacity,
		ParameterTemplate:    toWorkflowParameterTemplate(req.ParameterTemplate),
		Tags:                 req.Tags,

		DefinitionURI:         req.DefinitionURI,
		ReadmeMarkdown:        readme,
		ReadmePath:            req.ReadmePath,
		WorkflowBucketOwnerID: req.BucketOwnerID,
	}

	wv, err := idemCreate(
		h.idem, opCreateWorkflowVersion, req.RequestID, idemFingerprint(req)+workflowID,
		func(v *WorkflowVersion) string { return parentKey(v.WorkflowID, v.VersionName) },
		func(key string) (*WorkflowVersion, error) {
			return h.Backend.GetWorkflowVersion(workflowID, versionNameOf(key))
		},
		func() (*WorkflowVersion, error) { return h.Backend.CreateWorkflowVersion(in) },
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		keyArn:        wv.Arn,
		keyStatus:     wv.Status,
		keyTags:       wv.Tags,
		keyUUID:       wv.UUID,
		"versionName": wv.VersionName,
		"workflowId":  wv.WorkflowID,
	})
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
	if err := h.Backend.CheckWorkflowAccess(
		workflowID, c.QueryParam("type"), c.QueryParam("workflowOwnerId"),
	); err != nil {
		return h.mapError(c, err)
	}

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

	// Real ListWorkflowVersionsOutput's element (WorkflowVersionListItem) is
	// narrower than GetWorkflowVersionOutput -- see WorkflowVersionSummary's
	// doc comment.
	summaries := make([]WorkflowVersionSummary, 0, len(versions))
	for _, wv := range versions {
		summaries = append(summaries, newWorkflowVersionSummary(wv))
	}

	return c.JSON(http.StatusOK, map[string]any{keyItems: summaries, keyNextToken: next})
}

func (h *Handler) handleUpdateWorkflowVersion(
	c *echo.Context,
	workflowID, versionName string,
) error {
	var req struct {
		StorageCapacity *int   `json:"storageCapacity"`
		Description     string `json:"description"`
		StorageType     string `json:"storageType"`
		ReadmeMarkdown  string `json:"readmeMarkdown"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	err := h.Backend.UpdateWorkflowVersion(
		workflowID, versionName, req.Description, req.StorageType, req.ReadmeMarkdown, req.StorageCapacity,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// versionNameOf extracts the version name from a parentKey(workflowID, versionName).
func versionNameOf(key string) string {
	_, name, _ := strings.Cut(key, "|")

	return name
}
