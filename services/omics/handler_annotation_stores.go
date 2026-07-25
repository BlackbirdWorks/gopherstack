package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateAnnotationStore(c *echo.Context) error {
	var req struct {
		Tags         map[string]string `json:"tags"`
		Reference    map[string]any    `json:"reference"`
		SseConfig    map[string]any    `json:"sseConfig"`
		StoreOptions map[string]any    `json:"storeOptions"`
		Name         string            `json:"name"`
		StoreFormat  string            `json:"storeFormat"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	as, err := h.Backend.CreateAnnotationStore(
		req.Name,
		req.StoreFormat,
		req.Reference,
		req.SseConfig,
		req.StoreOptions,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, as)
}

func (h *Handler) handleDeleteAnnotationStore(c *echo.Context, name string) error {
	as, err := h.Backend.DeleteAnnotationStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, as)
}

func (h *Handler) handleGetAnnotationStore(c *echo.Context, name string) error {
	as, err := h.Backend.GetAnnotationStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, as)
}

func (h *Handler) handleListAnnotationStores(c *echo.Context) error {
	maxResults, nextToken := listQueryParams(c)

	stores, next, err := h.Backend.ListAnnotationStores(maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"annotationStores": stores, keyNextToken: next})
}

func (h *Handler) handleUpdateAnnotationStore(c *echo.Context, name string) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	as, err := h.Backend.UpdateAnnotationStore(name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, as)
}

func (h *Handler) handleStartAnnotationImportJob(c *echo.Context) error {
	var req struct {
		DestinationName string                 `json:"destinationName"`
		RoleArn         string                 `json:"roleArn"`
		Items           []AnnotationImportItem `json:"items"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartAnnotationImportJob(req.DestinationName, req.RoleArn, req.Items)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetAnnotationImportJob(c *echo.Context, jobID string) error {
	job, err := h.Backend.GetAnnotationImportJob(jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListAnnotationImportJobs(c *echo.Context) error {
	var req struct {
		Filter *ImportJobFilter `json:"filter"`
		IDs    []string         `json:"ids"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	jobs, next, err := h.Backend.ListAnnotationImportJobs(req.Filter, req.IDs, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyImportJobs: jobs, keyNextToken: next})
}

func (h *Handler) handleCancelAnnotationImportJob(c *echo.Context, jobID string) error {
	if err := h.Backend.CancelAnnotationImportJob(jobID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateAnnotationStoreVersion(c *echo.Context, name string) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		VersionName string            `json:"versionName"`
		Description string            `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	v, err := h.Backend.CreateAnnotationStoreVersion(
		name,
		req.VersionName,
		req.Description,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, v)
}

func (h *Handler) handleDeleteAnnotationStoreVersions(c *echo.Context, name string) error {
	var req struct {
		Versions []string `json:"versions"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	errs, err := h.Backend.DeleteAnnotationStoreVersions(name, req.Versions)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyErrors: errs})
}

func (h *Handler) handleGetAnnotationStoreVersion(c *echo.Context, name, versionName string) error {
	v, err := h.Backend.GetAnnotationStoreVersion(name, versionName)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleListAnnotationStoreVersions(c *echo.Context, name string) error {
	maxResults, nextToken := listQueryParams(c)

	versions, next, err := h.Backend.ListAnnotationStoreVersions(
		name,
		maxResults,
		nextToken,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{"annotationStoreVersions": versions, keyNextToken: next},
	)
}

func (h *Handler) handleUpdateAnnotationStoreVersion(
	c *echo.Context,
	name, versionName string,
) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	v, err := h.Backend.UpdateAnnotationStoreVersion(name, versionName, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, v)
}
