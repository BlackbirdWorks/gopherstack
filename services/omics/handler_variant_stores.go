package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateVariantStore(c *echo.Context) error {
	var req struct {
		Tags      map[string]string `json:"tags"`
		Reference map[string]any    `json:"reference"`
		Name      string            `json:"name"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	vs, err := h.Backend.CreateVariantStore(req.Name, req.Reference, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, vs)
}

func (h *Handler) handleDeleteVariantStore(c *echo.Context, name string) error {
	vs, err := h.Backend.DeleteVariantStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, vs)
}

func (h *Handler) handleGetVariantStore(c *echo.Context, name string) error {
	vs, err := h.Backend.GetVariantStore(name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, vs)
}

func (h *Handler) handleListVariantStores(c *echo.Context) error {
	maxResults, nextToken := listQueryParams(c)

	stores, next, err := h.Backend.ListVariantStores(maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"variantStores": stores, keyNextToken: next})
}

func (h *Handler) handleUpdateVariantStore(c *echo.Context, name string) error {
	var req struct {
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	vs, err := h.Backend.UpdateVariantStore(name, req.Description)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, vs)
}

func (h *Handler) handleStartVariantImportJob(c *echo.Context) error {
	var req struct {
		DestinationName string              `json:"destinationName"`
		RoleArn         string              `json:"roleArn"`
		Items           []VariantImportItem `json:"items"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	job, err := h.Backend.StartVariantImportJob(req.DestinationName, req.RoleArn, req.Items)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, job)
}

func (h *Handler) handleGetVariantImportJob(c *echo.Context, jobID string) error {
	job, err := h.Backend.GetVariantImportJob(jobID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, job)
}

func (h *Handler) handleListVariantImportJobs(c *echo.Context) error {
	var req struct {
		Filter *ImportJobFilter `json:"filter"`
		IDs    []string         `json:"ids"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	maxResults, nextToken := listQueryParams(c)

	jobs, next, err := h.Backend.ListVariantImportJobs(req.Filter, req.IDs, maxResults, nextToken)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyImportJobs: jobs, keyNextToken: next})
}

func (h *Handler) handleCancelVariantImportJob(c *echo.Context, jobID string) error {
	if err := h.Backend.CancelVariantImportJob(jobID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}
